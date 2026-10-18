#!/usr/bin/env python3
"""Test the gene-list validation of the ClinVar monitor candidate in a REAL, disposable Kestra 2.0.x server.

Question answered: does Kestra's own Pebble engine evaluate the candidate's `validate_genes` condition (and the `state_key_prefix`
input validator) the way the offline interpreter predicts? Nothing else is tested here.

Isolation by construction (not by hoping a service is unreachable):
  * The FULL candidate flow is only validated and created (schedule trigger disabled, gallery `extend:` block stripped, exactly as in the
    earlier integration test). It is NEVER executed: no code path here starts an execution of it.
  * Every execution runs a DERIVED validation-only flow built from the candidate's own YAML:
      - the `genes` and `state_key_prefix` inputs and the `validate_genes` task are copied VERBATIM (checked for equality);
      - `watch_genes` keeps its `values`, but its body is one harmless Log task named `read_snapshot` that prints the KV key the real
        task would use (the real key expression, verbatim);
      - probe Log tasks print what Kestra's engine computes for the condition and its parts (allowFailure, so a render error is
        recorded rather than hiding the rest);
      - the flow contains only Log, Fail and Loop tasks: no HTTP, script, KV, Slack or Docker task.
  * Credentials are throwaway; no secret of any kind exists on the server.

Strict result handling: every check must be PASS. A FAIL, an UNVERIFIED check or a check that never ran fails the job (UNVERIFIED also
emits a GitHub warning annotation), and so does a PASS count below --min-pass. If the real engine disagrees with the interpreter the
run reports it; nothing here rewrites the expression.
"""
import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pebble_subset as ps  # noqa: E402

TERMINAL = {"SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED", "RETRIED"}
PREFIXES = ("/api/v1/main", "/api/v1")
KV_RULE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]*")  # Kestra v2.0.6 KVStore.KEY_VALIDATOR_PATTERN, matched in full
TRIGGER_MARKER = "    type: io.kestra.plugin.core.trigger.Schedule\n"
STUB_NAMESPACE, STUB_ID = "ci.genevalidation", "gene-validation-stub"
ALLOWED_STUB_TYPES = {"io.kestra.plugin.core.log.Log", "io.kestra.plugin.core.execution.Fail", "io.kestra.plugin.core.flow.Loop"}
SECRETS = []

# ---- the cases ------------------------------------------------------------------------------------------------------------------
VALID_GENES = [["MAX"], ["BRCA1", "HLA-A"], ["TP53", "EGFR"], ["Brca1", "tp53"], ["a.b_c", "-x"]]
INVALID_GENES = [["BRCA1,TP53"], ["A,B"], ["MAX", "A,B"], [""], [" "], ["IGH@"], ["é"], ["MAX", ""], ["BRCA1 "], ["A;B"], ["A\nB"], []]
VALID_PREFIXES = ["clinvar_snapshot", "a1", "A.b-c_d"]
INVALID_PREFIXES = ["_x", ".x", "-x", "a b", "a@b", "a/b", ""]
EMPTY_LIST = []  # may be refused by Kestra's own input handling before any task: recorded, not counted against the expression


def case_id(prefix, genes):
    return "%s:%s" % (prefix, json.dumps(genes, ensure_ascii=True))


def expected_keys():
    keys = ["version", "candidate_validates", "candidate_created", "stub_validates", "stub_created", "probes_ran"]
    for genes in VALID_GENES:
        keys += ["valid %s %s" % (json.dumps(genes), part) for part in ("state", "validate_and_loop", "keys", "probes")]
    for genes in INVALID_GENES:
        keys += ["invalid %s %s" % (json.dumps(genes), part) for part in ("state", "validate_failed", "downstream_never_ran", "probes")]
    keys += ["prefix valid %r" % p for p in VALID_PREFIXES] + ["prefix invalid %r" % p for p in INVALID_PREFIXES]
    return keys


# ---- reporting ------------------------------------------------------------------------------------------------------------------
def redact(text):
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "***")
    return text


def say(*parts):
    print(redact(" ".join(str(p) for p in parts)), flush=True)


def annotation_escape(text):
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def b64(text):
    return base64.b64encode(text.encode()).decode()


class Report:
    RANK = {"INFO": 0, "PASS": 1, "UNVERIFIED": 2, "FAIL": 3}

    def __init__(self):
        self.items, self.by_key = [], {}

    def add(self, status, name, detail="", key=None):
        self.items.append((status, name, str(detail), key))
        if key and self.RANK[status] > self.RANK.get(self.by_key.get(key), -1):
            self.by_key[key] = status
        say("%-10s %s%s" % (status, name, (" -- " + str(detail)) if detail else ""))

    def check(self, name, ok, detail="", key=None):
        self.add("PASS" if ok else "FAIL", name, detail, key)
        return bool(ok)

    def unverified(self, name, why, key=None):
        self.add("UNVERIFIED", name, why, key)

    def info(self, name, detail=""):
        self.add("INFO", name, detail)

    def count(self, status):
        return sum(1 for s, _, _, _ in self.items if s == status)

    def problems(self, min_pass, keys):
        problems = []
        if self.count("FAIL"):
            problems.append("%d check(s) FAILED" % self.count("FAIL"))
        missing = [(k, self.by_key.get(k, "NOT RUN")) for k in keys if self.by_key.get(k) != "PASS"]
        if missing:
            problems.append("checks not established: " + ", ".join("%s=%s" % m for m in missing[:12]) + (" ..." if len(missing) > 12 else ""))
        if self.count("PASS") < min_pass:
            problems.append("only %d PASS results; the documented minimum is %d" % (self.count("PASS"), min_pass))
        return problems

    def finish(self, min_pass, keys):
        for status, name, detail, _ in self.items:
            if status == "UNVERIFIED":
                say("::warning title=UNVERIFIED::" + annotation_escape("%s: %s" % (name, detail)))
        problems = self.problems(min_pass, keys)
        for problem in problems:
            say("::error title=Gene validation run not accepted::" + annotation_escape(problem))
        return 1 if problems else 0

    def markdown(self, min_pass, keys):
        problems = self.problems(min_pass, keys)
        lines = ["## Kestra gene-validation results", "", "**Run accepted.**" if not problems else "**Run NOT accepted:** " + "; ".join(problems),
                 "", "| Status | Check | Detail |", "|---|---|---|"]
        for status, name, detail, _ in self.items:
            lines.append("| %s | %s | %s |" % (status, name.replace("|", "/"), redact(detail).replace("|", "/").replace("\n", " ")[:300]))
        lines += ["", "PASS %d (minimum %d), FAIL %d, UNVERIFIED %d, INFO %d" % (self.count("PASS"), min_pass, self.count("FAIL"), self.count("UNVERIFIED"), self.count("INFO"))]
        return "\n".join(lines) + "\n"


# ---- HTTP -----------------------------------------------------------------------------------------------------------------------
class Client:
    def __init__(self, base, user, password, timeout=60):
        self.base, self.timeout, self.prefix = base.rstrip("/"), timeout, None
        token = b64("%s:%s" % (user, password))
        SECRETS.extend([token, password, b64(password), "%s:%s" % (user, password)])
        self.auth = "Basic " + token

    def raw(self, method, path, data=None, content_type=None):
        request = urllib.request.Request(self.base + path, data=data, method=method)
        request.add_header("Authorization", self.auth)
        request.add_header("Accept", "application/json")
        if content_type:
            request.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, OSError) as exc:
            return None, type(exc).__name__.encode()

    def json(self, method, path, data=None, content_type=None):
        status, body = self.raw(method, path, data, content_type)
        try:
            return status, (json.loads(body.decode("utf-8")) if body else None)
        except ValueError:
            return status, None


def multipart(fields):
    boundary = "----ci" + uuid.uuid4().hex
    parts = ['--%s\r\nContent-Disposition: form-data; name="%s"\r\n\r\n%s\r\n' % (boundary, k, v) for k, v in fields.items()]
    return ("".join(parts) + "--%s--\r\n" % boundary).encode("utf-8"), "multipart/form-data; boundary=%s" % boundary


def snippet(value, limit=240):
    return redact(re.sub(r"\s+", " ", value if isinstance(value, str) else json.dumps(value))[:limit])


# ---- the flows ------------------------------------------------------------------------------------------------------------------
def strip_gallery_metadata(flow_text):
    """Remove the top-level `extend:` block (Blueprints-gallery metadata that Kestra's flow model rejects)."""
    lines = flow_text.replace("\r\n", "\n").split("\n")
    starts = [i for i, ln in enumerate(lines) if re.match(r"extend:(\s|$)", ln)]
    if len(starts) > 1:
        raise ValueError("expected at most one top-level extend: key, found %d" % len(starts))
    if not starts:
        return "\n".join(lines)
    start = starts[0]
    end = start + 1
    while end < len(lines) and (lines[end] == "" or lines[end][0] in " \t"):
        end += 1
    while start > 0 and lines[start - 1] == "":
        start -= 1
    return "\n".join(lines[:start] + lines[end:]).rstrip("\n") + "\n"


def disable_trigger(flow_text):
    text = flow_text.replace("\r\n", "\n")
    if text.count(TRIGGER_MARKER) != 1:
        raise ValueError("expected exactly one Schedule trigger line, found %d" % text.count(TRIGGER_MARKER))
    return text.replace(TRIGGER_MARKER, TRIGGER_MARKER + "    disabled: true\n")


def flow_identity(flow_text):
    ns = re.search(r"^namespace:\s*(\S+)\s*$", flow_text, re.M)
    fid = re.search(r"^id:\s*(\S+)\s*$", flow_text, re.M)
    if not ns or not fid:
        raise ValueError("cannot read the flow id and namespace")
    return ns.group(1), fid.group(1)


def split_top_level_or(body):
    """Split an expression body at its depth-0 ' or ' (outside parentheses and quotes)."""
    depth, quote, parts, last, i = 0, False, [], 0, 0
    while i < len(body):
        ch = body[i]
        if ch == "'":
            quote = not quote
        elif not quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and body.startswith(" or ", i):
                parts.append(body[last:i].strip())
                last = i + 4
                i += 3
        i += 1
    parts.append(body[last:].strip())
    return parts


def probes_from_condition(condition):
    """name -> expression body. condition/c1/c2 come from the candidate's own condition text; the rest isolate each construct."""
    body = condition.strip()
    assert body.startswith("{{") and body.endswith("}}"), "the condition must be a single {{ }} expression"
    body = body[2:-2].strip()
    pieces = split_top_level_or(body)
    if len(pieces) != 2:
        raise ValueError("expected the condition to be 'C1 or C2' at the top level, found %d part(s)" % len(pieces))
    return {
        "condition": body,
        "c1": pieces[0],
        "c2": pieces[1],
        "collapsed": "(inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true)",
        "gcount": "((inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true) | replace({',': ''})) | length",
        "entries": "(inputs.genes | length) ~ ''",
        "comma_removed": "(inputs.genes | join(',')) | replace({',': ''})",
        "or_literal": "(('a') != ('a')) or (('1' ~ '') != ('2' ~ ''))",
    }


def predicted(expression, genes):
    value, _, _ = ps.evaluate("{{ %s }}" % expression, {"inputs": {"genes": list(genes)}})
    return str(value).lower() if isinstance(value, bool) else str(value)


def derive_stub(candidate_text):
    """The validation-only flow. Returns (yaml text, probes). Raises if anything that must be verbatim is not."""
    doc = yaml.safe_load(candidate_text)
    inputs = {i["id"]: i for i in doc["inputs"]}
    validate = next(t for t in doc["tasks"] if t["id"] == "validate_genes")
    watch = next(t for t in doc["tasks"] if t["id"] == "watch_genes")
    real_get = next(t for t in watch["tasks"] if t["id"] == "read_snapshot")
    probes = probes_from_condition(validate["condition"])
    loop = json.loads(json.dumps(watch))
    loop.pop("outputs", None)
    loop["tasks"] = [{"id": "read_snapshot", "type": "io.kestra.plugin.core.log.Log", "message": "KEYCHECK " + real_get["key"]}]
    tasks = [{"id": "probe_" + name, "type": "io.kestra.plugin.core.log.Log", "allowFailure": True,
              "message": "PROBE %s={{ %s }}" % (name, expr)} for name, expr in probes.items()]
    tasks += [json.loads(json.dumps(validate)), loop]
    stub = {"id": STUB_ID, "namespace": STUB_NAMESPACE, "inputs": [json.loads(json.dumps(inputs["genes"])), json.loads(json.dumps(inputs["state_key_prefix"]))],
            "tasks": tasks}
    text = yaml.safe_dump(stub, sort_keys=False, width=100000, allow_unicode=True)
    back = yaml.safe_load(text)
    assert next(t for t in back["tasks"] if t["id"] == "validate_genes") == validate, "validate_genes is not verbatim"
    assert back["inputs"] == [inputs["genes"], inputs["state_key_prefix"]], "the inputs are not verbatim"
    types = {t["type"] for t in back["tasks"]} | {t["type"] for t in back["tasks"][-1]["tasks"]}
    assert types <= ALLOWED_STUB_TYPES, "the stub contains a task type that is not allowed: %s" % sorted(types - ALLOWED_STUB_TYPES)
    return text, probes


# ---- the driver -----------------------------------------------------------------------------------------------------------------
def state_of(execution):
    return ((execution or {}).get("state") or {}).get("current")


def tasks_by_id(execution):
    runs = (execution or {}).get("taskRunList")
    if not isinstance(runs, list):
        return None
    out = {}
    for run in runs:
        if isinstance(run, dict):
            out.setdefault(run.get("taskId"), []).append(((run.get("state") or {}).get("current")))
    return out


class Driver:
    def __init__(self, client, report, poll):
        self.client, self.report, self.poll = client, report, poll
        self.default_prefix = None  # the state_key_prefix input default, read from the candidate in run_all
        self.executed = []  # every (namespace, flow id) an execution was started for: the full candidate must never appear here

    def wait_for_server(self, timeout):
        deadline, rejected = time.monotonic() + timeout, 0
        while time.monotonic() < deadline:
            for prefix in PREFIXES:
                status, _ = self.client.raw("GET", prefix + "/flows/search?size=1")
                if status == 200:
                    self.client.prefix = prefix
                    return True
                if status == 401:
                    rejected += 1
            if rejected >= 3:
                self.report.check("the server accepts the throwaway credentials", False, "HTTP 401 three times")
                return False
            time.sleep(self.poll)
        return False

    def check_version(self, expected_prefix):
        reported = None
        for path in (self.client.prefix + "/configs", "/api/v1/configs"):
            status, data = self.client.json("GET", path)
            if status == 200 and isinstance(data, dict) and isinstance(data.get("version"), str) and data["version"].strip():
                reported = data["version"].strip()
                break
        name = "the server is Kestra %sx" % expected_prefix
        if reported is None:
            self.report.unverified(name, "the configs endpoint did not report a version", key="version")
            return
        ok = re.fullmatch(re.escape(expected_prefix) + r"\d+(?:[-+][0-9A-Za-z.-]+)?", reported.lstrip("v")) is not None
        self.report.check(name, ok, "configs endpoint reports %r" % reported, key="version")

    def deploy(self, label, flow_text, key_prefix):
        ns, fid = flow_identity(flow_text)
        status, data = self.client.json("POST", self.client.prefix + "/flows/validate", flow_text.encode("utf-8"), "application/x-yaml")
        if status == 200 and isinstance(data, list):
            problems = [i for i in data if isinstance(i, dict) and i.get("constraints")]
            self.report.check("Kestra validates the %s flow" % label, not problems, snippet(problems) if problems else "no constraint violation", key=key_prefix + "_validates")
        else:
            self.report.unverified("Kestra validates the %s flow" % label, "HTTP %s: %s" % (status, snippet(data)), key=key_prefix + "_validates")
        status, data = self.client.json("POST", self.client.prefix + "/flows", flow_text.encode("utf-8"), "application/x-yaml")
        created = self.report.check("the %s flow is created (%s.%s)" % (label, ns, fid), status == 200, "HTTP %s %s" % (status, "" if status == 200 else snippet(data)),
                                    key=key_prefix + "_created")
        return ns, fid, created

    def start(self, ns, fid, fields):
        assert (ns, fid) == (STUB_NAMESPACE, STUB_ID), "only the validation-only flow may ever be executed"
        self.executed.append((ns, fid))
        body, ctype = multipart(fields)
        return self.client.json("POST", "%s/executions/%s/%s" % (self.client.prefix, ns, fid), body, ctype)

    def fetch(self, execution_id):
        status, data = self.client.json("GET", "%s/executions/%s" % (self.client.prefix, execution_id))
        return data if status == 200 and isinstance(data, dict) else None

    def wait(self, execution_id):
        deadline = time.monotonic() + float(os.environ.get("DRIVER_EXEC_TIMEOUT", "300"))
        last = None
        while time.monotonic() < deadline:
            last = self.fetch(execution_id)
            if state_of(last) in TERMINAL:
                return last
            time.sleep(self.poll)
        return last

    def logs(self, execution_id):
        status, data = self.client.json("GET", "%s/logs/%s" % (self.client.prefix, execution_id))
        rows = data if isinstance(data, list) else (data or {}).get("results") if isinstance(data, dict) else None
        if status != 200 or not isinstance(rows, list):
            return None
        return [r for r in rows if isinstance(r, dict)]

    def children(self, parent_id):
        query = urllib.parse.urlencode({"filters[kind][EQUALS]": "LOOP", "filters[parentId][EQUALS]": parent_id, "size": "100"})
        status, data = self.client.json("GET", "%s/executions/search?%s" % (self.client.prefix, query))
        results = data.get("results") if isinstance(data, dict) else data if isinstance(data, list) else None
        if status != 200 or not isinstance(results, list):
            return None
        return [r for r in results if isinstance(r, dict) and r.get("id") and r.get("parentId", parent_id) == parent_id]

    def settle_children(self, parent_id, expected):
        """expected > 0: wait for that many TERMINAL sub-executions. expected == 0: absence needs repeated empty answers over a settle period."""
        deadline = time.monotonic() + float(os.environ.get("DRIVER_CHILD_TIMEOUT", "90"))
        settle = float(os.environ.get("DRIVER_CHILD_SETTLE", "6"))
        empty_since, polls, failures = None, 0, 0
        while time.monotonic() < deadline:
            rows = self.children(parent_id)
            if rows is None:
                failures += 1
                if failures >= 3:
                    return None
            elif expected == 0:
                if rows:
                    return rows
                polls += 1
                empty_since = empty_since or time.monotonic()
                if polls >= 3 and time.monotonic() - empty_since >= settle:
                    return []
            elif len(rows) >= expected and all(state_of(self.fetch(r["id"])) in TERMINAL for r in rows):
                return rows
            time.sleep(self.poll)
        return self.children(parent_id)

    def probe_values(self, rows):
        values = {}
        for row in rows or []:
            message = str(row.get("message", ""))
            m = re.match(r"PROBE (\w+)=(.*)$", message, re.S)
            if m:
                values[m.group(1)] = m.group(2).strip()
        return values

    def run_case(self, genes, prefix):
        status, data = self.start(STUB_NAMESPACE, STUB_ID, {"genes": json.dumps(genes), "state_key_prefix": prefix})
        if status in (200, 201) and isinstance(data, dict) and data.get("id"):
            return self.wait(data["id"]), status, data
        return None, status, data

    def gene_case(self, genes, valid, probes):
        label = json.dumps(genes)
        execution, status, data = self.run_case(genes, "clinvar_snapshot")
        kind = "valid" if valid else "invalid"
        k = lambda part: "%s %s %s" % (kind, label, part)
        if execution is None:
            if not valid and not genes:
                self.report.info("invalid %s: Kestra refused the empty list at input handling (HTTP %s); no task ran, the expression was not exercised" % (label, status), snippet(data))
                for part in ("state", "validate_failed", "downstream_never_ran", "probes"):
                    self.report.add("PASS", "invalid [] (%s): refused before any task ran" % part, "HTTP %s" % status, key=k(part))
                return
            for part in (("state", "validate_and_loop", "keys", "probes") if valid else ("state", "validate_failed", "downstream_never_ran", "probes")):
                self.report.add("FAIL", "%s %s (%s): the execution could not be created" % (kind, label, part), "HTTP %s %s" % (status, snippet(data)), key=k(part))
            return
        tasks = tasks_by_id(execution)
        rows = self.logs(execution["id"])
        got = self.probe_values(rows)
        # probes: what the real engine computed against what the offline interpreter predicts
        if rows is None:
            self.report.unverified("%s %s: probe values" % (kind, label), "the logs endpoint did not answer", key=k("probes"))
        else:
            # compared without leading/trailing whitespace: log lines may be trimmed, and the verdict does not depend on it
            diffs = {n: (got.get(n), predicted(e, genes).strip()) for n, e in probes.items() if (got.get(n) or "").strip().lower() != predicted(e, genes).strip().lower()}
            self.report.check("%s %s: Kestra's probe values equal the interpreter's" % (kind, label), not diffs,
                              "engine vs interpreter: %s" % diffs if diffs else "condition=%s c1=%s c2=%s entries=%s gcount=%s" % (
                                  got.get("condition"), got.get("c1"), got.get("c2"), got.get("entries"), got.get("gcount")), key=k("probes"))
        self.report.info("%s %s: execution %s" % (kind, label, state_of(execution)),
                         "tasks %s" % ({t: "/".join(map(str, s)) for t, s in (tasks or {}).items() if not t.startswith("probe_")}))
        if valid:
            self.report.check("valid %s: execution ends SUCCESS" % label, state_of(execution) == "SUCCESS", "state %s" % state_of(execution), key=k("state"))
            ok = tasks is not None and tasks.get("validate_genes") == ["SUCCESS"] and tasks.get("watch_genes") == ["SUCCESS"]
            self.report.check("valid %s: validate_genes SUCCESS and the loop ran" % label, ok, str({t: tasks.get(t) for t in ("validate_genes", "watch_genes")} if tasks else tasks), key=k("validate_and_loop"))
            kids = self.settle_children(execution["id"], len(genes))
            self.check_keys(execution, genes, kids, k("keys"))
        else:
            self.report.check("invalid %s: execution ends FAILED" % label, state_of(execution) == "FAILED", "state %s" % state_of(execution), key=k("state"))
            self.report.check("invalid %s: validate_genes FAILED" % label, tasks is not None and tasks.get("validate_genes") == ["FAILED"],
                              str(tasks.get("validate_genes") if tasks else tasks), key=k("validate_failed"))
            kids = self.settle_children(execution["id"], 0)
            name = "invalid %s: neither watch_genes nor read_snapshot ever ran" % label
            if tasks is None or kids is None:
                self.report.unverified(name, "the task list or the sub-execution search was not available", key=k("downstream_never_ran"))
            else:
                self.report.check(name, "watch_genes" not in tasks and not kids, "parent task runs of watch_genes: %s; LOOP sub-executions found: %d" % (tasks.get("watch_genes"), len(kids)),
                                  key=k("downstream_never_ran"))

    def check_keys(self, execution, genes, kids, key):
        name = "valid %s: one sub-execution per gene, read_snapshot SUCCESS, KV key valid under Kestra's rule" % json.dumps(genes)
        if kids is None or len(kids) != len(genes):
            self.report.unverified(name, "found %s sub-execution(s), expected %d" % (None if kids is None else len(kids), len(genes)), key=key)
            return
        seen, problems = [], []
        for row in kids:
            child = self.fetch(row["id"])
            rows = self.logs(row["id"]) or []
            ids = tasks_by_id(child) or {}
            keys = [m.group(1) for r in rows for m in [re.match(r"KEYCHECK (\S+)$", str(r.get("message", "")).strip())] if m]
            if ids.get("read_snapshot") != ["SUCCESS"] or len(keys) != 1:
                problems.append("child %s: read_snapshot %s, keys %s" % (row["id"], ids.get("read_snapshot"), keys))
            else:
                seen.append(keys[0])
        expected = sorted("clinvar_snapshot_%s" % g.strip().lower() for g in genes)
        if problems:
            self.report.check(name, False, "; ".join(problems), key=key)
        else:
            self.report.check(name, sorted(seen) == expected and all(KV_RULE.fullmatch(s) for s in seen), "keys %s" % sorted(seen), key=key)

    def used_key(self, execution):
        """The KV key the execution's read_snapshot task printed (KEYCHECK), or None when it cannot be read."""
        kids = self.settle_children(execution["id"], 1)
        if not kids or len(kids) != 1:
            return None
        found = [m.group(1) for r in (self.logs(kids[0]["id"]) or []) for m in [re.match(r"KEYCHECK (\S*)$", str(r.get("message", "")).strip())] if m]
        return found[0] if len(found) == 1 else None

    def prefix_case(self, prefix, valid):
        """Every prefix case records the key the run actually used. The empty string is a special case: Kestra may reject it, replace it
        with the input's default, or run with a truly empty prefix; only the last is a failure, and it is told apart by the recorded key."""
        key = "prefix %s %r" % ("valid" if valid else "invalid", prefix)
        execution, status, data = self.run_case(["MAX"], prefix)
        used = self.used_key(execution) if execution is not None and state_of(execution) == "SUCCESS" else None
        if prefix == "" or valid:
            self.report.info("prefix %r: observed" % prefix, "HTTP %s, state %s, KV key used: %r" % (status, state_of(execution), used))
        if valid:
            expected = "%s_max" % prefix
            ok = execution is not None and state_of(execution) == "SUCCESS" and used == expected and KV_RULE.fullmatch(used) is not None
            self.report.check("prefix %r is accepted, the run succeeds and the key is %r" % (prefix, expected), ok,
                              "HTTP %s, state %s, key %r" % (status, state_of(execution), used), key=key)
            return
        if execution is None:
            self.report.check("prefix %r is rejected when the execution is created" % prefix, status is not None and 400 <= status < 500, "HTTP %s %s" % (status, snippet(data)), key=key)
            return
        tasks = tasks_by_id(execution)
        if prefix == "" and state_of(execution) == "SUCCESS":
            name = "prefix '' (empty value) never runs with an empty prefix: it is replaced by the input default %r" % self.default_prefix
            if used is None:
                self.report.unverified(name, "the key the run used could not be read", key=key)
            else:
                expected = "%s_max" % self.default_prefix
                self.report.check(name, used == expected and KV_RULE.fullmatch(used) is not None, "observed key %r (default substituted: %s)" % (used, used == expected), key=key)
            return
        self.report.check("prefix %r is rejected before any task runs" % prefix, state_of(execution) == "FAILED" and tasks is not None and not tasks,
                          "state %s, task runs %s, key used %r" % (state_of(execution), sorted(tasks or {}), used), key=key)

    def run_all(self, candidate_text, expected_prefix):
        self.check_version(expected_prefix)
        self.default_prefix = next(i for i in yaml.safe_load(candidate_text)["inputs"] if i["id"] == "state_key_prefix")["defaults"]
        deploy_text = disable_trigger(strip_gallery_metadata(candidate_text))
        self.report.info("the full candidate is deployed only to be validated and created, never executed",
                         "gallery extend: block removed and 'disabled: true' added to the schedule trigger; %d -> %d lines" % (candidate_text.count("\n"), deploy_text.count("\n")))
        self.deploy("candidate", deploy_text, "candidate")
        stub_text, probes = derive_stub(candidate_text)
        self.report.info("the validation-only flow keeps the candidate's inputs and validate_genes verbatim", "probe tasks: %s" % ", ".join(sorted(probes)))
        ns, fid, created = self.deploy("validation-only", stub_text, "stub")
        if not created:
            return
        for genes in VALID_GENES:
            self.gene_case(genes, True, probes)
        for genes in INVALID_GENES:
            self.gene_case(genes, False, probes)
        for prefix in VALID_PREFIXES:
            self.prefix_case(prefix, True)
        for prefix in INVALID_PREFIXES:
            self.prefix_case(prefix, False)
        ran = any(k.endswith(" probes") and v == "PASS" for k, v in self.report.by_key.items())
        self.report.check("at least one case returned probe values computed by Kestra's engine", ran, "", key="probes_ran")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--expected-version-prefix", default="2.0.")
    parser.add_argument("--min-pass", type=int, default=len(expected_keys()))
    parser.add_argument("--startup-timeout", type=int, default=300)
    args = parser.parse_args(argv)
    user, password = os.environ.get("KESTRA_USER", ""), os.environ.get("KESTRA_PASSWORD", "")
    if not user or not password:
        print("KESTRA_USER and KESTRA_PASSWORD must be set (throwaway credentials of the throwaway server)")
        return 2
    SECRETS.extend([password, b64(password), b64("%s:%s" % (user, password)), "%s:%s" % (user, password)])
    host = urllib.parse.urlsplit(args.base_url).hostname
    if host not in ("127.0.0.1", "localhost"):
        say("refusing to talk to %r: this driver only ever targets a loopback Kestra" % host)
        return 2
    raw = Path(args.candidate).read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != args.expected_sha256.lower():
        say("::error title=Wrong candidate::candidate sha256 %s does not match the expected %s; stopping" % (actual, args.expected_sha256))
        return 2
    report = Report()
    driver = Driver(Client(args.base_url, user, password), report, float(os.environ.get("DRIVER_POLL_SECONDS", "2")))
    keys = expected_keys()
    try:
        if not driver.wait_for_server(args.startup_timeout):
            report.check("the Kestra server became ready", False, "no authenticated API answer within %ds" % args.startup_timeout)
        else:
            report.check("the Kestra server became ready", True, "API prefix %s" % driver.client.prefix)
            driver.run_all(raw.decode("utf-8"), args.expected_version_prefix)
    except Exception as exc:  # report, never a bare traceback; the checks already printed stay valid
        report.check("the driver finished without an internal error", False, "%s: %s" % (type(exc).__name__, exc))
    candidate_executed = any(fid != STUB_ID for _, fid in driver.executed)
    report.check("the full candidate flow was never executed", not candidate_executed, "executions started: %s" % sorted(set(driver.executed)))
    say("")
    say("PASS %d (minimum %d) | FAIL %d | UNVERIFIED %d | INFO %d" % (report.count("PASS"), args.min_pass, report.count("FAIL"), report.count("UNVERIFIED"), report.count("INFO")))
    code = report.finish(args.min_pass, keys)
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(report.markdown(args.min_pass, keys))
    return code


if __name__ == "__main__":
    sys.exit(main())
