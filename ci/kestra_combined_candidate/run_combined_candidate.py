#!/usr/bin/env python3
"""PREPARED, NOT EXECUTED AGAINST KESTRA. Observe, in a REAL disposable Kestra 2.0.x server, the EXACT combined Phase 9 candidate:
the strengthened `validate_genes` condition AND the Loop `values: "{{ inputs.genes | toJson | lower }}"`, working together.

The candidate is validated and created (schedule trigger disabled, gallery `extend:` block stripped) but is NEVER executed. A derived VALIDATION-ONLY flow
is built from the candidate's own YAML:
  * the `genes` and `state_key_prefix` inputs, the `validate_genes` task (description, condition, message) and the Loop `values` are copied VERBATIM (checked);
  * a parent Log task prints what Kestra renders for the `values` expression;
  * the Loop body is two harmless Log tasks: `read_snapshot` prints the KV key the real task would use (the real key expression, verbatim) and
    `iteration_probe` prints `item.index` and `item.value`;
  * only Log, Fail and Loop tasks exist: no HTTP, script, KV, Slack or Docker task, no trigger. Nothing can reach NCBI, Slack or any KV store.

EXPECTATIONS come from an independent oracle (a list is rejected unless it is non-empty and every entry fully matches [A-Za-z0-9._-]+) and from the offline
model of the Loop (JSON-parse the rendered, lower-cased text, exact-string distinct, order kept). The offline interpreter is only used to CROSS-CHECK the
oracle. OBSERVATIONS come only from Kestra. A disagreement is a FAIL; missing evidence is UNVERIFIED. This driver never edits an expression to make a case pass.
Key validity (Kestra's KVStore key pattern) is judged locally on the observed key; no KV store is touched, and the key analysis assumes the default prefix.

Strict result handling: every check must be PASS. FAIL, UNVERIFIED or a check that never ran fails the job, as does a PASS count below --min-pass.
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
GENE_RULE = re.compile(r"[A-Za-z0-9._-]+")           # the intended gene-name character set for ONE entry, matched in full
TRIGGER_MARKER = "    type: io.kestra.plugin.core.trigger.Schedule\n"
STUB_NAMESPACE = "ci.combined"
STUB_ID = "combined-validate-genes"
ALLOWED_STUB_TYPES = {"io.kestra.plugin.core.log.Log", "io.kestra.plugin.core.execution.Fail", "io.kestra.plugin.core.flow.Loop"}
PREFIX = "clinvar_snapshot"
SECRETS = []

COMBINED_CONDITION = ("{{ ((((inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true)) | replace({'g,': ''})) != 'g') or "
                      "((((((inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true)) | replace({',': ''})) | length) ~ '') != ((inputs.genes | length) ~ '')) }}")
COMBINED_VALUES = "{{ inputs.genes | toJson | lower }}"

# ---- cases ----------------------------------------------------------------------------------------------------------------------
# The three source sets of the Phase 8B / Phase 9 work, written out literally.
MATRIX_22 = [["BRCA1"], ["brca1", "TP53", "EGFR"], ["HLA-A"], ["C1orf43"], ["a.b_c"], ["BRCA1", "brca1"], [], [""], [" "], ["BRCA1", ""], ["BRCA1", " TP53"], ["TP53!"],
             ["TP/53"], ["TP53\n"], ["A,B"], ["BRCA1", "brca1,tp53"], ["BRCA1,TP53"], ["IGH@"], ["_TP53"], ["-TP53"], ["brca1,TP53,EGFR"], ["BRCA1,brca1"]]
EXTRA_28 = [["A,B", "C"], ["A,,B"], [",A"], ["A,"], ["g"], ["g,"], ["g", "g"], ["A", "B"], ["A-"], ["1"], ["0.5"], ["a"], ["TP53", "tp53"], ["HLA-A", "hla-a"], ["MAX", "MAX", "MAX"],
            ["BRCA1", "TP53", "EGFR", "MAX", "BRCA2"], ["A" * 120], ["Ünï"], ["TP53\r"], ["TP53\n", "BRCA1"], ["BRCA1", "TP53,"], ["-"], ["."], ["_"], ["BRCA1;TP53"],
            ["BRCA1|TP53"], ["{{x}}"], ["A_B-C.D"]]
REGRESSION_12 = [["BRCA1", "brca1"], ["brca1", "BRCA1"], ["BRCA1", "TP53", "brca1"], ["TP53", "EGFR", "TP53"], ["BRCA1", "BRCA1"], ["MAX", "MAX", "MAX"], ["Max", "MAX"],
                 ["HLA-A", "hla-a"], ["C1orf43", "C1ORF43"], ["a.b_c", "A.B_C"], ["EGFR", "TP53"], ["BRCA1", "TP53", "BRCA1", "brca1", "TP53"]]


def build_cases():
    """Distinct gene lists with every source that asked for them: (case id, genes, [sources]). Duplicates are removed ONLY by identical list, so every distinct
    input of the three sets is run exactly once and the mapping is kept."""
    order, sources = [], {}
    for name, items in (("matrix22", MATRIX_22), ("extra28", EXTRA_28), ("regression12", REGRESSION_12)):
        for position, genes in enumerate(items, 1):
            key = json.dumps(genes)
            if key not in sources:
                sources[key] = []
                order.append(genes)
            sources[key].append("%s#%d" % (name, position))
    return [(i, genes, sources[json.dumps(genes)]) for i, genes in enumerate(order, 1)]


CASES = build_cases()
PASS_PARTS = ("validation", "render", "count", "values", "keys", "key_rule")
STOP_PARTS = ("validation", "no_loop")


def oracle_stops(genes):
    return (not genes) or not all(GENE_RULE.fullmatch(g) for g in genes)


def expected_iterations(genes):
    """The Loop as the model reads it: each entry lower-cased by the values expression, then Kestra's exact-string .distinct(), first occurrence kept."""
    seen, out = set(), []
    for v in (g.lower() for g in genes):
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def expected_key_for(entry):
    return "%s_%s" % (PREFIX, entry.strip().lower())


def case_label(case_id):
    return "case%02d" % case_id


def expected_keys():
    keys = ["version", "full_validates", "full_created", "stub_validates", "stub_created"]
    for cid, genes, _ in CASES:
        for part in (STOP_PARTS if oracle_stops(genes) else PASS_PARTS):
            keys.append("%s %s %s" % (case_label(cid), json.dumps(genes), part))
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
            problems.append("checks not established: " + ", ".join("%s=%s" % m for m in missing[:10]) + (" ..." if len(missing) > 10 else ""))
        if self.count("PASS") < min_pass:
            problems.append("only %d PASS results; the documented minimum is %d" % (self.count("PASS"), min_pass))
        return problems

    def finish(self, min_pass, keys):
        for status, name, detail, _ in self.items:
            if status == "UNVERIFIED":
                say("::warning title=UNVERIFIED::" + annotation_escape("%s: %s" % (name, detail)))
        problems = self.problems(min_pass, keys)
        for problem in problems:
            say("::error title=Combined candidate run not accepted::" + annotation_escape(problem))
        return 1 if problems else 0

    def markdown(self, min_pass, keys):
        problems = self.problems(min_pass, keys)
        lines = ["## Kestra combined-candidate results", "", "**Run accepted.**" if not problems else "**Run NOT accepted:** " + "; ".join(problems), "",
                 "| Status | Check | Detail |", "|---|---|---|"]
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


# ---- the candidate and the derived flow -----------------------------------------------------------------------------------------
def strip_gallery_metadata(flow_text):
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


def extract_parts(flow_text):
    doc = yaml.safe_load(flow_text)
    validate = next(t for t in doc["tasks"] if t["id"] == "validate_genes")
    watch = next(t for t in doc["tasks"] if t["id"] == "watch_genes")
    return validate, watch


def derive_stub(flow_text):
    """The validation-only flow. Returns (yaml text, values expression)."""
    doc = yaml.safe_load(flow_text)
    inputs = {i["id"]: i for i in doc["inputs"]}
    validate, watch = extract_parts(flow_text)
    real_get = next(t for t in watch["tasks"] if t["id"] == "read_snapshot")
    loop = json.loads(json.dumps(watch))
    loop.pop("outputs", None)
    loop["tasks"] = [{"id": "read_snapshot", "type": "io.kestra.plugin.core.log.Log", "message": "KEYCHECK " + real_get["key"]},
                     {"id": "iteration_probe", "type": "io.kestra.plugin.core.log.Log", "message": "ITER index={{ item.index }} value={{ item.value }}"}]
    tasks = [{"id": "probe_values_render", "type": "io.kestra.plugin.core.log.Log", "allowFailure": True, "message": "VALUES=" + watch["values"]},
             json.loads(json.dumps(validate)), loop]
    stub = {"id": STUB_ID, "namespace": STUB_NAMESPACE, "inputs": [json.loads(json.dumps(inputs["genes"])), json.loads(json.dumps(inputs["state_key_prefix"]))],
            "tasks": tasks}
    text = yaml.safe_dump(stub, sort_keys=False, width=100000, allow_unicode=True)
    back = yaml.safe_load(text)
    assert next(t for t in back["tasks"] if t["id"] == "validate_genes") == validate, "validate_genes is not verbatim"
    assert next(t for t in back["tasks"] if t["id"] == "validate_genes")["condition"] == COMBINED_CONDITION, "the condition is not the expected combined condition"
    assert next(t for t in back["tasks"] if t["id"] == "watch_genes")["values"] == watch["values"] == COMBINED_VALUES, "the Loop values are not verbatim"
    assert back["inputs"] == [inputs["genes"], inputs["state_key_prefix"]], "the inputs are not verbatim"
    types = {t["type"] for t in back["tasks"]} | {t["type"] for t in back["tasks"][-1]["tasks"]}
    assert types <= ALLOWED_STUB_TYPES, "the stub contains a task type that is not allowed: %s" % sorted(types - ALLOWED_STUB_TYPES)
    return text, watch["values"]


def predicted_stop(condition, genes):
    return bool(ps.evaluate(condition, {"inputs": {"genes": list(genes)}})[0])


def predicted_render(genes):
    value, _, _ = ps.evaluate(COMBINED_VALUES, {"inputs": {"genes": list(genes)}})
    return json.loads(value)


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
        self.executed = []  # (namespace, flow id) of every execution started; only the validation-only flow may ever appear

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
        ns, fid = re.search(r"^namespace:\s*(\S+)\s*$", flow_text, re.M).group(1), re.search(r"^id:\s*(\S+)\s*$", flow_text, re.M).group(1)
        status, data = self.client.json("POST", self.client.prefix + "/flows/validate", flow_text.encode("utf-8"), "application/x-yaml")
        if status == 200 and isinstance(data, list):
            problems = [i for i in data if isinstance(i, dict) and i.get("constraints")]
            self.report.check("Kestra validates the %s" % label, not problems, snippet(problems) if problems else "no constraint violation", key=key_prefix + "_validates")
        else:
            self.report.unverified("Kestra validates the %s" % label, "HTTP %s: %s" % (status, snippet(data)), key=key_prefix + "_validates")
        status, data = self.client.json("POST", self.client.prefix + "/flows", flow_text.encode("utf-8"), "application/x-yaml")
        return self.report.check("the %s is created (%s.%s)" % (label, ns, fid), status == 200, "HTTP %s %s" % (status, "" if status == 200 else snippet(data)), key=key_prefix + "_created")

    def start(self, ns, fid, fields):
        assert ns == STUB_NAMESPACE and fid == STUB_ID, "only the validation-only flow may ever be executed"
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
        """Wait until the number of sub-executions is stable over a settle period, all are terminal, and at least `expected` exist."""
        deadline = time.monotonic() + float(os.environ.get("DRIVER_CHILD_TIMEOUT", "90"))
        settle = float(os.environ.get("DRIVER_CHILD_SETTLE", "4"))
        last_count, stable_since, stable_polls, failures = None, None, 0, 0
        while time.monotonic() < deadline:
            rows = self.children(parent_id)
            if rows is None:
                failures += 1
                if failures >= 3:
                    return None
            else:
                terminal = all(state_of(self.fetch(r["id"])) in TERMINAL for r in rows)
                if len(rows) == last_count and terminal:
                    stable_polls += 1
                    if stable_polls >= 3 and time.monotonic() - stable_since >= settle and len(rows) >= expected:
                        return rows
                else:
                    last_count, stable_since, stable_polls = len(rows), time.monotonic(), 1 if terminal else 0
            time.sleep(self.poll)
        return self.children(parent_id)

    def observe_children(self, rows):
        seen = []
        for row in rows:
            child = self.fetch(row["id"]) or {}
            loop_run = child.get("loopRun") if isinstance(child.get("loopRun"), dict) else {}
            logs = self.logs(row["id"])
            iter_lines = [m.groups() for r in (logs or []) for m in [re.match(r"ITER index=(\d+) value=(.*)$", str(r.get("message", "")).strip())] if m]
            keys = [m.group(1) for r in (logs or []) for m in [re.match(r"KEYCHECK (.*)$", str(r.get("message", "")).strip())] if m]
            seen.append({"id": row["id"], "index": loop_run.get("index"), "value": loop_run.get("value"), "iter": iter_lines[0] if len(iter_lines) == 1 else None,
                         "key": keys[0] if len(keys) == 1 else None, "state": state_of(child)})
        return sorted(seen, key=lambda s: (s["index"] is None, s["index"] if isinstance(s["index"], int) else 0))

    def run_case(self, case, condition):
        cid, genes, sources = case
        label = case_label(cid)
        stops = oracle_stops(genes)
        shown = "%s %s" % (label, json.dumps(genes))
        key = lambda part: "%s %s %s" % (label, json.dumps(genes), part)
        parts = STOP_PARTS if stops else PASS_PARTS
        if predicted_stop(condition, genes) != stops:
            for part in parts:
                self.report.add("FAIL", "%s: the offline interpreter disagrees with the independent oracle about validate_genes" % shown, "", key=key(part))
            return
        status, data = self.start(STUB_NAMESPACE, STUB_ID, {"genes": json.dumps(genes), "state_key_prefix": PREFIX})
        if not (status in (200, 201) and isinstance(data, dict) and data.get("id")):
            for part in parts:
                self.report.add("FAIL", "%s: the execution could not be created" % shown, "HTTP %s %s" % (status, snippet(data)), key=key(part))
            return
        execution = self.wait(data["id"])
        tasks = tasks_by_id(execution)
        self.report.info("%s [sources %s]: expected %s" % (shown, ",".join(sources), "STOPS at validate_genes" if stops else "passes validation"),
                         "observed state %s, validate_genes %s" % (state_of(execution), None if tasks is None else tasks.get("validate_genes")))
        if stops:
            self.report.check("%s: validate_genes stops the execution (expected FAILED at validate_genes)" % shown,
                              state_of(execution) == "FAILED" and tasks is not None and tasks.get("validate_genes") == ["FAILED"],
                              "state %s, validate_genes %s" % (state_of(execution), (tasks or {}).get("validate_genes")), key=key("validation"))
            kids = self.settle_children(data["id"], 0)
            name = "%s: no loop task and no LOOP sub-execution ever ran" % shown
            if tasks is None or kids is None:
                self.report.unverified(name, "the task list or the sub-execution search was not available", key=key("no_loop"))
            else:
                self.report.check(name, "watch_genes" not in tasks and not kids, "watch_genes runs %s, LOOP sub-executions %d" % (tasks.get("watch_genes"), len(kids)), key=key("no_loop"))
            return
        exp_iter = expected_iterations(genes)
        exp_render = [g.lower() for g in genes]
        exp_keys = [expected_key_for(v) for v in exp_iter]
        self.report.check("%s: validate_genes lets the list through (expected SUCCESS, validate_genes SUCCESS)" % shown,
                          state_of(execution) == "SUCCESS" and tasks is not None and tasks.get("validate_genes") == ["SUCCESS"],
                          "state %s, validate_genes %s" % (state_of(execution), (tasks or {}).get("validate_genes")), key=key("validation"))
        rows = self.logs(data["id"])
        rendered = [m.group(1) for r in (rows or []) for m in [re.match(r"VALUES=(.*)$", str(r.get("message", "")).strip(), re.S)] if m]
        name = "%s: Kestra renders the Loop values as the lower-cased JSON array %s" % (shown, json.dumps(exp_render))
        if rows is None:
            self.report.unverified(name, "the logs endpoint did not answer", key=key("render"))
        elif len(rendered) != 1:
            self.report.unverified(name, "found %d VALUES log line(s), expected 1" % len(rendered), key=key("render"))
        else:
            try:
                got = json.loads(rendered[0])
            except ValueError:
                got = None
            self.report.check(name, got == exp_render, "rendered text %r; parsed %r; predicted %r" % (rendered[0][:120], got, exp_render), key=key("render"))
        kids = self.settle_children(data["id"], len(exp_iter))
        obs = self.observe_children(kids) if kids is not None else None
        self.report.info("%s: expected iterations %s keys %s" % (shown, exp_iter, exp_keys),
                         "observed count %s; observed %s" % (None if kids is None else len(kids), None if obs is None else [(o["index"], o["value"], o["key"]) for o in obs]))
        name_count = "%s: number of LOOP sub-executions is %d (predicted)" % (shown, len(exp_iter))
        if kids is None:
            self.report.unverified(name_count, "the sub-execution search was not available", key=key("count"))
            for part in ("values", "keys", "key_rule"):
                self.report.unverified("%s: iteration %s" % (shown, part), "the sub-execution search was not available", key=key(part))
            return
        self.report.check(name_count, len(kids) == len(exp_iter), "observed %d" % len(kids), key=key("count"))
        name_values = "%s: iteration values and order are %s (loopRun and ITER log agree)" % (shown, exp_iter)
        if any(o["index"] is None or o["iter"] is None for o in obs):
            self.report.unverified(name_values, "a sub-execution lacks loopRun.index or its ITER log line: %s" % [(o["index"], o["iter"]) for o in obs], key=key("values"))
        else:
            ok = ([o["index"] for o in obs] == list(range(len(obs))) and [o["value"] for o in obs] == exp_iter
                  and [(int(o["iter"][0]), o["iter"][1]) for o in obs] == list(enumerate(exp_iter)))
            self.report.check(name_values, ok, "loopRun %s; ITER %s" % ([(o["index"], o["value"]) for o in obs], [o["iter"] for o in obs]), key=key("values"))
        name_keys = "%s: KEYCHECK keys are %s" % (shown, exp_keys)
        name_rule = "%s: every observed key is valid under Kestra's key rule (checked locally, no KV call, default prefix)" % shown
        if any(o["key"] is None for o in obs):
            self.report.unverified(name_keys, "a sub-execution has no readable KEYCHECK line", key=key("keys"))
            self.report.unverified(name_rule, "a sub-execution has no readable KEYCHECK line", key=key("key_rule"))
        else:
            keys = [o["key"] for o in obs]
            self.report.check(name_keys, keys == exp_keys, "observed %s; distinct %d of %d" % (keys, len(set(keys)), len(keys)), key=key("keys"))
            self.report.check(name_rule, all(bool(KV_RULE.fullmatch(k)) for k in keys), "observed validity %s" % [bool(KV_RULE.fullmatch(k)) for k in keys], key=key("key_rule"))

    def run_all(self, flow_text, expected_prefix):
        self.check_version(expected_prefix)
        deploy_text = disable_trigger(strip_gallery_metadata(flow_text))
        self.report.info("the full combined candidate is deployed only to be validated and created, never executed",
                         "extend: block removed, 'disabled: true' added; %d -> %d lines" % (flow_text.count("\n"), deploy_text.count("\n")))
        self.deploy("full combined candidate", deploy_text, "full")
        stub_text, values_expr = derive_stub(flow_text)
        self.report.info("the validation-only flow keeps the inputs, validate_genes and the Loop values verbatim", "values: %s" % values_expr)
        if not self.deploy("combined validation-only flow", stub_text, "stub"):
            return
        for case in CASES:
            self.run_case(case, COMBINED_CONDITION)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--expected-candidate-sha256", required=True)
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
    if actual != args.expected_candidate_sha256.lower():
        say("::error title=Wrong candidate::the candidate sha256 %s does not match the expected %s; stopping" % (actual, args.expected_candidate_sha256))
        return 2
    text = raw.decode("utf-8")
    validate, watch = extract_parts(text)
    if validate["condition"] != COMBINED_CONDITION or watch["values"] != COMBINED_VALUES:
        say("::error title=Unexpected expressions::the validate_genes condition or the Loop values differ from the reviewed combined text; stopping")
        return 2
    report = Report()
    driver = Driver(Client(args.base_url, user, password), report, float(os.environ.get("DRIVER_POLL_SECONDS", "2")))
    keys = expected_keys()
    try:
        if not driver.wait_for_server(args.startup_timeout):
            report.check("the Kestra server became ready", False, "no authenticated API answer within %ds" % args.startup_timeout)
        else:
            report.check("the Kestra server became ready", True, "API prefix %s" % driver.client.prefix)
            driver.run_all(text, args.expected_version_prefix)
    except Exception as exc:  # report, never a bare traceback; the checks already printed stay valid
        report.check("the driver finished without an internal error", False, "%s: %s" % (type(exc).__name__, exc))
    leaked = [x for x in driver.executed if x != (STUB_NAMESPACE, STUB_ID)]
    report.check("only the validation-only flow was ever executed (the full candidate never)", not leaked, "executions started: %d of %s" % (len(driver.executed), sorted(set(driver.executed))))
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
