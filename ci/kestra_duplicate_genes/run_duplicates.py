#!/usr/bin/env python3
"""Observe, in a REAL disposable Kestra 2.0.x server, how the Loop `values` expression handles duplicate and case-variant genes.

Two candidate files are compared: ORIGINAL (`values: "{{ inputs.genes }}"`) and PROPOSED (`values: "{{ inputs.genes | toJson | lower }}"`).
Everything else in them is identical (checked). For each, a derived VALIDATION-ONLY flow is built from the candidate's own YAML:
  * the `genes` and `state_key_prefix` inputs, the `validate_genes` task and the Loop `values` are copied VERBATIM (checked);
  * a parent Log task prints what Kestra renders for the `values` expression;
  * the Loop body is two harmless Log tasks: `read_snapshot` prints the KV key the real task would use (the real key expression, verbatim)
    and `iteration_probe` prints `item.index` and `item.value`;
  * only Log, Fail and Loop tasks exist: no HTTP, script, KV, Slack or Docker task, no trigger. Nothing can reach NCBI, Slack or any KV store.
The full candidates are only validated and created (schedule trigger disabled, gallery `extend:` block stripped; the PROPOSED one is deployed
under another namespace so the two can coexist). They are NEVER executed.

For every case and expression the EXPECTED observation comes from the offline model (pebble_subset.py + the reading of Kestra's
FlowableUtils.resolveValues: JSON-parse the rendered text, exact-string .distinct()); the ACTUAL observation comes from Kestra:
  rendered values (parent log), number of LOOP sub-executions, loopRun.index/value of each, ITER log lines, KEYCHECK keys.
A disagreement is reported as a FAIL. This driver never edits an expression to make a case pass.

Strict result handling: every check must be PASS. FAIL, UNVERIFIED (each also emits a ::warning) or a check that never ran fails the job,
as does a PASS count below --min-pass.
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
STUB_NAMESPACE = "ci.dupgenes"
STUB_IDS = {"original": "dup-genes-original", "proposed": "dup-genes-proposed"}
ALLOWED_STUB_TYPES = {"io.kestra.plugin.core.log.Log", "io.kestra.plugin.core.execution.Fail", "io.kestra.plugin.core.flow.Loop"}
PREFIX = "clinvar_snapshot"
SECRETS = []

# ---- cases ----------------------------------------------------------------------------------------------------------------------
REQUIRED_CASES = [["BRCA1"], ["brca1"], ["BRCA1", "brca1"], ["brca1", "BRCA1"], ["TP53", "EGFR"], ["TP53", "EGFR", "TP53"], ["BRCA1", "BRCA1"], ["BRCA1", "TP53", "brca1"]]
EDGE_VALID = [["MAX", "MAX"], ["MAX", "MAX", "MAX"], ["A", "B", "A", "B"], ["Max", "MAX"], ["max", "Max", "MAX"], ["HLA-A", "hla-a"], ["C1orf43", "C1ORF43"],
              ["TP53", "tp53", "EGFR", "egfr"], ["a.b_c", "A.B_C"], ["EGFR", "TP53"], ["BRCA1", "TP53", "BRCA1", "brca1", "TP53"], ["tp53", "TP53"], ["TP53", "tp53"]]
INVALID_CASES = [[], [""], [" "], ["A,B"], ["BRCA1", "brca1,tp53"], ["IGH@"]]
VALID_CASES = REQUIRED_CASES + EDGE_VALID
ALL_CASES = VALID_CASES + INVALID_CASES
EXPRESSIONS = ("original", "proposed")
VALID_PARTS = ("state", "render", "count", "values", "keys")
INVALID_PARTS = ("state", "no_loop")


def case_label(index, expression):
    """A label that is unique and safe on case-insensitive filesystems: never a gene name on its own."""
    return "case%02d_%s" % (index, expression)


def expected_keys():
    keys = ["version"] + ["%s_%s" % (e, part) for e in EXPRESSIONS for part in ("validates", "created")] + \
           ["%s_stub_%s" % (e, part) for e in EXPRESSIONS for part in ("validates", "created")]
    for e in EXPRESSIONS:
        for i, genes in enumerate(ALL_CASES):
            for part in (VALID_PARTS if genes in VALID_CASES else INVALID_PARTS):
                keys.append("%s %s %s" % (case_label(i, e), json.dumps(genes), part))
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
            say("::error title=Duplicate-gene run not accepted::" + annotation_escape(problem))
        return 1 if problems else 0

    def markdown(self, min_pass, keys):
        problems = self.problems(min_pass, keys)
        lines = ["## Kestra duplicate-gene results", "", "**Run accepted.**" if not problems else "**Run NOT accepted:** " + "; ".join(problems), "",
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


# ---- flows ----------------------------------------------------------------------------------------------------------------------
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


def retarget_namespace(flow_text, namespace):
    text, n = re.subn(r"(?m)^namespace:\s*\S+\s*$", "namespace: " + namespace, flow_text, count=1)
    if n != 1:
        raise ValueError("cannot rewrite the namespace")
    return text


def flow_identity(flow_text):
    ns = re.search(r"^namespace:\s*(\S+)\s*$", flow_text, re.M)
    fid = re.search(r"^id:\s*(\S+)\s*$", flow_text, re.M)
    if not ns or not fid:
        raise ValueError("cannot read the flow id and namespace")
    return ns.group(1), fid.group(1)


def values_of(doc):
    return next(t for t in doc["tasks"] if t["id"] == "watch_genes")["values"]


def derive_stub(candidate_text, label):
    """The validation-only flow for one candidate. Returns (yaml text, the candidate's `values` expression)."""
    doc = yaml.safe_load(candidate_text)
    inputs = {i["id"]: i for i in doc["inputs"]}
    validate = next(t for t in doc["tasks"] if t["id"] == "validate_genes")
    watch = next(t for t in doc["tasks"] if t["id"] == "watch_genes")
    real_get = next(t for t in watch["tasks"] if t["id"] == "read_snapshot")
    loop = json.loads(json.dumps(watch))
    loop.pop("outputs", None)
    loop["tasks"] = [{"id": "read_snapshot", "type": "io.kestra.plugin.core.log.Log", "message": "KEYCHECK " + real_get["key"]},
                     {"id": "iteration_probe", "type": "io.kestra.plugin.core.log.Log", "message": "ITER index={{ item.index }} value={{ item.value }}"}]
    tasks = [{"id": "probe_values_render", "type": "io.kestra.plugin.core.log.Log", "allowFailure": True, "message": "VALUES=" + watch["values"]},
             json.loads(json.dumps(validate)), loop]
    stub = {"id": STUB_IDS[label], "namespace": STUB_NAMESPACE, "inputs": [json.loads(json.dumps(inputs["genes"])), json.loads(json.dumps(inputs["state_key_prefix"]))],
            "tasks": tasks}
    text = yaml.safe_dump(stub, sort_keys=False, width=100000, allow_unicode=True)
    back = yaml.safe_load(text)
    assert next(t for t in back["tasks"] if t["id"] == "validate_genes") == validate, "validate_genes is not verbatim"
    assert values_of(back) == watch["values"], "the Loop values are not verbatim"
    assert back["inputs"] == [inputs["genes"], inputs["state_key_prefix"]], "the inputs are not verbatim"
    types = {t["type"] for t in back["tasks"]} | {t["type"] for t in back["tasks"][-1]["tasks"]}
    assert types <= ALLOWED_STUB_TYPES, "the stub contains a task type that is not allowed: %s" % sorted(types - ALLOWED_STUB_TYPES)
    return text, watch["values"]


# ---- the OFFLINE model (the expectation, never the evidence) ---------------------------------------------------------------------
def render_values(values_expr, genes):
    """Rendered `values` text. The ORIGINAL expression is ASSUMED to render a list as JSON array text (it did for ["MAX"] in the earlier live runs)."""
    if values_expr.strip() == "{{ inputs.genes }}":
        return json.dumps(genes, separators=(",", ":"), ensure_ascii=False)
    value, _, _ = ps.evaluate(values_expr, {"inputs": {"genes": list(genes)}})
    return value


def expected_iterations(values_expr, genes):
    """Kestra v2.0.6 FlowableUtils.resolveValues, string-array branch: JSON-parse the rendered text, exact-string .distinct(), order kept."""
    seen, out = set(), []
    for v in json.loads(render_values(values_expr, genes)):
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def expected_rendered_list(values_expr, genes):
    return json.loads(render_values(values_expr, genes))


def predicted_stop(condition, genes):
    return ps.evaluate(condition, {"inputs": {"genes": list(genes)}})[0]


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
        self.executed = []  # (namespace, flow id) of every execution started; only the two validation-only flows may ever appear

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
            self.report.check("Kestra validates the %s" % label, not problems, snippet(problems) if problems else "no constraint violation", key=key_prefix + "_validates")
        else:
            self.report.unverified("Kestra validates the %s" % label, "HTTP %s: %s" % (status, snippet(data)), key=key_prefix + "_validates")
        status, data = self.client.json("POST", self.client.prefix + "/flows", flow_text.encode("utf-8"), "application/x-yaml")
        created = self.report.check("the %s is created (%s.%s)" % (label, ns, fid), status == 200, "HTTP %s %s" % (status, "" if status == 200 else snippet(data)), key=key_prefix + "_created")
        return ns, fid, created

    def start(self, ns, fid, fields):
        assert ns == STUB_NAMESPACE and fid in STUB_IDS.values(), "only the validation-only flows may ever be executed"
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
        """The parent is terminal, so the loop has finished. Wait until the number of sub-executions is stable over a settle period, all
        are terminal, and (for expected > 0) at least `expected` exist. Counting a stable number guards against late extra iterations."""
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
        """For every sub-execution: loopRun.index/value, the ITER log line, the KEYCHECK key, the task states."""
        seen = []
        for row in rows:
            child = self.fetch(row["id"]) or {}
            loop_run = child.get("loopRun") if isinstance(child.get("loopRun"), dict) else {}
            logs = self.logs(row["id"])
            iter_lines = [m.groups() for r in (logs or []) for m in [re.match(r"ITER index=(\d+) value=(.*)$", str(r.get("message", "")).strip())] if m]
            keys = [m.group(1) for r in (logs or []) for m in [re.match(r"KEYCHECK (\S*)$", str(r.get("message", "")).strip())] if m]
            tasks = tasks_by_id(child) or {}
            seen.append({"id": row["id"], "index": loop_run.get("index"), "value": loop_run.get("value"), "iter": iter_lines[0] if len(iter_lines) == 1 else None,
                         "key": keys[0] if len(keys) == 1 else None, "state": state_of(child), "tasks": {t: s for t, s in tasks.items()}, "logs_ok": logs is not None})
        return sorted(seen, key=lambda s: (s["index"] is None, s["index"] if isinstance(s["index"], int) else 0))

    def run_case(self, index, genes, expression, values_expr, condition):
        label = case_label(index, expression)
        key = lambda part: "%s %s %s" % (label, json.dumps(genes), part)
        status, data = self.start(STUB_NAMESPACE, STUB_IDS[expression], {"genes": json.dumps(genes), "state_key_prefix": PREFIX})
        stops = predicted_stop(condition, genes)
        expect_invalid = genes in INVALID_CASES
        if stops != expect_invalid:
            for part in (INVALID_PARTS if expect_invalid else VALID_PARTS):
                self.report.add("FAIL", "%s: the offline prediction of validate_genes disagrees with the case table" % label, "", key=key(part))
            return
        if not (status in (200, 201) and isinstance(data, dict) and data.get("id")):
            for part in (INVALID_PARTS if expect_invalid else VALID_PARTS):
                self.report.add("FAIL", "%s %s: the execution could not be created" % (label, json.dumps(genes)), "HTTP %s %s" % (status, snippet(data)), key=key(part))
            return
        execution = self.wait(data["id"])
        tasks = tasks_by_id(execution)
        if expect_invalid:
            self.report.check("%s %s: execution ends FAILED (validate_genes stops it)" % (label, json.dumps(genes)),
                              state_of(execution) == "FAILED" and tasks is not None and tasks.get("validate_genes") == ["FAILED"],
                              "state %s, validate_genes %s" % (state_of(execution), (tasks or {}).get("validate_genes")), key=key("state"))
            kids = self.settle_children(data["id"], 0)
            name = "%s %s: no loop task and no LOOP sub-execution ever ran" % (label, json.dumps(genes))
            if tasks is None or kids is None:
                self.report.unverified(name, "the task list or the sub-execution search was not available", key=key("no_loop"))
            else:
                self.report.check(name, "watch_genes" not in tasks and not kids, "watch_genes runs %s, LOOP sub-executions %d" % (tasks.get("watch_genes"), len(kids)), key=key("no_loop"))
            return
        # valid list
        exp_iter = expected_iterations(values_expr, genes)
        exp_keys = ["%s_%s" % (PREFIX, v.strip().lower()) for v in exp_iter]
        self.report.check("%s %s: execution ends SUCCESS" % (label, json.dumps(genes)), state_of(execution) == "SUCCESS", "state %s" % state_of(execution), key=key("state"))
        rows = self.logs(data["id"])
        rendered = [m.group(1) for r in (rows or []) for m in [re.match(r"VALUES=(.*)$", str(r.get("message", "")).strip(), re.S)] if m]
        name = "%s %s: Kestra renders the Loop values as the model predicts" % (label, json.dumps(genes))
        if rows is None:
            self.report.unverified(name, "the logs endpoint did not answer", key=key("render"))
        elif len(rendered) != 1:
            self.report.unverified(name, "found %d VALUES log line(s), expected 1" % len(rendered), key=key("render"))
        else:
            try:
                got = json.loads(rendered[0])
            except ValueError:
                got = None
            want = expected_rendered_list(values_expr, genes)
            self.report.check(name, got == want, "rendered text %r; parsed %r; predicted %r" % (rendered[0][:120], got, want), key=key("render"))
        kids = self.settle_children(data["id"], len(exp_iter))
        obs = self.observe_children(kids) if kids is not None else None
        self.report.info("%s %s [%s]: expected iterations %s keys %s" % (label, json.dumps(genes), expression, exp_iter, exp_keys),
                         "observed count %s; observed %s" % (None if kids is None else len(kids), None if obs is None else [(o["index"], o["value"], o["key"]) for o in obs]))
        name_count = "%s %s: number of LOOP sub-executions is %d (predicted)" % (label, json.dumps(genes), len(exp_iter))
        if kids is None:
            self.report.unverified(name_count, "the sub-execution search was not available", key=key("count"))
            for part in ("values", "keys"):
                self.report.unverified("%s %s: iteration %s" % (label, json.dumps(genes), part), "the sub-execution search was not available", key=key(part))
            return
        self.report.check(name_count, len(kids) == len(exp_iter), "observed %d" % len(kids), key=key("count"))
        name_values = "%s %s: iteration values and order are %s (loopRun and ITER log agree)" % (label, json.dumps(genes), exp_iter)
        if any(o["index"] is None or o["iter"] is None for o in obs):
            self.report.unverified(name_values, "a sub-execution lacks loopRun.index or its ITER log line: %s" % [(o["index"], o["iter"]) for o in obs], key=key("values"))
        else:
            ok = ([o["index"] for o in obs] == list(range(len(obs))) and [o["value"] for o in obs] == exp_iter
                  and [(int(o["iter"][0]), o["iter"][1]) for o in obs] == list(enumerate(exp_iter)))
            self.report.check(name_values, ok, "loopRun %s; ITER %s" % ([(o["index"], o["value"]) for o in obs], [o["iter"] for o in obs]), key=key("values"))
        name_keys = "%s %s: KEYCHECK keys are %s and valid under Kestra's rule" % (label, json.dumps(genes), exp_keys)
        if any(o["key"] is None for o in obs):
            self.report.unverified(name_keys, "a sub-execution has no readable KEYCHECK line", key=key("keys"))
        else:
            keys = [o["key"] for o in obs]
            self.report.check(name_keys, keys == exp_keys and all(KV_RULE.fullmatch(k) for k in keys),
                              "observed %s; distinct %d of %d" % (keys, len(set(keys)), len(keys)), key=key("keys"))

    def run_all(self, texts, expected_prefix):
        self.check_version(expected_prefix)
        stubs = {}
        for label in EXPRESSIONS:
            text = texts[label]
            deploy_text = disable_trigger(strip_gallery_metadata(text))
            if label == "proposed":
                deploy_text = retarget_namespace(deploy_text, "ci.dupgenes.fullproposed")
            self.report.info("the full %s candidate is deployed only to be validated and created, never executed" % label,
                             "extend: block removed, 'disabled: true' added%s; %d -> %d lines" % ("; namespace retargeted so both can coexist" if label == "proposed" else "", text.count("\n"), deploy_text.count("\n")))
            self.deploy("full %s candidate" % label, deploy_text, label)
            stub_text, values_expr = derive_stub(text, label)
            self.report.info("the %s validation-only flow keeps inputs, validate_genes and Loop values verbatim" % label, "values: %s" % values_expr)
            ns, fid, created = self.deploy("%s validation-only flow" % label, stub_text, label + "_stub")
            stubs[label] = (created, values_expr, yaml.safe_load(texts[label]))
        for label in EXPRESSIONS:
            created, values_expr, doc = stubs[label]
            if not created:
                continue
            condition = next(t for t in doc["tasks"] if t["id"] == "validate_genes")["condition"]
            for index, genes in enumerate(ALL_CASES):
                self.run_case(index, genes, label, values_expr, condition)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", required=True)
    parser.add_argument("--proposed", required=True)
    parser.add_argument("--expected-original-sha256", required=True)
    parser.add_argument("--expected-proposed-sha256", required=True)
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
    raws = {"original": Path(args.original).read_bytes(), "proposed": Path(args.proposed).read_bytes()}
    for label, expected in (("original", args.expected_original_sha256), ("proposed", args.expected_proposed_sha256)):
        actual = hashlib.sha256(raws[label]).hexdigest()
        if actual != expected.lower():
            say("::error title=Wrong candidate::the %s candidate sha256 %s does not match the expected %s; stopping" % (label, actual, expected))
            return 2
    texts = {k: v.decode("utf-8") for k, v in raws.items()}
    a = [ln for ln in texts["original"].splitlines()]
    b = [ln for ln in texts["proposed"].splitlines()]
    differing = [(x, y) for x, y in zip(a, b) if x != y]
    if len(a) != len(b) or len(differing) != 1 or differing[0][0].strip() != 'values: "{{ inputs.genes }}"' or differing[0][1].strip() != 'values: "{{ inputs.genes | toJson | lower }}"':
        say("::error title=Unexpected difference::the proposed candidate must differ from the original only in the Loop values line; stopping")
        return 2
    report = Report()
    driver = Driver(Client(args.base_url, user, password), report, float(os.environ.get("DRIVER_POLL_SECONDS", "2")))
    keys = expected_keys()
    try:
        if not driver.wait_for_server(args.startup_timeout):
            report.check("the Kestra server became ready", False, "no authenticated API answer within %ds" % args.startup_timeout)
        else:
            report.check("the Kestra server became ready", True, "API prefix %s" % driver.client.prefix)
            driver.run_all(texts, args.expected_version_prefix)
    except Exception as exc:  # report, never a bare traceback; the checks already printed stay valid
        report.check("the driver finished without an internal error", False, "%s: %s" % (type(exc).__name__, exc))
    leaked = [x for x in driver.executed if x[0] != STUB_NAMESPACE or x[1] not in STUB_IDS.values()]
    report.check("no full candidate flow was ever executed", not leaked, "executions started: %s" % sorted(set(driver.executed)))
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
