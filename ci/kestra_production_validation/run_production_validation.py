#!/usr/bin/env python3
"""PREPARED, NOT EXECUTED AGAINST KESTRA. Observe, in a REAL disposable Kestra 2.0.x server, what the ORIGINAL production `validate_genes` condition does.

The production flow is NOT changed and is NOT executed. A derived VALIDATION-ONLY flow is built from the production YAML:
  * the `genes` and `state_key_prefix` inputs, the `validate_genes` task (condition and message) and the Loop `values` are copied VERBATIM (checked);
  * a parent Log task prints what Kestra renders for the `values` expression;
  * the Loop body is two harmless Log tasks: `read_snapshot` prints the KV key the real task would use (the real key expression, verbatim) and
    `iteration_probe` prints `item.index` and `item.value`;
  * only Log, Fail and Loop tasks exist: no HTTP, script, KV, Slack or Docker task, no trigger. Nothing can reach NCBI, Slack or any KV store.

For every case the EXPECTED result comes from the offline model (pebble_subset.py + the reading of Kestra's FlowableUtils.resolveValues); the ACTUAL
result comes from Kestra. A disagreement is a FAIL. Whether an entry yields a valid KV key is a SEPARATE property from passing validate_genes: it is
judged by matching the observed key against Kestra's key rule (KVStore.KEY_VALIDATOR_PATTERN, v2.0.6, source reading) LOCALLY. No KV store is touched,
so the rule itself is not exercised on Kestra.
The Batch 5 candidate's comparison result is carried in the case table as documentation only; it is not tested here.

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
STUB_NAMESPACE = "ci.prodvalidation"
STUB_ID = "production-validate-genes"
ALLOWED_STUB_TYPES = {"io.kestra.plugin.core.log.Log", "io.kestra.plugin.core.execution.Fail", "io.kestra.plugin.core.flow.Loop"}
PREFIX = "clinvar_snapshot"
SECRETS = []

# The exact production text this test is about (checked against the production file; see extract_production_parts).
PRODUCTION_CONDITION = "{{ ((inputs.genes | join(',')) | replace({'[A-Za-z0-9._@-]+': 'g'}, regexp=true) | replace({'g,': ''})) != 'g' }}"
PRODUCTION_CONDITION_SHA256 = hashlib.sha256(PRODUCTION_CONDITION.encode("utf-8")).hexdigest()
PRODUCTION_VALUES = "{{ inputs.genes }}"

# ---- cases ----------------------------------------------------------------------------------------------------------------------
# (id, genes, group, production_stops, batch5_stops (comparison only), key_valid_if_it_reaches_the_loop)
# key_valid is judged on the key the production key expression would build: <prefix>_<entry trimmed and lower-cased>. None where validation stops.
CASES = [
    (1, ["BRCA1"], "valid", False, False, True),
    (2, ["brca1", "TP53", "EGFR"], "valid", False, False, True),
    (3, ["HLA-A"], "valid", False, False, True),
    (4, ["C1orf43"], "valid", False, False, True),
    (5, ["a.b_c"], "valid", False, False, True),
    (6, ["BRCA1", "brca1"], "valid", False, False, True),
    (7, [], "rejected by both", True, True, None),
    (8, [""], "rejected by both", True, True, None),
    (9, [" "], "rejected by both", True, True, None),
    (10, ["BRCA1", ""], "rejected by both", True, True, None),
    (11, ["BRCA1", " TP53"], "rejected by both", True, True, None),
    (12, ["TP53!"], "rejected by both", True, True, None),
    (13, ["TP/53"], "rejected by both", True, True, None),
    (14, ["TP53\n"], "rejected by both", True, True, None),
    (15, ["A,B"], "predicted production gap", False, True, False),
    (16, ["BRCA1", "brca1,tp53"], "predicted production gap", False, True, False),
    (17, ["BRCA1,TP53"], "predicted production gap", False, True, False),
    (18, ["IGH@"], "predicted production gap", False, True, False),
    (19, ["_TP53"], "valid key (prefix and separator come first)", False, False, True),
    (20, ["-TP53"], "valid key (prefix and separator come first)", False, False, True),
    # the literal reading of two lines of the Phase 8B request: ONE entry that contains commas
    (21, ["brca1,TP53,EGFR"], "supplementary: one entry with commas", False, True, False),
    (22, ["BRCA1,brca1"], "supplementary: one entry with a comma", False, True, False),
]
CASE_IDS = [c[0] for c in CASES]
PASSING = [c for c in CASES if not c[3]]
STOPPING = [c for c in CASES if c[3]]
PASS_PARTS = ("validation", "render", "count", "values", "keys", "key_rule")
STOP_PARTS = ("validation", "no_loop")


def case_label(case_id):
    """Unique and safe on case-insensitive filesystems: never a gene name on its own."""
    return "case%02d" % case_id


def expected_keys():
    keys = ["version", "stub_validates", "stub_created"]
    for cid, genes, _, stops, _, _ in CASES:
        for part in (STOP_PARTS if stops else PASS_PARTS):
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
            say("::error title=Production validate_genes run not accepted::" + annotation_escape(problem))
        return 1 if problems else 0

    def markdown(self, min_pass, keys):
        problems = self.problems(min_pass, keys)
        lines = ["## Kestra production validate_genes results", "", "**Run accepted.**" if not problems else "**Run NOT accepted:** " + "; ".join(problems), "",
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


# ---- the production file and the derived flow -----------------------------------------------------------------------------------
def extract_production_parts(flow_text):
    """Return (validate task dict, condition text, line number of the condition, Loop task dict). Nothing is rewritten."""
    doc = yaml.safe_load(flow_text)
    validate = next(t for t in doc["tasks"] if t["id"] == "validate_genes")
    watch = next(t for t in doc["tasks"] if t["id"] == "watch_genes")
    lines = flow_text.replace("\r\n", "\n").split("\n")
    at = [i + 1 for i, ln in enumerate(lines) if ln.startswith("    condition:") and validate["condition"] in ln]
    return validate, validate["condition"], (at[0] if len(at) == 1 else None), watch


def derive_stub(flow_text):
    """The validation-only flow. Returns (yaml text, values expression)."""
    doc = yaml.safe_load(flow_text)
    inputs = {i["id"]: i for i in doc["inputs"]}
    validate, _, _, watch = extract_production_parts(flow_text)
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
    assert next(t for t in back["tasks"] if t["id"] == "validate_genes")["condition"] == PRODUCTION_CONDITION, "the condition is not the expected production condition"
    assert next(t for t in back["tasks"] if t["id"] == "watch_genes")["values"] == watch["values"] == PRODUCTION_VALUES, "the Loop values are not verbatim"
    assert back["inputs"] == [inputs["genes"], inputs["state_key_prefix"]], "the inputs are not verbatim"
    types = {t["type"] for t in back["tasks"]} | {t["type"] for t in back["tasks"][-1]["tasks"]}
    assert types <= ALLOWED_STUB_TYPES, "the stub contains a task type that is not allowed: %s" % sorted(types - ALLOWED_STUB_TYPES)
    return text, watch["values"]


# ---- the OFFLINE model (the expectation, never the evidence) ---------------------------------------------------------------------
def predicted_stop(condition, genes):
    return bool(ps.evaluate(condition, {"inputs": {"genes": list(genes)}})[0])


def expected_iterations(genes):
    """Kestra v2.0.6 FlowableUtils.resolveValues, string-array branch: JSON-parse the rendered text, exact-string .distinct(), order kept."""
    seen, out = set(), []
    for v in genes:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def expected_key_for(entry):
    return "%s_%s" % (PREFIX, entry.strip().lower())


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
        created = self.report.check("the %s is created (%s.%s)" % (label, ns, fid), status == 200, "HTTP %s %s" % (status, "" if status == 200 else snippet(data)), key=key_prefix + "_created")
        return created

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

    def run_case(self, case):
        cid, genes, group, stops, batch5_stops, key_valid = case
        label = case_label(cid)
        shown = "%s %s" % (label, json.dumps(genes))
        key = lambda part: "%s %s %s" % (label, json.dumps(genes), part)
        parts = STOP_PARTS if stops else PASS_PARTS
        if predicted_stop(PRODUCTION_CONDITION, genes) != stops:
            for part in parts:
                self.report.add("FAIL", "%s: the offline interpreter disagrees with the case table about validate_genes" % shown, "", key=key(part))
            return
        status, data = self.start(STUB_NAMESPACE, STUB_ID, {"genes": json.dumps(genes), "state_key_prefix": PREFIX})
        if not (status in (200, 201) and isinstance(data, dict) and data.get("id")):
            for part in parts:
                self.report.add("FAIL", "%s: the execution could not be created" % shown, "HTTP %s %s" % (status, snippet(data)), key=key(part))
            return
        execution = self.wait(data["id"])
        tasks = tasks_by_id(execution)
        self.report.info("%s [%s]: expected production %s; Batch 5 comparison (not tested) %s" % (shown, group, "STOPS" if stops else "passes", "STOPS" if batch5_stops else "passes"),
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
        # production validation is predicted to let this list through to the Loop
        exp_iter = expected_iterations(genes)
        exp_keys = [expected_key_for(v) for v in exp_iter]
        self.report.check("%s: validate_genes lets the list through (expected SUCCESS, validate_genes SUCCESS)" % shown,
                          state_of(execution) == "SUCCESS" and tasks is not None and tasks.get("validate_genes") == ["SUCCESS"],
                          "state %s, validate_genes %s" % (state_of(execution), (tasks or {}).get("validate_genes")), key=key("validation"))
        rows = self.logs(data["id"])
        rendered = [m.group(1) for r in (rows or []) for m in [re.match(r"VALUES=(.*)$", str(r.get("message", "")).strip(), re.S)] if m]
        name = "%s: Kestra renders the Loop values as the JSON array the model predicts" % shown
        if rows is None:
            self.report.unverified(name, "the logs endpoint did not answer", key=key("render"))
        elif len(rendered) != 1:
            self.report.unverified(name, "found %d VALUES log line(s), expected 1" % len(rendered), key=key("render"))
        else:
            try:
                got = json.loads(rendered[0])
            except ValueError:
                got = None
            self.report.check(name, got == genes, "rendered text %r; parsed %r; predicted %r" % (rendered[0][:120], got, genes), key=key("render"))
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
        name_rule = "%s: %s under Kestra's key rule (checked locally, no KV call)" % (shown, "every observed key is valid" if key_valid else "at least one observed key is invalid")
        if any(o["key"] is None for o in obs):
            self.report.unverified(name_keys, "a sub-execution has no readable KEYCHECK line", key=key("keys"))
            self.report.unverified(name_rule, "a sub-execution has no readable KEYCHECK line", key=key("key_rule"))
        else:
            keys = [o["key"] for o in obs]
            self.report.check(name_keys, keys == exp_keys, "observed %s" % keys, key=key("keys"))
            self.report.check(name_rule, all(bool(KV_RULE.fullmatch(k)) for k in keys) == key_valid,
                              "observed validity %s, expected %s for the list as a whole" % ([bool(KV_RULE.fullmatch(k)) for k in keys], "all valid" if key_valid else "at least one invalid"), key=key("key_rule"))

    def run_all(self, flow_text, expected_prefix):
        self.check_version(expected_prefix)
        stub_text, values_expr = derive_stub(flow_text)
        self.report.info("the validation-only flow keeps the inputs, validate_genes and the Loop values verbatim", "values: %s; condition sha256 %s" % (values_expr, PRODUCTION_CONDITION_SHA256[:16]))
        if not self.deploy("production validation-only flow", stub_text, "stub"):
            return
        for case in CASES:
            self.run_case(case)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--production", required=True)
    parser.add_argument("--expected-production-sha256", required=True)
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
    raw = Path(args.production).read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != args.expected_production_sha256.lower():
        say("::error title=Wrong flow::the production flow sha256 %s does not match the expected %s; stopping" % (actual, args.expected_production_sha256))
        return 2
    text = raw.decode("utf-8")
    _, condition, _, watch = extract_production_parts(text)
    if condition != PRODUCTION_CONDITION or watch["values"] != PRODUCTION_VALUES:
        say("::error title=Unexpected condition::the validate_genes condition or the Loop values differ from the reviewed production text; stopping")
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
    report.check("only the validation-only flow was ever executed", not leaked, "executions started: %d of %s" % (len(driver.executed), sorted(set(driver.executed))))
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
