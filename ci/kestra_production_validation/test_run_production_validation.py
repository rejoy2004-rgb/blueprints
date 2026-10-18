"""Offline tests of the production-validation driver (run_production_validation.py) and of its workflow. NOT a test of Kestra.

FakeKestra is a small HTTP server on 127.0.0.1 whose "engine" is the offline model (the interpreter plus the reading of Kestra's resolveValues).
Its switches make it behave like engines that DISAGREE with the model, so these tests show the driver would notice.
Only a real Kestra run can show how Kestra actually evaluates validate_genes.

Run: python -I test_run_production_validation.py     (needs PyYAML only)
"""
import base64
import hashlib
import io
import json
import os
import re
import sys
import threading
import unittest
import urllib.parse
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import pebble_subset as ps  # noqa: E402
import run_production_validation as rv  # noqa: E402

PRODUCTION = HERE / "production" / "clinvar-variant-reclassification-monitor.yaml"
PRODUCTION_SHA = "9ce300b17caf749e3033fa004d8f6749568ab5b141ee66ab0ceb7c27780394cb"
WORKFLOW = HERE.parent.parent / ".github" / "workflows" / "kestra-production-validation.yml"
USER, PASSWORD = "ci@example.com", "Ci1fakepasswordforthetests"
FAST = {"DRIVER_POLL_SECONDS": "0.005", "DRIVER_CHILD_SETTLE": "0.03", "DRIVER_CHILD_TIMEOUT": "5", "DRIVER_EXEC_TIMEOUT": "10"}
# The stricter condition of the Batch 5 candidate, used ONLY to make a fake engine behave differently from production.
STRICT_CONDITION = ("{{ ((((inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true)) | replace({'g,': ''})) != 'g') or "
                    "((((((inputs.genes | join(',')) | replace({'[A-Za-z0-9._-]+': 'g'}, regexp=true)) | replace({',': ''})) | length) ~ '') != ((inputs.genes | length) ~ '')) }}")


class FakeKestra:
    """Switches: condition (the text the engine evaluates; default = the deployed flow's own), distinct (drop exact duplicates), render ('json'|'java'),
    extra_late_child, drop_loop_run, search ('ok'|'error'), keycheck_override (a different key prefix), sanitize_keys (the engine silently repairs keys),
    leak_invalid (the Loop runs even when validate_genes fails), execute_blocked."""

    def __init__(self, version="2.0.6", condition=None, distinct=True, render="json", extra_late_child=False, drop_loop_run=False, search="ok",
                 keycheck_override=None, sanitize_keys=False, leak_invalid=False, execute_blocked=False):
        self.version, self.condition, self.distinct, self.render = version, condition, distinct, render
        self.extra_late_child, self.drop_loop_run, self.search = extra_late_child, drop_loop_run, search
        self.keycheck_override, self.sanitize_keys, self.leak_invalid, self.execute_blocked = keycheck_override, sanitize_keys, leak_invalid, execute_blocked
        self.flows, self.executions, self.children, self.requests, self.foreign_executions = {}, {}, {}, 0, 0
        self.lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                fake.handle(self, "GET")

            def do_POST(self):
                fake.handle(self, "POST")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def send(self, h, status, body):
        data = json.dumps(body).encode()
        h.send_response(status)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(data)))
        h.end_headers()
        h.wfile.write(data)

    def handle(self, h, method):
        with self.lock:
            self.requests += 1
            parsed = urllib.parse.urlsplit(h.path)
            path, query = parsed.path, urllib.parse.parse_qs(parsed.query)
            body = h.rfile.read(int(h.headers.get("Content-Length") or 0)) if h.headers.get("Content-Length") else b""
            p = "/api/v1/main"
            if path in ("/api/v1/configs", p + "/configs"):
                return self.send(h, 200, {"version": self.version}) if self.version else self.send(h, 404, {})
            if h.headers.get("Authorization") != "Basic " + base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode():
                return self.send(h, 401, {})
            if path == p + "/flows/search":
                return self.send(h, 200, {"results": []})
            if method == "POST" and path == p + "/flows/validate":
                return self.send(h, 200, [{"constraints": None}])
            if method == "POST" and path == p + "/flows":
                text = body.decode()
                ns, fid = re.search(r"(?m)^namespace:\s*(\S+)", text).group(1), re.search(r"(?m)^id:\s*(\S+)", text).group(1)
                if (ns, fid) in self.flows:
                    return self.send(h, 422, {"detail": "already exists"})
                self.flows[(ns, fid)] = yaml.safe_load(text)
                return self.send(h, 200, {"id": fid})
            m = re.fullmatch(re.escape(p) + r"/executions/([^/]+)/([^/]+)", path)
            if method == "POST" and m:
                return self.execute(h, m.group(1), m.group(2), body)
            if method == "GET" and path == p + "/executions/search":
                if self.search == "error":
                    return self.send(h, 500, {})
                kind = (query.get("filters[kind][EQUALS]") or [None])[0]
                parent = (query.get("filters[parentId][EQUALS]") or [None])[0]
                rows = [{"id": cid, "parentId": c["parentId"], "kind": "LOOP"} for cid, c in self.children.items() if kind == "LOOP" and c["parentId"] == parent]
                return self.send(h, 200, {"results": rows})
            m = re.fullmatch(re.escape(p) + r"/executions/([^/]+)", path)
            if method == "GET" and m:
                ex = self.executions.get(m.group(1)) or self.children.get(m.group(1))
                return self.send(h, 200, ex["json"]) if ex else self.send(h, 404, {})
            m = re.fullmatch(re.escape(p) + r"/logs/([^/]+)", path)
            if method == "GET" and m:
                ex = self.executions.get(m.group(1)) or self.children.get(m.group(1))
                return self.send(h, 200, ex["logs"]) if ex else self.send(h, 404, {})
            return self.send(h, 404, {"message": "no route"})

    def execute(self, h, ns, fid, body):
        if self.execute_blocked or (ns, fid) not in self.flows or (ns, fid) != (rv.STUB_NAMESPACE, rv.STUB_ID):
            self.foreign_executions += 1
            return self.send(h, 403 if self.execute_blocked else 404, {})
        fields = {k.decode(): v.decode() for k, v in re.findall(rb'name="([^"]+)"\r\n\r\n(.*?)\r\n--', body, re.S)}
        genes, prefix = json.loads(fields["genes"]), fields.get("state_key_prefix", "")
        flow = self.flows[(ns, fid)]
        validate = next(t for t in flow["tasks"] if t["id"] == "validate_genes")
        stops = ps.evaluate(self.condition or validate["condition"], {"inputs": {"genes": genes}})[0]
        eid = "ex%04d" % (len(self.executions) + 1)
        runs, logs = [], []
        reaches_loop = (not stops) or self.leak_invalid
        if reaches_loop:
            text = json.dumps(genes, separators=(",", ":")) if self.render == "json" else "[" + ", ".join(genes) + "]"
            logs.append({"message": "VALUES=" + text, "taskId": "probe_values_render"})
            runs.append({"taskId": "probe_values_render", "state": {"current": "SUCCESS"}})
        runs.append({"taskId": "validate_genes", "state": {"current": "FAILED" if stops else "SUCCESS"}})
        if reaches_loop:
            runs.append({"taskId": "watch_genes", "state": {"current": "SUCCESS"}})
            items = rv.expected_iterations(genes) if self.distinct else list(genes)
            if self.extra_late_child:
                items.append(items[-1] + "-extra")
            for index, value in enumerate(items):
                cid = "%s-c%d" % (eid, index)
                key = "%s_%s" % (self.keycheck_override or prefix, value.strip().lower())
                if self.sanitize_keys:
                    key = re.sub(r"[^A-Za-z0-9._-]", "_", key)
                cjson = {"id": cid, "kind": "LOOP", "parentId": eid, "state": {"current": "SUCCESS"}}
                if not self.drop_loop_run:
                    cjson["loopRun"] = {"index": index, "taskId": "watch_genes", "taskRunId": "t", "value": value, "parent": {}}
                self.children[cid] = {"parentId": eid, "json": cjson, "logs": [{"message": "KEYCHECK " + key, "taskId": "read_snapshot"},
                                                                                {"message": "ITER index=%d value=%s" % (index, value), "taskId": "iteration_probe"}]}
        self.executions[eid] = {"json": {"id": eid, "state": {"current": "FAILED" if stops else "SUCCESS"}, "taskRunList": runs}, "logs": logs}
        return self.send(h, 200, {"id": eid})


def run_main(fake, extra=(), env=None, production=PRODUCTION, sha=PRODUCTION_SHA, base=None):
    old = {k: os.environ.get(k) for k in list(FAST) + ["KESTRA_USER", "KESTRA_PASSWORD", "GITHUB_STEP_SUMMARY"]}
    os.environ.update(FAST)
    os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD})
    os.environ.pop("GITHUB_STEP_SUMMARY", None)
    os.environ.update(env or {})
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = rv.main(["--production", str(production), "--expected-production-sha256", sha, "--base-url", base or fake.url, "--startup-timeout", "3", *extra])
    finally:
        for k, v in old.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        rv.SECRETS.clear()
    return code, buffer.getvalue()


class DriverTests(unittest.TestCase):
    def fake(self, **kw):
        f = FakeKestra(**kw)
        self.addCleanup(f.close)
        return f

    def lines(self, out, status):
        return [ln for ln in out.splitlines() if ln.startswith(status)]

    def fails(self, out, *needles):
        return [ln for ln in self.lines(out, "FAIL") if all(n in ln for n in needles)]

    def test_happy_path_accepts_the_run_and_every_expected_key_passes(self):
        fake = self.fake()
        code, out = run_main(fake)
        self.assertEqual(code, 0, out)
        self.assertEqual(self.lines(out, "FAIL") + self.lines(out, "UNVERIFIED"), [], out)
        self.assertGreaterEqual(len(self.lines(out, "PASS")), len(rv.expected_keys()))
        self.assertEqual(fake.foreign_executions, 0)

    def test_the_expected_check_count_is_documented_and_unique(self):
        keys = rv.expected_keys()
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(keys), 3 + len(rv.PASSING) * len(rv.PASS_PARTS) + len(rv.STOPPING) * len(rv.STOP_PARTS))
        self.assertEqual((len(rv.CASES), len(rv.PASSING), len(rv.STOPPING), len(keys)), (22, 14, 8, 103))
        self.assertEqual(int(yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["env"]["MIN_PASS"]), len(keys))

    def test_the_twenty_requested_cases_are_present_in_order_plus_two_supplementary(self):
        by_id = {c[0]: c[1] for c in rv.CASES}
        self.assertEqual([by_id[i] for i in (1, 3, 4, 5, 6)], [["BRCA1"], ["HLA-A"], ["C1orf43"], ["a.b_c"], ["BRCA1", "brca1"]])
        self.assertEqual(by_id[2], ["brca1", "TP53", "EGFR"])
        self.assertEqual([by_id[i] for i in range(7, 15)], [[], [""], [" "], ["BRCA1", ""], ["BRCA1", " TP53"], ["TP53!"], ["TP/53"], ["TP53\n"]])
        self.assertEqual([by_id[i] for i in range(15, 21)], [["A,B"], ["BRCA1", "brca1,tp53"], ["BRCA1,TP53"], ["IGH@"], ["_TP53"], ["-TP53"]])
        self.assertEqual([by_id[21], by_id[22]], [["brca1,TP53,EGFR"], ["BRCA1,brca1"]])

    def test_case_labels_are_unique_and_safe_on_case_insensitive_filesystems(self):
        labels = [rv.case_label(i) for i in rv.CASE_IDS]
        self.assertEqual(len(labels), len({lb.lower() for lb in labels}))
        self.assertTrue(all(re.fullmatch(r"case\d\d", lb) for lb in labels))

    def test_the_case_table_agrees_with_the_offline_interpreter_for_the_production_condition(self):
        _, condition, line, _ = rv.extract_production_parts(PRODUCTION.read_text(encoding="utf-8"))
        self.assertEqual(condition, rv.PRODUCTION_CONDITION)
        self.assertEqual(line, 97)
        for cid, genes, group, stops, batch5, key_valid in rv.CASES:
            self.assertEqual(rv.predicted_stop(condition, genes), stops, (cid, genes))
            self.assertEqual(rv.predicted_stop(STRICT_CONDITION, genes), batch5, "Batch 5 comparison for %s" % (genes,))
            if stops:
                self.assertIsNone(key_valid)
            else:
                self.assertEqual(all(bool(rv.KV_RULE.fullmatch(rv.expected_key_for(g))) for g in genes), key_valid, (cid, genes))

    def test_validation_and_key_validity_are_separate_properties(self):
        gap = [c for c in rv.CASES if c[2] == "predicted production gap"]
        self.assertTrue(gap and all(not c[3] and c[5] is False for c in gap), "predicted to pass validation yet to build an invalid key")
        self.assertTrue(all(c[5] is True for c in rv.CASES if c[0] in (19, 20)), "a leading _ or - is not invalid: the prefix and separator come first")

    def test_the_derived_flow_keeps_the_production_text_verbatim_and_has_only_harmless_tasks(self):
        production_text = PRODUCTION.read_text(encoding="utf-8")
        text, values_expr = rv.derive_stub(production_text)
        cand, stub = yaml.safe_load(production_text), yaml.safe_load(text)
        self.assertEqual(next(t for t in stub["tasks"] if t["id"] == "validate_genes"), next(t for t in cand["tasks"] if t["id"] == "validate_genes"))
        self.assertEqual(values_expr, rv.PRODUCTION_VALUES)
        ci = {i["id"]: i for i in cand["inputs"]}
        self.assertEqual(stub["inputs"], [ci["genes"], ci["state_key_prefix"]])
        walk = []

        def collect(node):
            if isinstance(node, dict):
                walk.append(node.get("type"))
                [collect(v) for v in node.values()]
            elif isinstance(node, list):
                [collect(v) for v in node]
        collect(stub["tasks"])
        self.assertEqual({t for t in walk if t and t.startswith("io.kestra")}, rv.ALLOWED_STUB_TYPES)
        self.assertNotIn("secret(", json.dumps(stub["tasks"]))
        self.assertNotIn("triggers", stub)
        real_key = next(t for t in next(x for x in cand["tasks"] if x["id"] == "watch_genes")["tasks"] if t["id"] == "read_snapshot")["key"]
        self.assertIn(real_key, stub["tasks"][-1]["tasks"][0]["message"])
        self.assertEqual([t["id"] for t in stub["tasks"]], ["probe_values_render", "validate_genes", "watch_genes"])

    def test_a_derived_flow_with_a_different_condition_is_refused(self):
        changed = PRODUCTION.read_text(encoding="utf-8").replace("'g,': ''", "'g,': ' '", 1)
        self.assertNotEqual(changed, PRODUCTION.read_text(encoding="utf-8"))
        with self.assertRaises(AssertionError):
            rv.derive_stub(changed)

    def test_an_engine_with_the_stricter_condition_is_detected_on_the_predicted_gap_cases(self):
        code, out = run_main(self.fake(condition=STRICT_CONDITION))
        self.assertEqual(code, 1)
        for genes in (["A,B"], ["IGH@"], ["BRCA1,TP53"]):
            self.assertTrue(self.fails(out, json.dumps(genes), "lets the list through"), (genes, out))
        self.assertFalse(self.fails(out, '["BRCA1"]', "lets the list through"), "valid cases are unaffected")

    def test_an_engine_with_a_weaker_condition_is_detected_on_the_rejected_cases(self):
        code, out = run_main(self.fake(condition="{{ 'a' != 'a' }}"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "validate_genes stops the execution"), out)

    def test_a_loop_that_runs_after_a_failed_validation_is_detected(self):
        code, out = run_main(self.fake(leak_invalid=True))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "no loop task and no LOOP sub-execution ever ran"), out)

    def test_a_late_extra_iteration_is_counted(self):
        self.assertEqual(rv.expected_iterations(["A", "A", "B"]), ["A", "B"])
        code, out = run_main(self.fake(extra_late_child=True))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "number of LOOP sub-executions"), out)

    def test_a_list_rendered_as_java_text_instead_of_json_is_reported(self):
        code, out = run_main(self.fake(render="java"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "renders the Loop values", "predicted"), out)

    def test_keys_that_differ_from_the_prediction_are_detected(self):
        code, out = run_main(self.fake(keycheck_override="otherprefix"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "KEYCHECK keys are"), out)
        self.assertFalse(self.fails(out, "number of LOOP sub-executions"))

    def test_an_engine_that_silently_repairs_invalid_keys_is_detected(self):
        code, out = run_main(self.fake(sanitize_keys=True))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, '["A,B"]', "KEYCHECK keys are"), out)
        self.assertTrue(self.fails(out, '["IGH@"]', "KEYCHECK keys are"), out)

    def test_the_key_rule_check_fails_when_a_key_that_should_be_invalid_is_valid(self):
        # a fake that reports a valid key for an entry the table says builds an invalid one: make the table disagree with the observation
        original = rv.CASES[:]
        try:
            rv.CASES[:] = [(c[0], c[1], c[2], c[3], c[4], (not c[5]) if c[0] == 3 else c[5]) for c in original]
            code, out = run_main(self.fake())
        finally:
            rv.CASES[:] = original
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "case03", "Kestra's key rule"), out)

    def test_missing_loop_run_makes_the_value_check_unverified_not_passed(self):
        code, out = run_main(self.fake(drop_loop_run=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "UNVERIFIED") if "iteration values and order" in ln], out)

    def test_an_unverified_check_fails_the_run_even_when_the_minimum_pass_count_is_zero(self):
        code, out = run_main(self.fake(drop_loop_run=True), extra=("--min-pass", "0"))
        self.assertEqual(code, 1, out)
        self.assertIn("checks not established", out)
        self.assertNotIn("the documented minimum is", out)

    def test_an_unavailable_sub_execution_search_is_unverified_and_fails_the_run(self):
        code, out = run_main(self.fake(search="error"))
        self.assertEqual(code, 1)
        self.assertTrue(self.lines(out, "UNVERIFIED"))
        self.assertEqual(len([ln for ln in out.splitlines() if ln.startswith("::warning title=UNVERIFIED::")]), len(self.lines(out, "UNVERIFIED")))

    def test_a_wrong_or_unreported_version_does_not_pass(self):
        code, out = run_main(self.fake(version="1.3.43"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "the server is Kestra 2.0.x"), out)
        code, out = run_main(self.fake(version=None))
        self.assertEqual(code, 1)
        self.assertIn("version=UNVERIFIED", out)

    def test_a_wrong_production_hash_stops_before_any_request(self):
        fake = self.fake()
        code, out = run_main(fake, sha="0" * 64)
        self.assertEqual(code, 2)
        self.assertIn("does not match the expected", out)
        self.assertEqual(fake.requests, 0)

    def test_a_flow_with_a_different_condition_is_refused_before_any_request(self):
        tmp = HERE / ("tmp-prod-%d.yaml" % os.getpid())
        try:
            tmp.write_text(PRODUCTION.read_text(encoding="utf-8").replace("'g,': ''", "'g,': ' '", 1), encoding="utf-8", newline="\n")
            fake = self.fake()
            code, out = run_main(fake, production=tmp, sha=hashlib.sha256(tmp.read_bytes()).hexdigest())
            self.assertEqual(code, 2)
            self.assertIn("differ from the reviewed production text", out)
            self.assertEqual(fake.requests, 0)
        finally:
            tmp.unlink(missing_ok=True)

    def test_only_the_validation_only_flow_can_be_executed(self):
        fake = self.fake()
        run_main(fake)
        self.assertEqual(fake.foreign_executions, 0)
        driver = rv.Driver(rv.Client(fake.url, USER, PASSWORD), rv.Report(), 0.01)
        for ns, fid in (("company.team", "clinvar-variant-reclassification-monitor"), (rv.STUB_NAMESPACE, "another-flow")):
            with self.assertRaises(AssertionError):
                driver.start(ns, fid, {"genes": "[]"})
        self.assertEqual(driver.executed, [])
        rv.SECRETS.clear()

    def test_the_production_flow_itself_is_never_deployed(self):
        fake = self.fake()
        run_main(fake)
        self.assertEqual(list(fake.flows), [(rv.STUB_NAMESPACE, rv.STUB_ID)])

    def test_the_driver_refuses_a_non_loopback_target(self):
        code, out = run_main(self.fake(), base="https://kestra.invalid")
        self.assertEqual(code, 2)
        self.assertIn("refusing to talk", out)

    def test_missing_credentials_are_refused(self):
        old = os.environ.pop("KESTRA_PASSWORD", None)
        try:
            with redirect_stdout(io.StringIO()):
                code = rv.main(["--production", str(PRODUCTION), "--expected-production-sha256", PRODUCTION_SHA])
        finally:
            if old is not None:
                os.environ["KESTRA_PASSWORD"] = old
        self.assertEqual(code, 2)

    def test_the_documented_minimum_pass_count_is_enforced(self):
        code, out = run_main(self.fake(), extra=("--min-pass", "5000"))
        self.assertEqual(code, 1)
        self.assertIn("the documented minimum is 5000", out)

    def test_redaction_removes_every_registered_form_of_the_credentials(self):
        rv.SECRETS.clear()
        rv.Client("http://127.0.0.1:1", USER, "Ci1onlyfortestsXYZ")
        forms = ["Ci1onlyfortestsXYZ", base64.b64encode(b"Ci1onlyfortestsXYZ").decode(), base64.b64encode(("%s:Ci1onlyfortestsXYZ" % USER).encode()).decode(), "%s:Ci1onlyfortestsXYZ" % USER]
        buffer = io.StringIO()
        try:
            with redirect_stdout(buffer):
                for form in forms:
                    rv.say("value", form)
            for form in forms:
                self.assertNotIn(form, buffer.getvalue())
            self.assertEqual(buffer.getvalue().count("***"), len(forms))
        finally:
            rv.SECRETS.clear()

    def test_the_run_output_never_contains_the_credentials(self):
        _, out = run_main(self.fake())
        for secret in (PASSWORD, base64.b64encode(PASSWORD.encode()).decode(), base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode()):
            self.assertNotIn(secret, out)

    def test_the_step_summary_is_written_when_requested(self):
        target = HERE / ("summary-%d.md" % os.getpid())
        try:
            run_main(self.fake(), env={"GITHUB_STEP_SUMMARY": str(target)})
            self.assertIn("| Status | Check | Detail |", target.read_text(encoding="utf-8"))
        finally:
            target.unlink(missing_ok=True)

    def test_the_driver_and_workflow_never_reference_the_recovery_directory(self):
        for path in (HERE / "run_production_validation.py", WORKFLOW):
            self.assertNotIn("recovery", path.read_text(encoding="utf-8").lower(), path.name)

    def test_the_driver_has_no_network_call_to_anything_but_the_given_loopback_url(self):
        source = (HERE / "run_production_validation.py").read_text(encoding="utf-8")
        self.assertEqual(sorted(set(re.findall(r"https?://[^\s\"')]+", source))), ["http://127.0.0.1:8080"])
        self.assertNotRegex(source, r"eutils|ncbi\.nlm|hooks\.slack|api/v1/main/namespaces/[^\"']*/kv")


class WorkflowStaticChecks(unittest.TestCase):
    text = WORKFLOW.read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    steps = wf["jobs"]["validation"]["steps"]

    def runs(self):
        return [s.get("run", "") for s in self.steps]

    def test_triggers_permissions_and_runner(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        self.assertEqual(set(self.wf[True]), {"push"})
        self.assertEqual(self.wf[True]["push"]["branches"], ["ci/kestra-production-validation"])
        self.assertEqual(self.wf["jobs"]["validation"]["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.wf["jobs"]["validation"]["timeout-minutes"], 30)
        self.assertNotRegex(self.text, r"self-hosted|schedule:|workflow_dispatch|pull_request")

    def test_actions_are_pinned_and_nothing_is_uploaded_or_secret(self):
        uses = [s["uses"] for s in self.steps if "uses" in s]
        self.assertEqual(len(uses), 2)
        for u in uses:
            self.assertRegex(u, r"^[\w./-]+@[0-9a-f]{40}$")
        active = "\n".join(ln for ln in self.text.splitlines() if not ln.lstrip().startswith("#"))
        self.assertNotRegex(active, r"upload-artifact|secrets\.|SLACK|NCBI|GITHUB_TOKEN|token:")

    def test_the_production_hash_is_asserted_before_anything_runs(self):
        self.assertEqual(self.wf["env"]["EXPECTED_PRODUCTION_SHA256"], PRODUCTION_SHA)
        step = next(r for r in self.runs() if "sha256sum" in r)
        self.assertIn("exit 1", step)
        names = [s.get("name", "") for s in self.steps]
        self.assertLess(next(i for i, n in enumerate(names) if n.startswith("Assert the flow")), next(i for i, n in enumerate(names) if n.startswith("Start Kestra")))

    def test_kestra_is_pinned_by_digest_loopback_only_and_not_privileged(self):
        self.assertRegex(self.wf["env"]["KESTRA_IMAGE"], r"^kestra/kestra:v2\.0\.6@sha256:bc2e8b5c639c3ff1f7ba9c1aadef4686a87669895a1d80c23d01c58d45785914$")
        start = next(r for r in self.runs() if "docker run -d" in r)
        self.assertIn("-p 127.0.0.1:8080:8080", start)
        for forbidden in ("--privileged", "docker.sock", "--cap-add", "--network", "--user root", "-v "):
            self.assertNotIn(forbidden, start, forbidden)

    def test_the_throwaway_credentials_are_masked_in_every_form_before_use(self):
        start = next(r for r in self.runs() if "add-mask" in r)
        self.assertEqual(start.count("::add-mask::"), 3)
        self.assertLess(start.rindex("::add-mask::"), start.index("GITHUB_ENV"))
        config = yaml.safe_load(re.search(r"cat <<EOF\n(.*?)\nEOF", start, re.S).group(1).replace("$password", "pw"))
        self.assertIs(config["kestra"]["anonymous-usage-report"]["enabled"], False)
        self.assertIs(config["kestra"]["ui-anonymous-usage-report"]["enabled"], False)

    def test_cleanup_always_runs_and_server_logs_are_redacted(self):
        always = [s for s in self.steps if s.get("if") == "always()"]
        self.assertEqual(len(always), 2)
        self.assertIn("sed", always[0]["run"])
        self.assertIn("docker rm -f kestra", always[1]["run"])

    def test_the_driver_is_called_with_the_production_flow_its_hash_and_a_minimum(self):
        call = next(r for r in self.runs() if "run_production_validation.py" in r and "test_" not in r)
        for part in ("--production ci/kestra_production_validation/production/", '--expected-production-sha256 "$EXPECTED_PRODUCTION_SHA256"', '--min-pass "$MIN_PASS"', "--base-url http://127.0.0.1:8080"):
            self.assertIn(part, call)

    def test_run_steps_interpolate_no_github_expressions(self):
        for r in self.runs():
            self.assertNotIn("${{", r)


if __name__ == "__main__":
    unittest.main(verbosity=2)
