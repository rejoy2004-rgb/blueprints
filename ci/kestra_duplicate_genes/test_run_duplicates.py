"""Offline tests of the duplicate-gene driver (run_duplicates.py) and of its workflow. NOT a test of Kestra.

FakeKestra is a small HTTP server on 127.0.0.1 whose "engine" is the offline model (the interpreter plus the reading of Kestra's
resolveValues). Its switches make it behave like engines that DISAGREE with the model, so these tests show the driver would notice.
Only a real Kestra run can show how Kestra actually iterates.

Run: python -I test_run_duplicates.py     (needs PyYAML only)
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
import run_duplicates as rd  # noqa: E402

ORIGINAL = HERE / "candidate" / "original" / "clinvar-variant-reclassification-monitor.yaml"
PROPOSED = HERE / "candidate" / "proposed" / "clinvar-variant-reclassification-monitor.yaml"
ORIGINAL_SHA = "19a33124e97deabe3cd4155025d2570614cdc2ab71b44dd53e966b167afa3978"
PROPOSED_SHA = "a266d05537c97484b52ecb89216b5ff346b50b3e0caa46184a02b30f51ef4061"
WORKFLOW = HERE.parent.parent / ".github" / "workflows" / "kestra-duplicate-genes.yml"
USER, PASSWORD = "ci@example.com", "Ci1fakepasswordforthetests"
FAST = {"DRIVER_POLL_SECONDS": "0.005", "DRIVER_CHILD_SETTLE": "0.03", "DRIVER_CHILD_TIMEOUT": "5", "DRIVER_EXEC_TIMEOUT": "10"}


class FakeKestra:
    """Switches: distinct (engine drops exact duplicates), lowercase_proposed (engine really applies `lower`), render ('json' or 'java'),
    extra_late_child (an additional iteration appears after the others), drop_loop_run (child JSON without loopRun), search ('ok'|'error')."""

    def __init__(self, version="2.0.6", distinct=True, lowercase_proposed=True, render="json", extra_late_child=False, drop_loop_run=False, search="ok",
                 execute_blocked=False, loop_run_value=None, keycheck_override=None, leak_invalid=False):
        self.loop_run_value, self.keycheck_override, self.leak_invalid = loop_run_value, keycheck_override, leak_invalid
        self.version, self.distinct, self.lowercase_proposed, self.render = version, distinct, lowercase_proposed, render
        self.extra_late_child, self.drop_loop_run, self.search, self.execute_blocked = extra_late_child, drop_loop_run, search, execute_blocked
        self.flows, self.executions, self.children, self.requests, self.full_executions = {}, {}, {}, 0, 0
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
                return self.send(h, 200, [{"constraints": "bad" if re.search(r"(?m)^extend:", body.decode()) else None}])
            if method == "POST" and path == p + "/flows":
                text = body.decode()
                if re.search(r"(?m)^extend:", text):
                    return self.send(h, 422, {"detail": "Unrecognized field extend"})
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

    def engine_values(self, values_expr, genes):
        """What this fake engine iterates. By default exactly the offline model."""
        proposed = "lower" in values_expr
        if proposed and not self.lowercase_proposed:
            rendered_list = list(genes)
        else:
            rendered_list = rd.expected_rendered_list(values_expr, genes)
        text = json.dumps(rendered_list, separators=(",", ":")) if self.render == "json" else "[" + ", ".join(rendered_list) + "]"
        if not self.distinct:
            return text, list(rendered_list)
        seen, out = set(), []
        for v in rendered_list:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return text, out

    def execute(self, h, ns, fid, body):
        if self.execute_blocked or (ns, fid) not in self.flows or fid not in rd.STUB_IDS.values():
            self.full_executions += 1
            return self.send(h, 403 if self.execute_blocked else 404, {})
        fields = {k.decode(): v.decode() for k, v in re.findall(rb'name="([^"]+)"\r\n\r\n(.*?)\r\n--', body, re.S)}
        genes, prefix = json.loads(fields["genes"]), fields.get("state_key_prefix", "")
        flow = self.flows[(ns, fid)]
        validate = next(t for t in flow["tasks"] if t["id"] == "validate_genes")
        stops = ps.evaluate(validate["condition"], {"inputs": {"genes": genes}})[0]
        eid = "ex%04d" % (len(self.executions) + 1)
        values_expr = rd.values_of(flow)
        runs, logs = [], []
        if not stops or self.leak_invalid:
            text, iterations = self.engine_values(values_expr, genes)
            logs.append({"message": "VALUES=" + text, "taskId": "probe_values_render"})
            runs.append({"taskId": "probe_values_render", "state": {"current": "SUCCESS"}})
        runs.append({"taskId": "validate_genes", "state": {"current": "FAILED" if stops else "SUCCESS"}})
        if not stops or self.leak_invalid:
            runs.append({"taskId": "watch_genes", "state": {"current": "SUCCESS"}})
            items = list(iterations)
            if self.extra_late_child:
                items.append(items[-1] + "-extra")
            for index, value in enumerate(items):
                cid = "%s-c%d" % (eid, index)
                loop_run = {"index": index, "taskId": "watch_genes", "taskRunId": "t", "value": value.upper() if self.loop_run_value == "upper" else value, "parent": {}}
                cjson = {"id": cid, "kind": "LOOP", "parentId": eid, "state": {"current": "SUCCESS"},
                         "taskRunList": [{"taskId": "read_snapshot", "state": {"current": "SUCCESS"}}, {"taskId": "iteration_probe", "state": {"current": "SUCCESS"}}]}
                if not self.drop_loop_run:
                    cjson["loopRun"] = loop_run
                self.children[cid] = {"parentId": eid, "json": cjson, "logs": [
                    {"message": "KEYCHECK %s_%s" % (self.keycheck_override or prefix, value.strip().lower()), "taskId": "read_snapshot"},
                    {"message": "ITER index=%d value=%s" % (index, value), "taskId": "iteration_probe"}]}
        self.executions[eid] = {"json": {"id": eid, "state": {"current": "FAILED" if stops else "SUCCESS"}, "taskRunList": runs}, "logs": logs}
        return self.send(h, 200, {"id": eid})


def run_main(fake, extra=(), env=None, original=ORIGINAL, proposed=PROPOSED, o_sha=ORIGINAL_SHA, p_sha=PROPOSED_SHA, base=None):
    old = {k: os.environ.get(k) for k in list(FAST) + ["KESTRA_USER", "KESTRA_PASSWORD", "GITHUB_STEP_SUMMARY"]}
    os.environ.update(FAST)
    os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD})
    os.environ.pop("GITHUB_STEP_SUMMARY", None)
    os.environ.update(env or {})
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = rd.main(["--original", str(original), "--proposed", str(proposed), "--expected-original-sha256", o_sha, "--expected-proposed-sha256", p_sha,
                            "--base-url", base or fake.url, "--startup-timeout", "3", *extra])
    finally:
        for k, v in old.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        rd.SECRETS.clear()
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
        self.assertGreaterEqual(len(self.lines(out, "PASS")), len(rd.expected_keys()))
        self.assertEqual(fake.full_executions, 0)

    def test_the_eight_required_cases_are_present_and_the_edge_cases_are_a_reasonable_set(self):
        self.assertEqual(rd.REQUIRED_CASES, [["BRCA1"], ["brca1"], ["BRCA1", "brca1"], ["brca1", "BRCA1"], ["TP53", "EGFR"], ["TP53", "EGFR", "TP53"], ["BRCA1", "BRCA1"],
                                             ["BRCA1", "TP53", "brca1"]])
        self.assertGreaterEqual(len(rd.ALL_CASES), 25)
        self.assertEqual(len(rd.expected_keys()), len(set(rd.expected_keys())))
        kinds = {"exact duplicates": any(len(set(g)) < len(g) and len({x.lower() for x in g}) == len(set(g)) for g in rd.VALID_CASES),
                 "case variants": any(len({x.lower() for x in g}) < len(set(g)) for g in rd.VALID_CASES),
                 "different genes": any(len({x.lower() for x in g}) > 1 for g in rd.VALID_CASES),
                 "ordering": ["EGFR", "TP53"] in rd.VALID_CASES and ["TP53", "EGFR"] in rd.VALID_CASES and ["tp53", "TP53"] in rd.VALID_CASES and ["TP53", "tp53"] in rd.VALID_CASES,
                 "empty": [] in rd.INVALID_CASES, "malformed": ["A,B"] in rd.INVALID_CASES and ["IGH@"] in rd.INVALID_CASES}
        self.assertTrue(all(kinds.values()), kinds)

    def test_case_labels_are_unique_and_safe_on_case_insensitive_filesystems(self):
        labels = [rd.case_label(i, e) for i in range(len(rd.ALL_CASES)) for e in rd.EXPRESSIONS]
        self.assertEqual(len(labels), len({lb.lower() for lb in labels}))
        self.assertTrue(all(re.fullmatch(r"case\d\d_(original|proposed)", lb) for lb in labels))

    def test_the_original_and_proposed_candidates_differ_in_exactly_the_loop_values_line(self):
        a, b = ORIGINAL.read_text(encoding="utf-8").splitlines(), PROPOSED.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(a), len(b))
        changed = [(x.strip(), y.strip()) for x, y in zip(a, b) if x != y]
        self.assertEqual(changed, [('values: "{{ inputs.genes }}"', 'values: "{{ inputs.genes | toJson | lower }}"')])
        self.assertEqual(hashlib.sha256(ORIGINAL.read_bytes()).hexdigest(), ORIGINAL_SHA)
        self.assertEqual(hashlib.sha256(PROPOSED.read_bytes()).hexdigest(), PROPOSED_SHA)

    def test_the_derived_flows_keep_the_candidate_text_verbatim_and_have_only_harmless_tasks(self):
        for label, path in (("original", ORIGINAL), ("proposed", PROPOSED)):
            text, values_expr = rd.derive_stub(path.read_text(encoding="utf-8"), label)
            cand, stub = yaml.safe_load(path.read_text(encoding="utf-8")), yaml.safe_load(text)
            self.assertEqual(next(t for t in stub["tasks"] if t["id"] == "validate_genes"), next(t for t in cand["tasks"] if t["id"] == "validate_genes"))
            self.assertEqual(rd.values_of(stub), rd.values_of(cand))
            self.assertEqual(values_expr, rd.values_of(cand))
            ci = {i["id"]: i for i in cand["inputs"]}
            self.assertEqual(stub["inputs"], [ci["genes"], ci["state_key_prefix"]])
            types = {t["type"] for t in stub["tasks"]} | {t["type"] for t in stub["tasks"][-1]["tasks"]}
            self.assertEqual(types, rd.ALLOWED_STUB_TYPES)
            walk = []

            def collect(node):
                if isinstance(node, dict):
                    walk.append(node.get("type"))
                    [collect(v) for v in node.values()]
                elif isinstance(node, list):
                    [collect(v) for v in node]
            collect(stub["tasks"])
            self.assertTrue({t for t in walk if t and t.startswith("io.kestra")} <= rd.ALLOWED_STUB_TYPES)
            self.assertNotIn("secret(", json.dumps(stub["tasks"]))
            self.assertNotIn("triggers", stub)
            loop = stub["tasks"][-1]
            real_key = next(t for t in next(x for x in cand["tasks"] if x["id"] == "watch_genes")["tasks"] if t["id"] == "read_snapshot")["key"]
            self.assertIn(real_key, loop["tasks"][0]["message"])
            self.assertEqual([t["id"] for t in stub["tasks"]], ["probe_values_render", "validate_genes", "watch_genes"])
            self.assertEqual(stub["tasks"][0]["message"], "VALUES=" + rd.values_of(cand), "the render probe prints exactly the candidate\x27s values expression")

    def test_the_offline_model_reproduces_the_batch_7_predictions(self):
        o, p = '{{ inputs.genes }}', '{{ inputs.genes | toJson | lower }}'
        self.assertEqual(rd.expected_iterations(o, ["BRCA1", "brca1"]), ["BRCA1", "brca1"])
        self.assertEqual(rd.expected_iterations(p, ["BRCA1", "brca1"]), ["brca1"])
        self.assertEqual(rd.expected_iterations(p, ["brca1", "BRCA1"]), ["brca1"])
        self.assertEqual(rd.expected_iterations(o, ["TP53", "EGFR", "TP53"]), ["TP53", "EGFR"])
        self.assertEqual(rd.expected_iterations(p, ["BRCA1", "TP53", "brca1"]), ["brca1", "tp53"])
        self.assertEqual(rd.expected_iterations(o, ["BRCA1", "BRCA1"]), ["BRCA1"])

    def test_the_case_table_agrees_with_the_validation_prediction(self):
        cond = next(t for t in yaml.safe_load(ORIGINAL.read_text(encoding="utf-8"))["tasks"] if t["id"] == "validate_genes")["condition"]
        for genes in rd.VALID_CASES:
            self.assertFalse(rd.predicted_stop(cond, genes), genes)
        for genes in rd.INVALID_CASES:
            self.assertTrue(rd.predicted_stop(cond, genes), genes)

    def test_an_engine_that_does_not_deduplicate_is_detected(self):
        code, out = run_main(self.fake(distinct=False))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "number of LOOP sub-executions"), out)

    def test_an_engine_that_ignores_lower_in_the_proposed_expression_is_detected(self):
        code, out = run_main(self.fake(lowercase_proposed=False))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "_proposed", "number of LOOP sub-executions") or self.fails(out, "_proposed", "renders the Loop values"), out)
        self.assertFalse(self.fails(out, "_original"), "the original expression is unaffected by this fault")

    def test_a_list_rendered_as_java_text_instead_of_json_is_reported(self):
        code, out = run_main(self.fake(render="java"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "renders the Loop values", "predicted"), out)

    def test_a_late_extra_iteration_is_counted(self):
        code, out = run_main(self.fake(extra_late_child=True))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "number of LOOP sub-executions"), out)

    def test_missing_loop_run_makes_the_value_check_unverified_not_passed(self):
        code, out = run_main(self.fake(drop_loop_run=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "UNVERIFIED") if "iteration values and order" in ln], out)

    def test_a_loop_run_value_that_disagrees_with_the_iter_log_is_detected(self):
        code, out = run_main(self.fake(loop_run_value="upper"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "iteration values and order"), out)

    def test_keys_that_differ_from_the_prediction_are_detected_even_when_counts_and_values_agree(self):
        code, out = run_main(self.fake(keycheck_override="otherprefix"))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "KEYCHECK keys are"), out)
        self.assertFalse(self.fails(out, "number of LOOP sub-executions"), "counts agree in this fault")

    def test_a_loop_that_runs_after_a_failed_validation_is_detected(self):
        code, out = run_main(self.fake(leak_invalid=True))
        self.assertEqual(code, 1)
        self.assertTrue(self.fails(out, "no loop task and no LOOP sub-execution ever ran"), out)

    def test_an_unverified_check_fails_the_run_even_when_the_minimum_pass_count_is_zero(self):
        # the UNVERIFIED rule must hold on its own, not only because UNVERIFIED checks lower the PASS count below the minimum
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

    def test_a_wrong_candidate_hash_stops_before_any_request(self):
        fake = self.fake()
        for kw in ({"o_sha": "0" * 64}, {"p_sha": "0" * 64}):
            code, out = run_main(fake, **kw)
            self.assertEqual(code, 2)
            self.assertIn("does not match the expected", out)
        self.assertEqual(fake.requests, 0)

    def test_a_proposed_candidate_with_any_other_change_is_refused(self):
        tmp = HERE / ("tmp-prop-%d.yaml" % os.getpid())
        try:
            tmp.write_text(PROPOSED.read_text(encoding="utf-8") + "# an extra line the reviewed candidate does not have\n", encoding="utf-8", newline="\n")
            fake = self.fake()
            code, out = run_main(fake, proposed=tmp, p_sha=hashlib.sha256(tmp.read_bytes()).hexdigest())
            self.assertEqual(code, 2)
            self.assertIn("must differ from the original only in the Loop values line", out)
            self.assertEqual(fake.requests, 0)
        finally:
            tmp.unlink(missing_ok=True)

    def test_the_full_candidates_are_never_executed(self):
        fake = self.fake()
        run_main(fake)
        self.assertEqual(fake.full_executions, 0)
        driver = rd.Driver(rd.Client(fake.url, USER, PASSWORD), rd.Report(), 0.01)
        for ns, fid in (("company.team", "clinvar-variant-reclassification-monitor"), ("ci.dupgenes.fullproposed", "clinvar-variant-reclassification-monitor")):
            with self.assertRaises(AssertionError):
                driver.start(ns, fid, {"genes": "[]"})
        self.assertEqual(driver.executed, [])
        rd.SECRETS.clear()

    def test_the_driver_refuses_a_non_loopback_target(self):
        code, out = run_main(self.fake(), base="https://kestra.invalid")
        self.assertEqual(code, 2)
        self.assertIn("refusing to talk", out)

    def test_missing_credentials_are_refused(self):
        old = os.environ.pop("KESTRA_PASSWORD", None)
        try:
            with redirect_stdout(io.StringIO()):
                code = rd.main(["--original", str(ORIGINAL), "--proposed", str(PROPOSED), "--expected-original-sha256", ORIGINAL_SHA, "--expected-proposed-sha256", PROPOSED_SHA])
        finally:
            if old is not None:
                os.environ["KESTRA_PASSWORD"] = old
        self.assertEqual(code, 2)

    def test_the_documented_minimum_pass_count_is_enforced(self):
        code, out = run_main(self.fake(), extra=("--min-pass", "5000"))
        self.assertEqual(code, 1)
        self.assertIn("the documented minimum is 5000", out)

    def test_redaction_removes_every_registered_form_of_the_credentials(self):
        rd.SECRETS.clear()
        rd.Client("http://127.0.0.1:1", USER, "Ci1onlyfortestsXYZ")
        forms = ["Ci1onlyfortestsXYZ", base64.b64encode(b"Ci1onlyfortestsXYZ").decode(), base64.b64encode(("%s:Ci1onlyfortestsXYZ" % USER).encode()).decode(), "%s:Ci1onlyfortestsXYZ" % USER]
        buffer = io.StringIO()
        try:
            with redirect_stdout(buffer):
                for form in forms:
                    rd.say("value", form)
            for form in forms:
                self.assertNotIn(form, buffer.getvalue())
            self.assertEqual(buffer.getvalue().count("***"), len(forms))
        finally:
            rd.SECRETS.clear()

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
        for path in (HERE / "run_duplicates.py", WORKFLOW):
            self.assertNotIn("recovery", path.read_text(encoding="utf-8").lower(), path.name)


class WorkflowStaticChecks(unittest.TestCase):
    text = WORKFLOW.read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    steps = wf["jobs"]["duplicates"]["steps"]

    def runs(self):
        return [s.get("run", "") for s in self.steps]

    def test_triggers_permissions_and_runner(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        self.assertEqual(set(self.wf[True]), {"push"})
        self.assertEqual(self.wf[True]["push"]["branches"], ["ci/kestra-duplicate-genes"])
        self.assertEqual(self.wf["jobs"]["duplicates"]["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.wf["jobs"]["duplicates"]["timeout-minutes"], 30)
        self.assertNotRegex(self.text, r"self-hosted|schedule:|workflow_dispatch|pull_request")

    def test_actions_are_pinned_and_nothing_is_uploaded_or_secret(self):
        uses = [s["uses"] for s in self.steps if "uses" in s]
        self.assertEqual(len(uses), 2)
        for u in uses:
            self.assertRegex(u, r"^[\w./-]+@[0-9a-f]{40}$")
        active = "\n".join(ln for ln in self.text.splitlines() if not ln.lstrip().startswith("#"))
        self.assertNotRegex(active, r"upload-artifact|secrets\.|SLACK|NCBI|GITHUB_TOKEN|token:")

    def test_both_candidate_hashes_are_asserted_before_anything_runs(self):
        self.assertEqual(self.wf["env"]["EXPECTED_ORIGINAL_SHA256"], ORIGINAL_SHA)
        self.assertEqual(self.wf["env"]["EXPECTED_PROPOSED_SHA256"], PROPOSED_SHA)
        step = next(r for r in self.runs() if "sha256sum" in r)
        self.assertIn("exit 1", step)
        self.assertIn("||", step)
        names = [s.get("name", "") for s in self.steps]
        self.assertLess(next(i for i, n in enumerate(names) if n.startswith("Assert both")), next(i for i, n in enumerate(names) if n.startswith("Start Kestra")))

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

    def test_the_driver_is_called_with_both_candidates_their_hashes_and_a_minimum(self):
        call = next(r for r in self.runs() if "run_duplicates.py" in r and "test_" not in r)
        for part in ("--original ci/kestra_duplicate_genes/candidate/original/", "--proposed ci/kestra_duplicate_genes/candidate/proposed/",
                     '--expected-original-sha256 "$EXPECTED_ORIGINAL_SHA256"', '--expected-proposed-sha256 "$EXPECTED_PROPOSED_SHA256"',
                     '--min-pass "$MIN_PASS"', "--base-url http://127.0.0.1:8080"):
            self.assertIn(part, call)
        self.assertEqual(int(self.wf["env"]["MIN_PASS"]), len(rd.expected_keys()))

    def test_run_steps_interpolate_no_github_expressions(self):
        for r in self.runs():
            self.assertNotIn("${{", r)


if __name__ == "__main__":
    unittest.main(verbosity=2)
