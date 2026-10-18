"""Offline tests of the gene-validation driver (run_validation.py) and of the workflow that runs it.

The driver is exercised against FakeKestra, a small HTTP server on 127.0.0.1 whose "engine" is the offline interpreter. It therefore tests
the DRIVER's logic (derivation of the validation-only flow, verdicts, strict exit codes, isolation, redaction). It cannot show how Kestra's
real Pebble engine behaves: only the real run can.

Run: python -I test_run_validation.py     (needs PyYAML only)
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
import run_validation as rv  # noqa: E402

CANDIDATE = HERE / "candidate" / "clinvar-variant-reclassification-monitor.yaml"
CANDIDATE_SHA = "19a33124e97deabe3cd4155025d2570614cdc2ab71b44dd53e966b167afa3978"
WORKFLOW = HERE.parent.parent / ".github" / "workflows" / "kestra-gene-validation.yml"
USER, PASSWORD = "ci@example.com", "Ci1fakepasswordforthetests"
FAST = {"DRIVER_POLL_SECONDS": "0.005", "DRIVER_CHILD_SETTLE": "0.03", "DRIVER_CHILD_TIMEOUT": "5", "DRIVER_EXEC_TIMEOUT": "10"}


class FakeKestra:
    """Behaviour switches inject the faults the driver must catch."""

    def __init__(self, version="2.0.6", flip=(), leak=False, prefix_check=True, refuse_empty=False, validate_error=False, search="ok", logs=True,
                 wrong_probe=None):
        self.version, self.flip, self.leak, self.prefix_check, self.refuse_empty = version, [json.dumps(g) for g in flip], leak, prefix_check, refuse_empty
        self.validate_error, self.search, self.logs_on, self.wrong_probe = validate_error, search, logs, wrong_probe
        self.flows, self.executions, self.children, self.requests = {}, {}, {}, 0
        self.candidate_executions = 0
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
        self.lock = threading.Lock()
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
            expected = "Basic " + base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode()
            if h.headers.get("Authorization") != expected:
                return self.send(h, 401, {})
            if path == p + "/flows/search":
                return self.send(h, 200, {"results": []})
            if method == "POST" and path == p + "/flows/validate":
                text = body.decode()
                bad = self.validate_error or re.search(r"(?m)^extend:", text)
                return self.send(h, 200, [{"constraints": "bad flow" if bad else None}])
            if method == "POST" and path == p + "/flows":
                text = body.decode()
                if re.search(r"(?m)^extend:", text):
                    return self.send(h, 422, {"detail": "Unrecognized field extend"})
                ns = re.search(r"(?m)^namespace:\s*(\S+)", text).group(1)
                fid = re.search(r"(?m)^id:\s*(\S+)", text).group(1)
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
                if not self.logs_on or not ex:
                    return self.send(h, 404, {})
                return self.send(h, 200, ex["logs"])
            return self.send(h, 404, {"message": "no route"})

    def execute(self, h, ns, fid, body):
        if fid != rv.STUB_ID:
            self.candidate_executions += 1
        fields = {k.decode(): v.decode() for k, v in re.findall(rb'name="([^"]+)"\r\n\r\n(.*?)\r\n--', body, re.S)}
        flow = self.flows.get((ns, fid))
        if flow is None:
            return self.send(h, 404, {})
        genes, prefix = json.loads(fields["genes"]), fields.get("state_key_prefix", "")
        inputs = {i["id"]: i for i in flow["inputs"]}
        if self.prefix_check and re.search(inputs["state_key_prefix"]["validator"], prefix) is None:
            return self.send(h, 422, {"detail": "invalid state_key_prefix"})
        if self.refuse_empty and not genes:
            return self.send(h, 422, {"detail": "genes is required"})
        eid = "ex%03d" % (len(self.executions) + 1)
        validate = next(t for t in flow["tasks"] if t["id"] == "validate_genes")
        stops = ps.evaluate(validate["condition"], {"inputs": {"genes": genes}})[0]
        if json.dumps(genes) in self.flip:
            stops = not stops
        logs, runs = [], []
        for t in flow["tasks"]:
            if t["id"].startswith("probe_"):
                name, expr = re.match(r"PROBE (\w+)=\{\{ (.*) \}\}$", t["message"], re.S).groups()
                value, _, _ = ps.evaluate("{{ %s }}" % expr, {"inputs": {"genes": genes}})
                text = str(value).lower() if isinstance(value, bool) else str(value)
                if self.wrong_probe == name:
                    text = "WRONG"
                logs.append({"message": "PROBE %s=%s" % (name, text), "taskId": t["id"], "level": "INFO"})
                runs.append({"taskId": t["id"], "state": {"current": "SUCCESS"}})
        runs.append({"taskId": "validate_genes", "state": {"current": "FAILED" if stops else "SUCCESS"}})
        state = "FAILED" if stops else "SUCCESS"
        if not stops or self.leak:
            runs.append({"taskId": "watch_genes", "state": {"current": "SUCCESS"}})
            for gene in genes:
                cid = "%s-c%d" % (eid, len(self.children))
                self.children[cid] = {"parentId": eid, "json": {"id": cid, "kind": "LOOP", "parentId": eid, "state": {"current": "SUCCESS"},
                                                                 "taskRunList": [{"taskId": "read_snapshot", "state": {"current": "SUCCESS"}}]},
                                      "logs": [{"message": "KEYCHECK %s_%s" % (prefix, gene.strip().lower()), "taskId": "read_snapshot"}]}
        self.executions[eid] = {"json": {"id": eid, "state": {"current": state}, "taskRunList": runs}, "logs": logs}
        return self.send(h, 200, {"id": eid})


def run_main(fake, extra=(), env=None, candidate=CANDIDATE, sha=CANDIDATE_SHA, base=None):
    old = {k: os.environ.get(k) for k in list(FAST) + ["KESTRA_USER", "KESTRA_PASSWORD", "GITHUB_STEP_SUMMARY"]}
    os.environ.update(FAST)
    os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD})
    os.environ.pop("GITHUB_STEP_SUMMARY", None)
    os.environ.update(env or {})
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = rv.main(["--candidate", str(candidate), "--expected-sha256", sha, "--base-url", base or fake.url, "--startup-timeout", "3", *extra])
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

    def test_happy_path_accepts_the_run_and_every_expected_key_passes(self):
        fake = self.fake()
        code, out = run_main(fake)
        self.assertEqual(code, 0, out)
        self.assertEqual(self.lines(out, "FAIL") + self.lines(out, "UNVERIFIED"), [], out)
        self.assertGreaterEqual(len(self.lines(out, "PASS")), len(rv.expected_keys()))
        self.assertEqual(fake.candidate_executions, 0)

    def test_the_cases_cover_what_was_asked(self):
        for genes in (["MAX"], ["BRCA1", "HLA-A"], ["TP53", "EGFR"]):
            self.assertIn(genes, rv.VALID_GENES)
        self.assertTrue(any(g != g.lower() and g != g.upper() for lst in rv.VALID_GENES for g in lst), "a mixed-case valid identifier")
        for genes in (["BRCA1,TP53"], ["A,B"], ["MAX", "A,B"], [""], [" "], ["IGH@"], ["é"]):
            self.assertIn(genes, rv.INVALID_GENES)
        self.assertEqual(len(rv.expected_keys()), len(set(rv.expected_keys())), "keys are unique")

    def test_the_derived_flow_keeps_the_candidate_text_verbatim_and_has_only_harmless_tasks(self):
        text, probes = rv.derive_stub(CANDIDATE.read_text(encoding="utf-8"))
        cand, stub = yaml.safe_load(CANDIDATE.read_text(encoding="utf-8")), yaml.safe_load(text)
        validate = next(t for t in cand["tasks"] if t["id"] == "validate_genes")
        self.assertEqual(next(t for t in stub["tasks"] if t["id"] == "validate_genes"), validate)
        self.assertEqual(probes["condition"], validate["condition"].strip()[2:-2].strip())
        self.assertEqual(probes["c1"] + " or " + probes["c2"], probes["condition"])
        cand_inputs = {i["id"]: i for i in cand["inputs"]}
        self.assertEqual(stub["inputs"], [cand_inputs["genes"], cand_inputs["state_key_prefix"]])
        types = {t["type"] for t in stub["tasks"]} | {t["type"] for t in stub["tasks"][-1]["tasks"]}
        self.assertEqual(types, rv.ALLOWED_STUB_TYPES)
        walked = []

        def collect(node):
            if isinstance(node, dict):
                walked.append(node.get("type"))
                for v in node.values():
                    collect(v)
            elif isinstance(node, list):
                for v in node:
                    collect(v)
        collect(stub["tasks"])
        self.assertTrue({t for t in walked if t and t.startswith("io.kestra")} <= rv.ALLOWED_STUB_TYPES)
        self.assertNotIn("secret(", json.dumps(stub["tasks"]), "no task reads a secret")
        self.assertNotIn("eutils", json.dumps(stub["tasks"]), "no task mentions NCBI E-utilities")
        self.assertNotIn("triggers", stub, "the validation-only flow has no trigger")
        order = [t["id"] for t in stub["tasks"]]
        self.assertLess(order.index("validate_genes"), order.index("watch_genes"))
        real_key = next(t for t in next(x for x in cand["tasks"] if x["id"] == "watch_genes")["tasks"] if t["id"] == "read_snapshot")["key"]
        self.assertIn(real_key, stub["tasks"][-1]["tasks"][0]["message"], "the KV key expression is the real one, verbatim")
        self.assertEqual(stub["namespace"], rv.STUB_NAMESPACE)

    def test_every_probe_is_evaluable_by_the_interpreter_so_the_engine_can_be_compared(self):
        _, probes = rv.derive_stub(CANDIDATE.read_text(encoding="utf-8"))
        self.assertEqual(set(probes), {"condition", "c1", "c2", "collapsed", "gcount", "entries", "comma_removed", "or_literal"})
        self.assertEqual(rv.predicted(probes["condition"], ["A,B"]), "true")
        self.assertEqual(rv.predicted(probes["condition"], ["A", "B"]), "false")
        self.assertEqual(rv.predicted(probes["or_literal"], ["x"]), "true")
        self.assertEqual(rv.predicted(probes["comma_removed"], ["a", "b"]), "ab")

    def test_the_full_candidate_is_never_executed(self):
        fake = self.fake()
        run_main(fake)
        self.assertEqual(fake.candidate_executions, 0)
        driver = rv.Driver(rv.Client(fake.url, USER, PASSWORD), rv.Report(), 0.01)
        with self.assertRaises(AssertionError):
            driver.start("company.team", "clinvar-variant-reclassification-monitor", {"genes": "[]"})
        self.assertEqual(driver.executed, [])
        rv.SECRETS.clear()

    def test_an_engine_that_accepts_an_embedded_comma_is_a_failure(self):
        code, out = run_main(self.fake(flip=[["A,B"]]))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "invalid" in ln and "A,B" in ln], out)

    def test_an_engine_that_rejects_a_legitimate_list_is_a_failure(self):
        code, out = run_main(self.fake(flip=[["MAX"]]))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "valid [\"MAX\"]" in ln], out)

    def test_downstream_tasks_running_after_a_failed_validation_is_a_failure(self):
        code, out = run_main(self.fake(leak=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "neither watch_genes nor read_snapshot ever ran" in ln], out)

    def test_probe_values_that_differ_from_the_interpreter_are_reported_as_disagreement(self):
        code, out = run_main(self.fake(wrong_probe="c2"))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "probe values equal the interpreter" in ln and "c2" in ln], out)

    def test_an_invalid_prefix_that_is_accepted_is_a_failure(self):
        code, out = run_main(self.fake(prefix_check=False))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "prefix '_x'" in ln], out)

    def test_a_wrong_or_unreported_version_does_not_pass(self):
        code, out = run_main(self.fake(version="1.3.43"))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "the server is Kestra 2.0.x" in ln], out)
        code, out = run_main(self.fake(version=None))
        self.assertEqual(code, 1)
        self.assertIn("version=UNVERIFIED", out)

    def test_a_candidate_that_kestra_will_not_validate_is_a_failure(self):
        code, out = run_main(self.fake(validate_error=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "FAIL") if "validates the candidate flow" in ln], out)

    def test_an_unavailable_sub_execution_search_is_unverified_and_fails_the_run(self):
        code, out = run_main(self.fake(search="error"))
        self.assertEqual(code, 1)
        self.assertTrue(self.lines(out, "UNVERIFIED"))
        self.assertEqual(len([ln for ln in out.splitlines() if ln.startswith("::warning title=UNVERIFIED::")]), len(self.lines(out, "UNVERIFIED")))

    def test_missing_logs_are_unverified_not_passed(self):
        code, out = run_main(self.fake(logs=False))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.lines(out, "UNVERIFIED") if "probe values" in ln], out)

    def test_an_empty_list_refused_by_kestra_input_handling_is_recorded_not_counted_against_the_expression(self):
        code, out = run_main(self.fake(refuse_empty=True))
        self.assertEqual(code, 0, out)
        self.assertTrue([ln for ln in self.lines(out, "INFO") if "refused the empty list" in ln and "not exercised" in ln], out)

    def test_a_wrong_candidate_hash_stops_before_any_request(self):
        fake = self.fake()
        code, out = run_main(fake, sha="0" * 64)
        self.assertEqual(code, 2)
        self.assertEqual(fake.requests, 0)
        self.assertIn("does not match the expected", out)

    def test_the_driver_refuses_a_non_loopback_target(self):
        code, out = run_main(self.fake(), base="https://kestra.invalid")
        self.assertEqual(code, 2)
        self.assertIn("refusing to talk", out)

    def test_missing_credentials_are_refused(self):
        old = os.environ.pop("KESTRA_PASSWORD", None)
        try:
            with redirect_stdout(io.StringIO()):
                code = rv.main(["--candidate", str(CANDIDATE), "--expected-sha256", CANDIDATE_SHA])
        finally:
            if old is not None:
                os.environ["KESTRA_PASSWORD"] = old
        self.assertEqual(code, 2)

    def test_the_documented_minimum_pass_count_is_enforced(self):
        code, out = run_main(self.fake(), extra=("--min-pass", "5000"))
        self.assertEqual(code, 1)
        self.assertIn("the documented minimum is 5000", out)

    def test_every_form_of_the_credentials_is_redacted(self):
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

    def test_redaction_removes_every_registered_form_of_the_credentials_from_anything_printed(self):
        rv.SECRETS.clear()
        rv.Client("http://127.0.0.1:1", USER, "Ci1onlyfortestsXYZ")
        forms = ["Ci1onlyfortestsXYZ", base64.b64encode(b"Ci1onlyfortestsXYZ").decode(), base64.b64encode(("%s:Ci1onlyfortestsXYZ" % USER).encode()).decode(),
                 "%s:Ci1onlyfortestsXYZ" % USER]
        buffer = io.StringIO()
        try:
            with redirect_stdout(buffer):
                for form in forms:
                    rv.say("value", form)
            for form in forms:
                self.assertNotIn(form, buffer.getvalue())
            self.assertEqual(buffer.getvalue().count("***"), len(forms))
            self.assertNotIn(forms[0], rv.snippet("body " + forms[0]))
        finally:
            rv.SECRETS.clear()

    def test_the_candidate_file_in_this_tree_is_the_expected_one(self):
        self.assertEqual(hashlib.sha256(CANDIDATE.read_bytes()).hexdigest(), CANDIDATE_SHA)

    def test_the_top_level_or_split_ignores_or_inside_quotes_and_parentheses(self):
        self.assertEqual(rv.split_top_level_or("(a or b) or ('x or y' ~ z)"), ["(a or b)", "('x or y' ~ z)"])


class WorkflowStaticChecks(unittest.TestCase):
    text = WORKFLOW.read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    steps = wf["jobs"]["validate"]["steps"]

    def runs(self):
        return [s.get("run", "") for s in self.steps]

    def test_triggers_permissions_and_runner(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        self.assertEqual(set(self.wf[True]), {"push"})
        self.assertEqual(self.wf[True]["push"]["branches"], ["ci/kestra-gene-validation"])
        self.assertEqual(self.wf["jobs"]["validate"]["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.wf["jobs"]["validate"]["timeout-minutes"], 20)
        self.assertNotRegex(self.text, r"self-hosted|schedule:|workflow_dispatch|pull_request")

    def test_actions_are_pinned_and_nothing_is_uploaded_or_secret(self):
        uses = [s["uses"] for s in self.steps if "uses" in s]
        self.assertEqual(len(uses), 2)
        for u in uses:
            self.assertRegex(u, r"^[\w./-]+@[0-9a-f]{40}$")
        active = "\n".join(ln for ln in self.text.splitlines() if not ln.lstrip().startswith("#"))  # comments may mention what is excluded
        self.assertNotRegex(active, r"upload-artifact|secrets\.|SLACK|NCBI|GITHUB_TOKEN|token:")

    def test_the_candidate_hash_is_asserted_before_anything_runs(self):
        self.assertEqual(self.wf["env"]["EXPECTED_CANDIDATE_SHA256"], CANDIDATE_SHA)
        step = next(r for r in self.runs() if "sha256sum" in r)
        self.assertIn("exit 1", step)
        names = [s.get("name", "") for s in self.steps]
        self.assertLess(next(i for i, n in enumerate(names) if n.startswith("Assert the candidate")),
                        next(i for i, n in enumerate(names) if n.startswith("Start Kestra")))

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
        self.assertEqual(config["kestra"]["server"]["basic-auth"]["username"], "ci@example.com")

    def test_cleanup_always_runs_and_server_logs_are_redacted(self):
        always = [s for s in self.steps if s.get("if") == "always()"]
        self.assertEqual(len(always), 2)
        self.assertIn("sed", always[0]["run"])
        self.assertIn("docker rm -f kestra", always[1]["run"])

    def test_the_driver_is_called_with_the_candidate_its_hash_and_a_minimum(self):
        call = next(r for r in self.runs() if "run_validation.py" in r and "test_" not in r)
        for part in ("--candidate ci/kestra_gene_validation/candidate/clinvar-variant-reclassification-monitor.yaml", '--expected-sha256 "$EXPECTED_CANDIDATE_SHA256"',
                     '--min-pass "$MIN_PASS"', "--base-url http://127.0.0.1:8080"):
            self.assertIn(part, call)
        self.assertEqual(int(self.wf["env"]["MIN_PASS"]), len(rv.expected_keys()))

    def test_run_steps_interpolate_no_github_expressions(self):
        for r in self.runs():
            self.assertNotIn("${{", r)


if __name__ == "__main__":
    unittest.main(verbosity=2)
