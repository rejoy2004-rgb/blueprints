"""Tests for the Kestra integration driver (run_integration.py) and static checks of kestra-integration.yml.

The driver is exercised against FakeKestra, a small HTTP server on 127.0.0.1 that imitates the parts of Kestra's REST API the driver
uses. It encodes the ASSUMPTIONS the driver makes about Kestra 2.0 (paths, JSON shapes, states). It therefore tests the driver's own
logic, including that every failure mode it must detect IS detected. It cannot show that Kestra really behaves that way: only the
real run on a real server can.

Run: python -I test_run_integration.py     (PyYAML only; set CLINVAR_FLOW to also check the transformation on the real flow)
"""
import io
import json
import os
import re
import sys
import threading
import unittest
import urllib.parse
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

sys.dont_write_bytecode = True  # `python -I` ignores PYTHONDONTWRITEBYTECODE; importing the driver must not leave a __pycache__ behind
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run_integration as driver  # noqa: E402

WORKFLOW = HERE.parent.parent / ".github" / "workflows" / "kestra-integration.yml"
USER, PASSWORD = "ci@example.com", "Ci1fakepasswordforthetests"
SAMPLE_FLOW = """id: clinvar-variant-reclassification-monitor
namespace: company.team

triggers:
  - id: weekly_check
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 6 * * 2"

tasks:
  - id: noop
    type: io.kestra.plugin.core.log.Log
    message: hi
"""


GALLERY_BLOCK = """
extend:
  title: Gallery title
  description: |
    Markdown with a line that looks like
    extend: nothing
  tags:
    - a
    - b
"""
# a nested key named `extend` must survive stripping
SAMPLE_WITH_EXTEND = SAMPLE_FLOW.replace("    message: hi\n", "    message: hi\n    extend: nested-and-kept\n") + GALLERY_BLOCK


class FakeKestra:
    """Imitates the Kestra endpoints the driver uses. Behaviour switches let tests inject faults."""

    def __init__(self, version="2.0.6", prefix="/api/v1/main", enforce_concurrency=True, logs=True, run_slack=False,
                 tamper_kv_on_failure=False, flow_validation_error=False, stall=False, task_list=False, outputs=False,
                 configs=True, task_ids_in_logs=True, empty_kv_after_baseline=False, label=None):
        # Kestra 2.0 execution JSON carries no task outputs, so the fake omits them by default (outputs=True adds them, task_list=True adds states)
        self.task_list, self.outputs, self.configs, self.label = task_list, outputs, configs, label
        self.task_ids_in_logs, self.empty_kv_after_baseline = task_ids_in_logs, empty_kv_after_baseline
        self.version, self.prefix, self.stall = version, prefix, stall
        self.enforce_concurrency, self.logs_enabled, self.run_slack = enforce_concurrency, logs, run_slack
        self.tamper_kv_on_failure, self.flow_validation_error = tamper_kv_on_failure, flow_validation_error
        self.kv, self.executions, self.flows, self.order = {}, {}, {}, []
        self.clock = 0
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

    # ---- helpers
    def stamp(self):
        self.clock += 1
        return (datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=self.clock)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

    def send(self, h, status, body):
        data = json.dumps(body).encode() if not isinstance(body, bytes) else body
        h.send_response(status)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(data)))
        h.end_headers()
        h.wfile.write(data)

    def authorised(self, h):
        import base64
        return h.headers.get("Authorization") == "Basic " + base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode()

    def handle(self, h, method):
        with self.lock:
            path = urllib.parse.urlsplit(h.path).path
            length = int(h.headers.get("Content-Length") or 0)
            body = h.rfile.read(length) if length else b""
            if path == "/api/v1/configs" or path == self.prefix + "/configs":
                if not self.configs:
                    return self.send(h, 404, {})
                if self.configs == "malformed":
                    return self.send(h, 200, b"<html>not json</html>")
                if self.configs == "noversion":
                    return self.send(h, 200, {"edition": "OSS"})
                if self.configs == "nonstring":
                    return self.send(h, 200, {"version": 206})
                return self.send(h, 200, {"version": self.version})
            if not self.authorised(h):
                return self.send(h, 401, {"message": "Unauthorized"})
            p = self.prefix
            if method == "GET" and path == p + "/flows/search":
                return self.send(h, 200, {"results": [], "total": 0})
            gallery = re.search(rb"^extend:", body, re.M) is not None
            if method == "POST" and path == p + "/flows/validate":
                if gallery:
                    return self.send(h, 200, [{"index": 0, "constraints": 'Validation error: Unrecognized field "extend"'}])
                return self.send(h, 200, [{"constraints": "tasks: bad" if self.flow_validation_error else None}])
            if method == "POST" and path == p + "/flows" and gallery:
                return self.send(h, 422, {"title": "Validation failed", "detail": 'Unrecognized field "extend"'})
            if method == "POST" and path == p + "/flows":
                ns = re.search(rb"^namespace:\s*(\S+)", body, re.M).group(1).decode()
                fid = re.search(rb"^id:\s*(\S+)", body, re.M).group(1).decode()
                self.flows[(ns, fid)] = body.decode()
                return self.send(h, 200, {"id": fid, "namespace": ns})
            m = re.fullmatch(re.escape(p) + r"/flows/([^/]+)/([^/]+)", path)
            if method == "GET" and m:
                if (m.group(1), m.group(2)) in self.flows:
                    return self.send(h, 200, {"id": m.group(2), "namespace": m.group(1)})
                return self.send(h, 404, {})
            m = re.fullmatch(re.escape(p) + r"/executions/([^/]+)/([^/]+)", path)
            if method == "POST" and m:
                return self.create_execution(h, body)
            m = re.fullmatch(re.escape(p) + r"/executions/([^/]+)", path)
            if method == "GET" and m:
                return self.get_execution(h, m.group(1))
            m = re.fullmatch(re.escape(p) + r"/logs/([^/]+)", path)
            if method == "GET" and m:
                if not self.logs_enabled:
                    return self.send(h, 404, {})
                rows = self.executions[m.group(1)].get("logs", [])
                return self.send(h, 200, [dict({"message": x}, **({"taskId": t} if self.task_ids_in_logs else {})) for t, x in rows])
            m = re.fullmatch(re.escape(p) + r"/namespaces/([^/]+)/kv/([^/]+)", path)
            if method == "GET" and m:
                key = urllib.parse.unquote(m.group(2))
                if key not in self.kv:
                    return self.send(h, 404, {"message": "not found"})
                return self.send(h, 200, {"type": "STRING", "value": self.kv[key]})
            return self.send(h, 404, {"message": "no such route %s" % path})

    # ---- executions
    def create_execution(self, h, body):
        fields = dict(re.findall(rb'name="([^"]+)"\r\n\r\n(.*?)\r\n--', body, re.S))
        inputs = {k.decode(): v.decode() for k, v in fields.items()}
        ex_id = "ex%02d" % (len(self.executions) + 1)
        running_other = [e for e in self.order if not self.executions[e]["finished"]]
        ex = {"id": ex_id, "inputs": inputs, "polls": 0, "finished": False, "blocked_by": None, "histories": [("CREATED", self.stamp())], "state": "CREATED",
              "tasks": [], "logs": []}
        if self.enforce_concurrency and running_other:
            ex["blocked_by"] = running_other[0]
            ex["histories"].append(("QUEUED", self.stamp()))
            ex["state"] = "QUEUED"
        else:
            ex["histories"].append(("RUNNING", self.stamp()))
            ex["state"] = "RUNNING"
        self.executions[ex_id] = ex
        self.order.append(ex_id)
        self.send(h, 200, {"id": ex_id})

    def get_execution(self, h, ex_id):
        ex = self.executions.get(ex_id)
        if ex is None:
            return self.send(h, 404, {})
        self.tick()
        self.send(h, 200, self.render(ex))

    def tick(self):
        """Every poll advances ALL executions, like a real server where executions progress on their own."""
        for ex_id in self.order:
            ex = self.executions[ex_id]
            if ex["finished"] or self.stall:
                continue
            if ex["blocked_by"]:
                if not self.executions[ex["blocked_by"]]["finished"]:
                    continue
                ex["blocked_by"] = None
                ex["histories"].append(("RUNNING", self.stamp()))
                ex["state"] = "RUNNING"
            ex["polls"] += 1
            if ex["polls"] >= 2:
                self.finalize(ex)

    def render(self, ex):
        data = {"id": ex["id"], "state": {"current": ex["state"], "histories": [{"state": s, "date": d} for s, d in ex["histories"]]}}
        if self.task_list:
            data["taskRunList"] = [t if self.outputs else {k: v for k, v in t.items() if k != "outputs"} for t in ex["tasks"]]
        return data

    def task(self, task_id, state, outputs=None):
        return {"taskId": task_id, "state": {"current": state}, "outputs": outputs or {}}

    @staticmethod
    def summary_line(gene, phase, mode, scanned, total, size, requests):
        return ("%s: %s (%s). scanned=%d of %d matching records, new=0, reclassified=0, newly_classified=0, review_status_only=0, "
                "classification_missing=0, snapshot_size=%d, requests=%d" % (gene, phase, mode, scanned, total, size, requests))

    def finalize(self, ex):
        inputs = ex["inputs"]
        gene = json.loads(inputs["genes"])[0].upper()
        key = "%s_%s" % (inputs["state_key_prefix"], gene.lower())
        existing = json.loads(self.kv[key]) if key in self.kv else None
        extra = inputs.get("extra_query", "")
        today = datetime.now(timezone.utc).date().isoformat()
        ok = ["validate_genes", "read_snapshot"]
        tasks = [self.task(t, "SUCCESS") for t in ok]
        failed = gene.startswith("ZZZ") or (existing is not None and (existing.get("query") or "") != extra)
        if failed:
            tasks += [self.task("compare_snapshots", "FAILED"), self.task("log_failure", "SUCCESS")]
            reason = ("ClinVar does not recognise part of the query for %s: ['%s[gene]']." % (gene, gene)) if gene.startswith("ZZZ") else (
                "Stored snapshot was built with a different extra query ('', now %r)" % extra)
            ex["logs"] = [("compare_snapshots", reason), ("log_failure", "ClinVar monitor FAILED in flow clinvar-variant-reclassification-monitor")]
            if self.run_slack:
                ex["logs"].append(("slack_failure", "slack sent"))
            if self.tamper_kv_on_failure and existing:
                existing["checkpoint"] = "1999-01-01"
                self.kv[key] = json.dumps(existing, separators=(",", ":"))
            state = "FAILED"
        else:
            if existing is None:
                snap = {"schema": 2, "gene": gene, "query": extra, "checkpoint": today, "reconciled": today, "classes": ["Pathogenic"], "reviews": ["x"],
                        "variants": {str(i): [0, 0] for i in range(707)}}
                v = {"gene": gene, "mode": "FULL", "baseline": True, "phase": "BASELINE", "alertable": False, "scanned": 707, "total_matches": 707,
                     "slack_text": "", "summary": "%s: BASELINE (full). scanned=707 of 707" % gene, "date_window_comparison": "NOT APPLICABLE"}
                line = self.summary_line(gene, "BASELINE", "full", 707, 707, 707, 6)
                ex["logs"] = [("compare_snapshots", "Pulling image python:3.12-slim for the task"),
                              ("compare_snapshots", "  Baseline stored for %s: 707 records. No alert is sent on the first run." % gene),
                              ("compare_snapshots", line),
                              ("run_summary", "ClinVar monitor finished.\n- %s" % line)]
            else:
                snap = dict(existing, checkpoint=today)
                v = {"gene": gene, "mode": "INCREMENTAL", "baseline": False, "phase": "NO CHANGES", "alertable": False, "scanned": 28, "total_matches": 28,
                     "slack_text": "", "summary": "%s: NO CHANGES (incremental)" % gene, "date_window_comparison": "NOT APPLICABLE"}
                line = self.summary_line(gene, "NO CHANGES", "incremental", 28, 28, 707, 3)
                ex["logs"] = [("compare_snapshots", line), ("run_summary", "ClinVar monitor finished.\n- %s" % line)]
            self.kv[key] = json.dumps(snap, separators=(",", ":"))
            if self.empty_kv_after_baseline and existing is None:
                self.kv[key] = json.dumps({"schema": 2, "gene": gene, "variants": {}})
            tasks += [self.task("compare_snapshots", "SUCCESS", {"vars": v}), self.task("report_gene", "SUCCESS"), self.task("notify_changes", "SUCCESS"),
                      self.task("save_snapshot", "SUCCESS"), self.task("run_summary", "SUCCESS")]
            if self.run_slack:
                tasks.append(self.task("slack_alert", "SUCCESS"))
                ex["logs"].append(("slack_alert", "slack sent"))
            state = "SUCCESS"
        ex["tasks"], ex["state"], ex["finished"] = tasks, state, True
        ex["histories"].append((state, self.stamp()))


WORK = Path(os.environ.get("TEST_DRIVER_TMP") or os.environ.get("TEMP") or ".")
STUB = WORK / ("docker-stub-%d.py" % os.getpid())
STUB_SOURCE = '''import os, sys
args = sys.argv[1:]
if os.environ.get("STUB_MODE") == "broken":
    sys.exit(1)
if args and args[0] == "inspect":
    print(os.environ.get("STUB_LABEL", "2.0.6"))
elif args and args[0] == "events":
    for i in range(int(os.environ.get("STUB_EVENTS", "6"))):
        print("c%d" % i)
else:
    sys.exit(2)
'''


def setUpModule():
    STUB.write_text(STUB_SOURCE, encoding="utf-8")


def tearDownModule():
    STUB.unlink(missing_ok=True)


def run_driver(fake, flow_text=SAMPLE_FLOW, extra_args=(), env=None, docker=True):
    flow_file = WORK / ("flow-%d.yaml" % threading.get_ident())
    flow_file.write_text(flow_text, encoding="utf-8")
    keys = ("KESTRA_USER", "KESTRA_PASSWORD", "DRIVER_POLL_SECONDS", "GITHUB_STEP_SUMMARY", "DRIVER_DOCKER_COMMAND", "DRIVER_EXEC_TIMEOUT",
            "STUB_MODE", "STUB_LABEL", "STUB_EVENTS")
    old = {k: os.environ.get(k) for k in keys}
    os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD, "DRIVER_POLL_SECONDS": "0.01", "DRIVER_EXEC_TIMEOUT": "20"})
    os.environ.pop("GITHUB_STEP_SUMMARY", None)
    if docker:
        os.environ["DRIVER_DOCKER_COMMAND"] = json.dumps([sys.executable, "-I", str(STUB)])
    else:
        os.environ["DRIVER_DOCKER_COMMAND"] = json.dumps(["no-such-docker-binary-for-the-tests"])
    os.environ.update(env or {})
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = driver.main(["--flow", str(flow_file), "--base-url", fake.url, "--startup-timeout", "5", *extra_args])
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        flow_file.unlink(missing_ok=True)
        driver.SECRETS.clear()
    return code, buffer.getvalue()


class DriverAgainstFakeKestra(unittest.TestCase):
    def fake(self, **kw):
        f = FakeKestra(**kw)
        self.addCleanup(f.close)
        return f

    def statuses(self, out, status):
        return [ln for ln in out.splitlines() if ln.startswith(status)]

    def test_happy_path_passes_every_check_and_prints_no_password(self):
        code, out = run_driver(self.fake())
        self.assertEqual(code, 0, out)
        self.assertEqual(self.statuses(out, "FAIL"), [], out)
        self.assertEqual(self.statuses(out, "UNVERIFIED"), [], out)
        self.assertGreaterEqual(len(self.statuses(out, "PASS")), driver.MIN_PASS)
        self.assertNotIn("::warning", out)
        self.assertNotIn(PASSWORD, out)
        import base64
        self.assertNotIn(base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode(), out)

    def test_every_scenario_is_exercised(self):
        _, out = run_driver(self.fake())
        for label in ("S1 baseline", "S2 incremental", "S3 invalid gene", "S4 changed extra query", "S5 concurrency"):
            self.assertIn(label, out)

    def test_an_old_kestra_version_fails(self):
        code, out = run_driver(self.fake(version="1.3.43"))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "the server is Kestra 2.0.x" in ln], out)

    def test_the_tenantless_api_form_is_detected(self):
        code, out = run_driver(self.fake(prefix="/api/v1"))
        self.assertEqual(code, 0, out)
        self.assertIn("API prefix /api/v1", out)

    def test_a_slack_task_that_runs_is_detected(self):
        code, out = run_driver(self.fake(run_slack=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "no Slack task ran" in ln], out)

    def test_a_failed_run_that_changes_the_stored_snapshot_is_detected(self):
        code, out = run_driver(self.fake(tamper_kv_on_failure=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "byte-for-byte unchanged" in ln], out)

    def test_missing_concurrency_enforcement_is_detected(self):
        code, out = run_driver(self.fake(enforce_concurrency=False))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "did not run at the same time" in ln], out)

    def test_the_driver_never_needs_task_outputs(self):
        # REGRESSION F1: the 2.0 execution JSON carries no outputs; the default fake has none and the run must still be accepted
        for fake in (self.fake(task_list=False), self.fake(task_list=True, outputs=False), self.fake(task_list=True, outputs=True)):
            code, out = run_driver(fake)
            self.assertEqual(code, 0, out)
            self.assertEqual(self.statuses(out, "FAIL"), [], out)

    def test_missing_outputs_do_not_cause_the_old_false_failure(self):
        # REGRESSION F2: an absent summary must never be compared with the snapshot (previously None vs a count -> FAIL)
        code, out = run_driver(self.fake(logs=False))
        self.assertEqual(self.statuses(out, "FAIL"), [], out)
        self.assertTrue([ln for ln in self.statuses(out, "UNVERIFIED") if "snapshot's entry count equals" in ln], out)
        self.assertTrue([ln for ln in self.statuses(out, "PASS") if "schema 2 snapshot" in ln], out)

    def test_an_empty_snapshot_is_a_real_failure(self):
        code, out = run_driver(self.fake(empty_kv_after_baseline=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "schema 2 snapshot" in ln], out)

    def test_the_slack_check_works_from_the_task_list_when_present(self):
        code, out = run_driver(self.fake(task_list=True, run_slack=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "no Slack task ran" in ln and "S1" in ln], out)

    def test_an_unavailable_logs_endpoint_leaves_critical_slack_checks_unverified_and_fails_the_job(self):
        # without logs and without a task list there is no evidence that no Slack task ran: critical, so the job must not be green
        code, out = run_driver(self.fake(logs=False))
        self.assertEqual(code, 1, out)
        unverified = self.statuses(out, "UNVERIFIED")
        self.assertTrue([ln for ln in unverified if "no Slack task ran" in ln], out)
        self.assertIn("::error title=Integration run not accepted::", out)
        self.assertIn("s1_slack=UNVERIFIED", out)

    def test_every_unverified_result_emits_a_github_warning_annotation(self):
        code, out = run_driver(self.fake(logs=False))
        unverified = self.statuses(out, "UNVERIFIED")
        warnings = [ln for ln in out.splitlines() if ln.startswith("::warning title=UNVERIFIED::")]
        self.assertGreaterEqual(len(unverified), 3)
        self.assertEqual(len(warnings), len(unverified), out)

    def test_critical_checks_can_be_established_from_the_task_list_when_logs_are_missing(self):
        code, out = run_driver(self.fake(logs=False, task_list=True))
        self.assertTrue([ln for ln in self.statuses(out, "UNVERIFIED") if "script's summary line" in ln or "loopOutputs" in ln], out)
        self.assertEqual(self.statuses(out, "FAIL"), [], out)
        self.assertNotIn("s1_slack=", out)

    def test_missing_kestra_version_evidence_fails_the_job(self):
        code, out = run_driver(self.fake(configs=False), docker=False)
        self.assertEqual(code, 1, out)
        self.assertTrue([ln for ln in self.statuses(out, "UNVERIFIED") if "the server is Kestra 2.0.x" in ln], out)
        self.assertIn("version=UNVERIFIED", out)

    def test_a_nightly_image_label_with_a_2_0_6_config_endpoint_passes(self):
        # REGRESSION (run 39011785668): the real image label said "nightly" while /configs said 2.0.6
        code, out = run_driver(self.fake(), env={"STUB_LABEL": "nightly"})
        self.assertEqual(code, 0, out)
        self.assertTrue([ln for ln in self.statuses(out, "PASS") if "the server is Kestra 2.0.x" in ln and "'2.0.6'" in ln], out)
        self.assertTrue([ln for ln in self.statuses(out, "INFO") if "image label" in ln and "nightly" in ln], out)

    def test_an_image_label_never_decides_the_version_check(self):
        for label in ("1.3.43", "2.0.6", "nightly", "<no value>", ""):
            code, out = run_driver(self.fake(), env={"STUB_LABEL": label})
            self.assertEqual(code, 0, (label, out))
            self.assertEqual(self.statuses(out, "FAIL"), [], (label, out))

    def test_an_incompatible_config_version_fails_whatever_the_label_says(self):
        for version in ("1.3.43", "2.1.0", "3.0.0", "20.0.6", "2.0", "2.0.", "2.0.x", "nightly", "v1.0.0"):
            code, out = run_driver(self.fake(version=version), env={"STUB_LABEL": "2.0.6"})
            self.assertEqual(code, 1, (version, out))
            self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "the server is Kestra 2.0.x" in ln], (version, out))

    def test_release_style_config_versions_are_accepted(self):
        for version in ("2.0.0", "2.0.6", "v2.0.12", "2.0.7-rc1", "2.0.6+build.5"):
            code, out = run_driver(self.fake(version=version))
            self.assertEqual(code, 0, (version, out))

    def test_a_missing_or_malformed_config_endpoint_never_produces_a_pass(self):
        for configs in (False, "malformed", "noversion", "nonstring"):
            for label in ("2.0.6", None):
                env = {"STUB_LABEL": label} if label else {"STUB_MODE": "broken"}
                code, out = run_driver(self.fake(configs=configs), env=env)
                self.assertEqual(code, 1, (configs, label, out))
                self.assertEqual([ln for ln in self.statuses(out, "PASS") if "the server is Kestra" in ln], [], (configs, label, out))
                self.assertIn("version=UNVERIFIED", out)

    def test_an_absent_image_label_changes_nothing(self):
        code, out = run_driver(self.fake(), docker=False)
        self.assertTrue([ln for ln in self.statuses(out, "PASS") if "the server is Kestra 2.0.x" in ln], out)
        self.assertTrue([ln for ln in self.statuses(out, "INFO") if "image label" in ln and "absent" in ln], out)

    def test_the_gallery_metadata_is_stripped_before_deployment_and_the_run_is_accepted(self):
        # REGRESSION (run 39011785668): Kestra rejected the top-level extend: block
        fake = self.fake()
        code, out = run_driver(fake, flow_text=SAMPLE_WITH_EXTEND)
        self.assertEqual(code, 0, out)
        deployed = next(iter(fake.flows.values()))
        self.assertNotRegex(deployed, r"(?m)^extend:")
        self.assertIn("extend: nested-and-kept", deployed)
        self.assertEqual(yaml.safe_load(deployed)["tasks"][0]["extend"], "nested-and-kept")
        self.assertTrue([ln for ln in self.statuses(out, "PASS") if "Kestra validates the flow YAML" in ln], out)

    def test_without_stripping_the_fake_rejects_the_flow_like_the_real_server(self):
        fake = self.fake()
        original = driver.strip_gallery_metadata
        driver.strip_gallery_metadata = lambda text: (text, 0)
        try:
            code, out = run_driver(fake, flow_text=SAMPLE_WITH_EXTEND)
        finally:
            driver.strip_gallery_metadata = original
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "the flow is created" in ln], out)
    def test_missing_docker_runner_evidence_fails_the_job(self):
        code, out = run_driver(self.fake(), docker=False)
        self.assertEqual(code, 1, out)
        self.assertIn("docker_runner=UNVERIFIED", out)
        code, out = run_driver(self.fake(), env={"STUB_MODE": "broken"})
        self.assertEqual(code, 1, out)
        self.assertIn("docker_runner=UNVERIFIED", out)

    def test_no_container_creation_events_is_a_failure(self):
        code, out = run_driver(self.fake(), env={"STUB_EVENTS": "0"})
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "Docker daemon created task containers" in ln], out)

    def test_the_documented_minimum_pass_count_is_enforced(self):
        code, out = run_driver(self.fake(), extra_args=("--min-pass", "500"))
        self.assertEqual(code, 1)
        self.assertIn("the documented minimum is 500", out)
        code, out = run_driver(self.fake(), extra_args=("--min-pass", str(driver.MIN_PASS)))
        self.assertEqual(code, 0, out)

    def test_critical_checks_that_never_ran_fail_the_job(self):
        # S1 stalls, so S2..S5 never run; their critical keys are NOT RUN
        code, out = run_driver(self.fake(stall=True), env={"DRIVER_EXEC_TIMEOUT": "0.3"})
        self.assertEqual(code, 1)
        self.assertIn("s2_success=NOT RUN", out)

    def test_critical_keys_cover_the_required_evidence(self):
        for key in ("version", "docker_runner", "s1_slack", "s1_snapshot", "s3_kv_unchanged", "s4_kv_unchanged"):
            self.assertIn(key, driver.CRITICAL)

    def test_both_encodings_of_the_credentials_are_redacted(self):
        import base64
        code, out = run_driver(self.fake())
        for secret in (PASSWORD, base64.b64encode(PASSWORD.encode()).decode(), base64.b64encode(("%s:%s" % (USER, PASSWORD)).encode()).decode()):
            self.assertNotIn(secret, out)

    def test_a_validation_error_in_the_flow_fails(self):
        code, out = run_driver(self.fake(flow_validation_error=True))
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "validates the flow YAML" in ln], out)

    def test_an_execution_that_never_finishes_is_a_failure_not_a_hang(self):
        code, out = run_driver(self.fake(stall=True), env={"DRIVER_EXEC_TIMEOUT": "0.3"})
        self.assertEqual(code, 1)
        self.assertTrue([ln for ln in self.statuses(out, "FAIL") if "execution ends SUCCESS" in ln], out)
        self.assertIn("S2 to S5 skipped", out)

    def test_the_concurrency_result_does_not_depend_on_which_execution_is_created_first(self):
        for _ in range(8):
            code, out = run_driver(self.fake())
            self.assertEqual(code, 0, out)

    def test_wrong_credentials_fail_fast_without_a_traceback(self):
        code, out = run_driver(self.fake(), env={"KESTRA_PASSWORD": "Wrong1password"})
        self.assertEqual(code, 1)
        self.assertNotIn("Traceback", out)
        self.assertIn("HTTP 401", out)

    def test_a_server_that_never_starts_is_a_failure(self):
        fake = self.fake()
        fake.close()
        code, out = run_driver(fake, extra_args=("--startup-timeout", "1"))
        self.assertEqual(code, 1)
        self.assertIn("became ready", out)

    def test_the_driver_refuses_a_non_loopback_target(self):
        old = os.environ.get("KESTRA_USER"), os.environ.get("KESTRA_PASSWORD")
        os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD})
        try:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = driver.main(["--flow", str(HERE / "run_integration.py"), "--base-url", "https://kestra.invalid"])
        finally:
            for k, v in zip(("KESTRA_USER", "KESTRA_PASSWORD"), old):
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
            driver.SECRETS.clear()
        self.assertEqual(code, 2)
        self.assertIn("refusing to talk", buffer.getvalue())

    def test_missing_credentials_are_refused(self):
        old = os.environ.pop("KESTRA_PASSWORD", None)
        try:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = driver.main(["--flow", "x"])
        finally:
            if old is not None:
                os.environ["KESTRA_PASSWORD"] = old
        self.assertEqual(code, 2)

    def test_the_step_summary_file_is_written_when_requested(self):
        target = Path(os.environ.get("TEST_DRIVER_TMP") or os.environ.get("TEMP") or ".") / "summary-test.md"
        target.unlink(missing_ok=True)
        try:
            run_driver(self.fake(), env={"GITHUB_STEP_SUMMARY": str(target)})
            text = target.read_text(encoding="utf-8")
            self.assertIn("| Status | Check | Detail |", text)
            self.assertNotIn(PASSWORD, text)
        finally:
            target.unlink(missing_ok=True)


class Helpers(unittest.TestCase):
    def test_the_only_change_to_the_flow_is_one_added_line(self):
        out = driver.disable_trigger(SAMPLE_FLOW)
        before, after = SAMPLE_FLOW.splitlines(), out.splitlines()
        self.assertEqual(len(after), len(before) + 1)
        self.assertEqual([ln for ln in after if ln not in before], ["    disabled: true"])
        self.assertEqual(yaml.safe_load(out)["triggers"][0]["disabled"], True)

    def test_stripping_removes_only_the_top_level_extend_block(self):
        stripped, removed = driver.strip_gallery_metadata(SAMPLE_WITH_EXTEND)
        self.assertGreater(removed, 5)
        before, after = yaml.safe_load(SAMPLE_WITH_EXTEND), yaml.safe_load(stripped)
        self.assertIn("extend", before)
        self.assertNotIn("extend", after)
        before.pop("extend")
        self.assertEqual(before, after, "everything except the top-level extend key is preserved")
        self.assertEqual(after["tasks"][0]["extend"], "nested-and-kept", "a nested key named extend survives")
        self.assertEqual(stripped, SAMPLE_FLOW.replace("    message: hi\n", "    message: hi\n    extend: nested-and-kept\n"), "the remaining text is byte-identical")

    def test_content_after_the_extend_block_is_preserved(self):
        text = SAMPLE_FLOW + "extend:\n  title: t\n\n# trailing comment\nafter: 1\n"
        stripped, _ = driver.strip_gallery_metadata(text)
        self.assertEqual(yaml.safe_load(stripped)["after"], 1)
        self.assertNotIn("extend", yaml.safe_load(stripped))
        self.assertIn("# trailing comment", stripped)
    def test_a_flow_without_extend_is_returned_unchanged(self):
        self.assertEqual(driver.strip_gallery_metadata(SAMPLE_FLOW), (SAMPLE_FLOW, 0))

    def test_two_top_level_extend_keys_are_refused(self):
        with self.assertRaises(ValueError):
            driver.strip_gallery_metadata(SAMPLE_WITH_EXTEND + "extend:\n  x: 1\n")

    def test_text_that_merely_mentions_extend_is_not_stripped(self):
        text = SAMPLE_FLOW.replace("message: hi", "message: |\n      extend: inside a block scalar\n      extended: no")
        self.assertEqual(driver.strip_gallery_metadata(text), (text, 0))
        self.assertEqual(driver.strip_gallery_metadata(text.replace("tasks:", "extended: 1\ntasks:"))[1], 0)

    def test_the_deployment_copy_keeps_identity_inputs_tasks_and_has_the_disabled_trigger(self):
        flow = SAMPLE_WITH_EXTEND.replace("tasks:", "inputs:\n  - id: genes\n    type: JSON\n    defaults: '[\"MAX\"]'\n\ntasks:", 1)
        deployed, removed = driver.prepare_deployment(flow)
        before, after = yaml.safe_load(flow), yaml.safe_load(deployed)
        self.assertEqual(driver.flow_identity(deployed), driver.flow_identity(flow))
        for key in ("id", "namespace", "inputs", "tasks"):
            self.assertEqual(before[key], after[key])
        self.assertIs(after["triggers"][0]["disabled"], True)
        after["triggers"][0].pop("disabled")
        before.pop("extend")
        self.assertEqual(before, after)
        self.assertEqual(sorted(set(after) ^ set(yaml.safe_load(flow))), ["extend"])

    def test_the_original_flow_text_is_never_modified(self):
        original = SAMPLE_WITH_EXTEND
        copy = str(original)
        driver.prepare_deployment(original)
        self.assertEqual(original, copy)

    def test_the_driver_never_writes_the_flow_file(self):
        flow_file = WORK / "flow-readonly-check.yaml"
        flow_file.write_text(SAMPLE_WITH_EXTEND, encoding="utf-8", newline="")
        before = flow_file.read_bytes()
        fake = FakeKestra()
        try:
            old = {k: os.environ.get(k) for k in ("KESTRA_USER", "KESTRA_PASSWORD", "DRIVER_POLL_SECONDS", "DRIVER_DOCKER_COMMAND")}
            os.environ.update({"KESTRA_USER": USER, "KESTRA_PASSWORD": PASSWORD, "DRIVER_POLL_SECONDS": "0.01",
                               "DRIVER_DOCKER_COMMAND": json.dumps([sys.executable, "-I", str(STUB)])})
            with redirect_stdout(io.StringIO()):
                driver.main(["--flow", str(flow_file), "--base-url", fake.url, "--startup-timeout", "5"])
        finally:
            fake.close()
            driver.SECRETS.clear()
            for k, v in old.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        self.assertEqual(flow_file.read_bytes(), before)
        flow_file.unlink(missing_ok=True)

    @unittest.skipUnless(os.environ.get("CLINVAR_FLOW"), "CLINVAR_FLOW not set")
    def test_the_real_flow_deployment_copy_differs_only_by_extend_and_the_disabled_line(self):
        raw = Path(os.environ["CLINVAR_FLOW"]).read_bytes()
        text = raw.decode("utf-8").replace("\r\n", "\n")
        deployed, removed = driver.prepare_deployment(text)
        before, after = yaml.safe_load(text), yaml.safe_load(deployed)
        self.assertIn("extend", before)
        self.assertNotIn("extend", after)
        self.assertGreater(removed, 5)
        self.assertIs(after["triggers"][0].pop("disabled"), True)
        before.pop("extend")
        self.assertEqual(before, after, "tasks, inputs, errors, triggers and every other key are preserved")
        self.assertEqual(driver.flow_identity(deployed), ("company.team", "clinvar-variant-reclassification-monitor"))
        self.assertEqual(Path(os.environ["CLINVAR_FLOW"]).read_bytes(), raw)

    def test_the_transformation_refuses_a_flow_without_exactly_one_schedule_trigger(self):
        with self.assertRaises(ValueError):
            driver.disable_trigger("id: a\nnamespace: b\n")
        with self.assertRaises(ValueError):
            driver.disable_trigger(SAMPLE_FLOW + driver.TRIGGER_MARKER)

    @unittest.skipUnless(os.environ.get("CLINVAR_FLOW"), "CLINVAR_FLOW not set")
    def test_the_real_flow_is_transformed_by_exactly_one_line(self):
        text = Path(os.environ["CLINVAR_FLOW"]).read_text(encoding="utf-8").replace("\r\n", "\n")
        out = driver.disable_trigger(text)
        a, b = text.splitlines(), out.splitlines()
        self.assertEqual(len(b), len(a) + 1)
        self.assertEqual([ln for ln in b if ln not in a], ["    disabled: true"])
        parsed_before, parsed_after = yaml.safe_load(text), yaml.safe_load(out)
        self.assertIs(parsed_after["triggers"][0]["disabled"], True)
        parsed_after["triggers"][0].pop("disabled")
        self.assertEqual(parsed_before, parsed_after, "nothing but the trigger flag may differ")
        self.assertEqual(driver.flow_identity(text), ("company.team", "clinvar-variant-reclassification-monitor"))

    def test_multipart_encoding(self):
        body, ctype = driver.multipart({"genes": '["MAX"]', "notify_slack": "false"})
        boundary = ctype.split("boundary=")[1]
        text = body.decode()
        self.assertIn('name="genes"\r\n\r\n["MAX"]\r\n', text)
        self.assertIn('name="notify_slack"\r\n\r\nfalse\r\n', text)
        self.assertTrue(text.endswith("--%s--\r\n" % boundary))

    def test_redaction_removes_secrets_from_everything_printed(self):
        driver.SECRETS[:] = ["hunter2hunter2"]
        try:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                driver.say("login hunter2hunter2 ok")
            self.assertEqual(buffer.getvalue().strip(), "login *** ok")
            self.assertNotIn("hunter2hunter2", driver.snippet("body hunter2hunter2"))
        finally:
            driver.SECRETS.clear()

    def test_every_encoding_of_the_credentials_is_registered_for_redaction(self):
        # FIX 7: the raw password, its base64 form and the base64 Basic token must all be removed from anything printed
        import base64
        driver.SECRETS.clear()
        try:
            driver.Client("http://127.0.0.1:1", USER, "Ci1onlyfortestsXYZ")
            forms = ["Ci1onlyfortestsXYZ", base64.b64encode(b"Ci1onlyfortestsXYZ").decode(), base64.b64encode(("%s:Ci1onlyfortestsXYZ" % USER).encode()).decode()]
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                for form in forms:
                    driver.say("value", form)
            for form in forms:
                self.assertNotIn(form, buffer.getvalue())
            self.assertEqual(buffer.getvalue().count("***"), 3)
        finally:
            driver.SECRETS.clear()
    def test_time_parsing_accepts_nanosecond_style_timestamps(self):
        self.assertIsNotNone(driver.parse_time("2026-10-17T05:56:08.123456789Z"))
        self.assertIsNone(driver.parse_time("not a date"))


class WorkflowStaticChecks(unittest.TestCase):
    text = WORKFLOW.read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    steps = wf["jobs"]["integration"]["steps"]

    def runs(self):
        return [s.get("run", "") for s in self.steps]

    def test_least_privilege_and_triggers(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        events = set(self.wf[True])
        self.assertEqual(events, {"push", "workflow_dispatch"})
        self.assertEqual(self.wf[True]["push"]["branches"], ["ci/kestra-integration"])
        self.assertNotIn("schedule", events)

    def test_actions_are_pinned_to_full_commit_shas(self):
        uses = [s["uses"] for s in self.steps if "uses" in s]
        self.assertEqual(len(uses), 3)
        for u in uses:
            self.assertRegex(u, r"^[\w./-]+@[0-9a-f]{40}$")

    def test_the_kestra_image_is_pinned_by_digest_and_is_a_2_0_release(self):
        image = self.wf["env"]["KESTRA_IMAGE"]
        self.assertRegex(image, r"^kestra/kestra:v2\.0\.\d+@sha256:[0-9a-f]{64}$")

    def test_no_repository_secret_and_no_slack_or_ncbi_configuration(self):
        self.assertNotRegex(self.text, r"secrets\.")
        self.assertNotRegex(self.text, r"SLACK|NCBI_API_KEY|SECRET_")

    def test_run_steps_interpolate_no_github_expressions(self):
        for r in self.runs():
            self.assertNotIn("${{", r)

    def test_the_only_expression_using_an_input_is_the_checkout_ref(self):
        used = re.findall(r"\$\{\{[^}]*inputs\.[^}]*\}\}", self.text)
        self.assertEqual(len(used), 1)
        self.assertIn("ref:", [ln.strip().split(" ")[0] for ln in self.text.splitlines() if "inputs.flow_ref ||" in ln][0])

    def test_the_server_is_bound_to_loopback_only_and_not_privileged(self):
        start = next(r for r in self.runs() if "docker run -d" in r)
        self.assertIn("-p 127.0.0.1:8080:8080", start)
        self.assertNotIn("--privileged", start)
        socket_lines = [ln.strip() for ln in self.text.splitlines() if "docker.sock" in ln]
        self.assertEqual(socket_lines, ["-v /var/run/docker.sock:/var/run/docker.sock \\"], "the Docker socket may be mounted in exactly one place")

    def test_the_password_is_masked_before_it_is_stored_and_meets_kestra_policy(self):
        start = next(r for r in self.runs() if "add-mask" in r)
        self.assertLess(start.index("::add-mask::"), start.index("GITHUB_ENV"))
        # FIX 7: the raw password and both base64 forms are masked before anything is written or used
        self.assertEqual(start.count("::add-mask::"), 3)
        for mask in ('::add-mask::$password"', '::add-mask::$b64_password"', '::add-mask::$b64_basic"'):
            self.assertIn(mask, start)
        self.assertLess(start.rindex("::add-mask::"), start.index("GITHUB_ENV"))
        self.assertLess(start.index("b64_basic="), start.index("::add-mask::"))
        for line in start.splitlines():
            if re.search(r"\$\{?(password|b64_password|b64_basic)\b", line) and line.lstrip().startswith("echo"):
                self.assertTrue("::add-mask::" in line or line.rstrip().endswith('>> "$GITHUB_ENV"'), "a credential is printed: " + line)
        self.assertIn('password="Ci1$(openssl rand -hex 16)"', start)
        sample = "Ci1" + "ab12" * 8  # a worst case with no further upper-case letter
        self.assertTrue(len(sample) >= 8 and re.search(r"[A-Z]", sample) and re.search(r"[a-z]", sample) and re.search(r"\d", sample))
        self.assertIn("username: ci@example.com", start)

    def test_server_logs_are_redacted_and_the_container_is_always_removed(self):
        always = [s for s in self.steps if s.get("if") == "always()"]
        self.assertEqual(len(always), 2)
        self.assertIn("sed", always[0]["run"])
        for var in ("KESTRA_PASSWORD", "KESTRA_PASSWORD_B64", "KESTRA_BASIC_B64"):
            self.assertIn("$" + var + "|", always[0]["run"])
        self.assertIn("docker rm -f kestra", always[1]["run"])

    def test_the_flow_commit_is_asserted_not_just_printed(self):
        # FIX 4
        self.assertEqual(self.wf["env"]["EXPECTED_FLOW_SHA"], "f530d0a8aaa54f2ac12d59d3b8089bcbc0abb527")
        step = next(r for r in self.runs() if "EXPECTED_FLOW_SHA" in r and "rev-parse HEAD" in r)
        self.assertRegex(step, r'if \[ "\$actual" != "\$EXPECTED_FLOW_SHA" \]')
        self.assertIn("exit 1", step)
        names = [s.get("name", "") for s in self.steps]
        assert_at = next(i for i, n in enumerate(names) if n.startswith("Assert the flow commit"))
        self.assertLess(assert_at, next(i for i, n in enumerate(names) if n.startswith("Unit tests")))

    def test_anonymous_usage_reporting_is_disabled_at_the_documented_place(self):
        # FIX 5: both keys are direct children of the top-level `kestra:` key (Kestra v2.0.6 application.yml)
        start = next(r for r in self.runs() if "add-mask" in r)
        block = re.search(r"cat <<EOF\n(.*?)\n\s*EOF", start, re.S).group(1)
        config = yaml.safe_load("\n".join(ln[10:] if ln.startswith(" " * 10) else ln for ln in block.replace("$password", "pw").splitlines()))
        self.assertIs(config["kestra"]["anonymous-usage-report"]["enabled"], False)
        self.assertIs(config["kestra"]["ui-anonymous-usage-report"]["enabled"], False)
        self.assertEqual(config["kestra"]["server"]["basic-auth"]["username"], "ci@example.com")

    def test_the_flow_task_image_is_pinned_by_a_verified_digest_and_the_platform_is_checked(self):
        # FIX 6
        self.assertRegex(self.wf["env"]["FLOW_IMAGE_PINNED"], r"^python@sha256:[0-9a-f]{64}$")
        self.assertEqual(self.wf["env"]["FLOW_IMAGE_PINNED"], "python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1")
        pull = next(r for r in self.runs() if "docker pull" in r)
        self.assertIn('docker pull "$FLOW_IMAGE_PINNED"', pull)
        self.assertNotRegex(pull, r'docker pull "\$FLOW_IMAGE"')
        self.assertIn("linux/amd64", pull)
        self.assertRegex(pull, r'if \[ "\$platform" != "linux/amd64" \]; then\n(.*\n)*?\s*exit 1\n\s*fi\n\s*docker tag')
        self.assertLess(pull.index("linux/amd64"), pull.index("docker tag"))
        self.assertIn('docker tag "$FLOW_IMAGE_PINNED" "$FLOW_IMAGE"', pull)

    def test_strict_driver_invocation(self):
        # FIX 3
        call = next(r for r in self.runs() if "run_integration.py" in r and "test_" not in r)
        for part in ('--min-pass "$MIN_PASS"', "--container kestra", '--flow-image "$FLOW_IMAGE"'):
            self.assertIn(part, call)
        self.assertEqual(self.wf["env"]["MIN_PASS"], str(driver.MIN_PASS))

    def test_no_artifact_upload_no_extra_privileges_and_no_host_network(self):
        self.assertNotRegex(self.text, r"upload-artifact|--cap-add|--network|--net |--pid|--ipc|--device|self-hosted")

    def test_pyyaml_is_installed_from_wheels_only_with_a_pinned_version(self):
        install = next(r for r in self.runs() if "pip install" in r)
        self.assertIn("--only-binary=:all:", install)
        self.assertIn("pyyaml==6.0.3", install)

    def test_the_flow_ref_default_is_the_feature_branch_and_the_job_has_a_timeout(self):
        self.assertEqual(self.wf[True]["workflow_dispatch"]["inputs"]["flow_ref"]["default"], "feat/clinvar-variant-reclassification-monitor")
        self.assertEqual(self.wf["jobs"]["integration"]["timeout-minutes"], 45)
        self.assertEqual(self.wf["jobs"]["integration"]["runs-on"], "ubuntu-24.04")

    def test_the_driver_is_called_with_the_unmodified_flow_path_and_the_small_gene(self):
        call = next(r for r in self.runs() if "run_integration.py" in r and "test_" not in r)
        self.assertIn("flow-under-test/flows/clinvar-variant-reclassification-monitor.yaml", call)
        self.assertIn("--gene MAX", call)
        self.assertIn("--base-url http://127.0.0.1:8080", call)


if __name__ == "__main__":
    unittest.main(verbosity=2)
