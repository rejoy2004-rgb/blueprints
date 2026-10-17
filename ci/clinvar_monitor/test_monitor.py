"""Tests for the hybrid ClinVar monitor (full baseline, [mdat] incremental scans, periodic reconciliation).

The Python task is extracted from the flow YAML and run as a subprocess against a local mock of NCBI
E-utilities (MockNCBI below). Nothing here contacts NCBI or Slack, and nothing needs the kestra package.
The mock reproduces the response layout observed live (ESearch/ESummary JSON, errorlist for unknown
genes, no errorlist for a valid empty window). It does NOT prove how real ClinVar behaves; see the
Stage 0 / 0b probe notes for what was verified against the live API.

Run: py -I tests\\test_monitor.py    (needs PyYAML)
The flow file under test is given by the CLINVAR_FLOW environment variable (required).
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from datetime import date
from types import SimpleNamespace
from unittest import mock as unittest_mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

if not os.environ.get("CLINVAR_FLOW"):
    sys.exit("Set CLINVAR_FLOW to the path of the flow file under test.")
FLOW = Path(os.environ["CLINVAR_FLOW"])
D0 = "2026-10-01"   # baseline day used by most tests
SECRET = "SECRETKEY123"


# ----------------------------------------------------------------------------- mock NCBI
TERM = re.compile(r"^(?P<gene>[^\[\s]+)\[gene\](?: AND \((?P<extra>.*)\))?"
                  r"(?: AND (?P<a>\d{4}/\d\d/\d\d):(?P<b>\d{4}/\d\d/\d\d)\[mdat\])?$")


def to_date(text):
    return date.fromisoformat(text.replace("/", "-"))


class MockNCBI:
    def __init__(self):
        self.db = {}
        self.known_genes = set()
        self.faults = []
        self.log = []
        self.hide = set()
        self.drop_in_summary = set()
        self.error_records = set()
        self.page_overlap = False
        handler = type("H", (BaseHTTPRequestHandler,), {"log_message": lambda *a: None,
                                                        "do_GET": lambda h: self._get(h)})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.server.daemon_threads = True
        self.url = "http://127.0.0.1:%d/" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    # ---- fixtures
    def add(self, uid, gene="BRCA1", cls="Pathogenic", review="criteria provided, single submitter",
            mdat="2026-09-01", title=None):
        self.known_genes.add(gene)
        self.db[str(uid)] = {"gene": gene, "cls": cls, "review": review, "mdat": date.fromisoformat(mdat),
                             "title": title or "variant %s" % uid}

    def change(self, uid, cls=None, mdat=None, review=None):
        rec = self.db[str(uid)]
        if cls is not None:
            rec["cls"] = cls
        if review is not None:
            rec["review"] = review
        if mdat is not None:
            rec["mdat"] = date.fromisoformat(mdat)

    def fault(self, path_part, kind, times=1):
        self.faults.append([path_part, kind, times])

    def requests(self, path_part):
        return [r for r in self.log if path_part in r["path"]]

    # ---- server side
    def _next_fault(self, path):
        for item in self.faults:
            if item[0] in path and item[2] > 0:
                item[2] -= 1
                return item[1]
        return None

    def _get(self, handler):
        parsed = urllib.parse.urlparse(handler.path)
        params = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        self.log.append({"path": parsed.path, "params": params, "raw": parsed.query, "t": time.monotonic()})

        def send(status, body, headers=()):
            try:
                handler.send_response(status)
                for k, v in headers:
                    handler.send_header(k, v)
                handler.send_header("Content-Length", str(len(body)))
                handler.end_headers()
                handler.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

        fault = self._next_fault(parsed.path)
        if isinstance(fault, int):
            return send(fault, b"", [("Retry-After", "0")])
        if fault == "badjson":
            return send(200, b"<html>Service unavailable</html>")
        if isinstance(fault, dict):  # {"__body__": ..., "__headers__": {...}} adds headers to an injected 200 response
            body = fault.get("__body__", fault)
            return send(200, json.dumps(body).encode(), list((fault.get("__headers__") or {}).items()))
        if fault == "sleep":
            time.sleep(2)
        body = self._esearch(params) if parsed.path.endswith("esearch.fcgi") else self._esummary(params)
        send(200, json.dumps(body).encode())

    def _esearch(self, p):
        m = TERM.match(p["term"])
        if not m:
            return {"esearchresult": {"ERROR": "mock cannot parse %r" % p["term"]}}
        gene = m.group("gene")
        if gene not in self.known_genes:
            return {"header": {"type": "esearch"}, "esearchresult": {
                "count": "0", "retmax": "0", "retstart": "0", "idlist": [], "translationset": [],
                "errorlist": {"phrasesnotfound": [p["term"]], "fieldsnotfound": []},
                "warninglist": {"phrasesignored": [], "quotedphrasesnotfound": [], "outputmessages": ["No items found."]}}}
        lo = to_date(m.group("a")) if m.group("a") else None
        hi = to_date(m.group("b")) if m.group("b") else None
        hits = sorted((u for u, r in self.db.items() if r["gene"] == gene and (lo is None or lo <= r["mdat"] <= hi)),
                      key=int, reverse=True)
        visible = [u for u in hits if u not in self.hide]
        start, size = int(p.get("retstart", 0)), int(p.get("retmax", 20))
        if self.page_overlap and start > 0:
            start -= 1
        page = visible[start:start + size]
        return {"header": {"type": "esearch", "version": "0.3"}, "esearchresult": {
            "count": str(len(hits)), "retmax": str(len(page)), "retstart": p.get("retstart", "0"),
            "idlist": page, "translationset": [], "querytranslation": p["term"]}}

    def _esummary(self, p):
        result = {"uids": []}
        for uid in p["id"].split(","):
            if uid in self.drop_in_summary or uid not in self.db:
                continue
            result["uids"].append(uid)
            r = self.db[uid]
            if uid in self.error_records:
                result[uid] = {"uid": uid, "error": "cannot get document summary"}
                continue
            result[uid] = {
                "uid": uid, "accession": "VCV%09d" % int(uid), "accession_version": "VCV%09d.1" % int(uid),
                "title": r["title"], "record_status": "",
                "germline_classification": {"description": r["cls"], "last_evaluated": "2025/10/18 00:00",
                                            "review_status": r["review"], "fda_recognized_database": "", "trait_set": []},
                "genes": [{"symbol": r["gene"], "geneid": "1", "strand": "-", "source": "submitted"}]}
        return {"header": {"type": "esummary", "version": "0.3"}, "result": result}


# ----------------------------------------------------------------------------- script runner
def load_flow():
    return yaml.safe_load(FLOW.read_text(encoding="utf-8"))


def compare_task(flow):
    loop = next(t for t in flow["tasks"] if t["id"] == "watch_genes")
    return next(t for t in loop["tasks"] if t["id"] == "compare_snapshots")


SCRIPT = compare_task(load_flow())["script"]


class Result:
    def __init__(self, proc, work):
        self.code, self.stdout, self.stderr, self.work = proc.returncode, proc.stdout, proc.stderr, work
        self.out = {}
        for ln in proc.stdout.splitlines():
            if ln.startswith("::{") and ln.endswith("}::"):
                self.out = json.loads(ln[2:-2])["outputs"]
        path = Path(work, "snapshot_out.json")
        self.snap_text = path.read_text() if path.exists() else None
        self.snap = json.loads(self.snap_text) if self.snap_text else None
        report = Path(work, "report.json")
        self.report = json.loads(report.read_text()) if report.exists() else None

    @property
    def variants(self):
        return decode(self.snap)


def decode(snap):
    return {uid: (snap["classes"][c] if c >= 0 else None, snap["reviews"][r] if r >= 0 else None)
            for uid, (c, r) in snap["variants"].items()}


WORKDIRS = []  # every run() directory; MockCase.tearDown deletes them so the suite leaves nothing behind

# The embedded script receives ONLY these inherited variables (interpreter, library-path, locale and temp-dir plumbing; none is a
# credential) plus the explicit test configuration set in script_environment(). Nothing else from the CI runner or from a
# developer's shell (tokens, webhooks, proxies, production settings) is passed on.
INHERITED_ENV = ("PATH", "SYSTEMROOT", "WINDIR", "LD_LIBRARY_PATH", "LANG", "LC_ALL", "LC_CTYPE", "TEMP", "TMP", "TMPDIR")
TEST_ENV = ("EUTILS_BASE_URL", "EUTILS_MIN_INTERVAL", "EUTILS_BACKOFF", "CLINVAR_TODAY", "NCBI_API_KEY")


def script_environment(mock, today, extra=None):
    extra = dict(extra or {})
    if "EUTILS_BASE_URL" in extra:
        raise AssertionError("a test must never redirect the script away from the local mock")
    environ = {name: os.environ[name] for name in INHERITED_ENV if name in os.environ}
    environ.update({"EUTILS_BASE_URL": mock.url, "EUTILS_MIN_INTERVAL": "0", "EUTILS_BACKOFF": "0",
                    "CLINVAR_TODAY": today, "NCBI_API_KEY": ""})
    environ.update(extra)
    if not environ["EUTILS_BASE_URL"].startswith("http://127.0.0.1:"):
        raise AssertionError("the script must only ever talk to the loopback mock")
    return environ


def run(mock, snapshot="", today="2026-10-16", gene="BRCA1", env=None, **cfg):
    work = tempfile.mkdtemp(dir=os.environ.get("CLINVAR_TEST_TMP") or None)
    WORKDIRS.append(work)
    config = {"gene": gene, "classifications": [], "alert_on_new_variants": True, "extra_query": "",
              "contact_email": "", "fail_if_snapshot_missing": False, "overlap_days": 7, "reconcile_days": 28,
              "max_lines": 15}
    config.update(cfg)
    if not isinstance(snapshot, str):
        snapshot = json.dumps(snapshot)
    Path(work, "config.json").write_text(json.dumps(config))
    Path(work, "snapshot.json").write_text(snapshot)
    Path(work, "script.py").write_text(SCRIPT)
    environ = script_environment(mock, today, env)
    proc = subprocess.run([sys.executable, "-I", "script.py"], cwd=work, capture_output=True, text=True,
                          timeout=180, env=environ)
    return Result(proc, work)


def baseline(mock, today=D0, **kw):
    res = run(mock, "", today=today, **kw)
    assert res.code == 0, res.stderr
    return res.snap


class MockCase(unittest.TestCase):
    def setUp(self):
        self.mock = MockNCBI()
        self.addCleanup(self.mock.close)
        self.addCleanup(self.remove_workdirs)

    @staticmethod
    def remove_workdirs():
        while WORKDIRS:
            shutil.rmtree(WORKDIRS.pop(), ignore_errors=True)

    def seed(self, n=3, **kw):
        for uid in range(1, n + 1):
            self.mock.add(uid, **kw)

    def assert_failed_cleanly(self, res, needle):
        self.assertNotEqual(res.code, 0, res.stdout)
        self.assertIn(needle, res.stderr)
        self.assertIsNone(res.snap_text, "snapshot must not be written on failure")
        self.assertNotIn(SECRET, res.stdout + res.stderr)


# ----------------------------------------------------------------------------- baseline
class BaselineTests(MockCase):
    def test_full_scan_stores_a_baseline_without_alerting(self):
        self.mock.add(1)
        self.mock.add(2, cls="Uncertain significance")
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual((res.out["mode"], res.out["phase"], res.out["alertable"]), ("FULL", "BASELINE", False))
        self.assertEqual((res.out["new_count"], res.out["reclassified_count"]), (0, 0))
        self.assertEqual(res.snap["schema"], 2)
        self.assertEqual((res.snap["gene"], res.snap["checkpoint"], res.snap["reconciled"]), ("BRCA1", D0, D0))
        self.assertEqual(res.variants["2"][0], "Uncertain significance")
        self.assertIn("Baseline stored for BRCA1", res.stdout)

    def test_baseline_pages_through_every_result(self):
        for uid in range(1, 1001):
            self.mock.add(uid)
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        starts = [int(r["params"]["retstart"]) for r in self.mock.requests("esearch")]
        self.assertEqual(starts, [0, 400, 800])
        sizes = [len(r["params"]["id"].split(",")) for r in self.mock.requests("esummary")]
        self.assertEqual(sizes, [200] * 5)
        self.assertEqual(len(res.snap["variants"]), 1000)
        self.assertEqual(res.out["scanned"], 1000)

    def test_large_gene_snapshot_stays_compact(self):
        # BRCA1 had 16,104 ClinVar records when probed on 2026-10-16.
        classes = ["Pathogenic", "Likely pathogenic", "Uncertain significance", "Likely benign", "Benign"]
        for uid in range(1, 16105):
            self.mock.add(uid, cls=classes[uid % 5], review="criteria provided, single submitter")
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esearch")), 41)
        self.assertEqual(len(self.mock.requests("esummary")), 81)
        size = len(res.snap_text.encode())
        print("\n  16,104-record snapshot: %d bytes" % size)
        self.assertLess(size, 600000)

    def test_snapshot_over_the_size_limit_fails_instead_of_truncating(self):
        self.seed(50)
        res = run(self.mock, today=D0, env={"SNAPSHOT_MAX_BYTES": "200"})
        self.assert_failed_cleanly(res, "safety limit")

    def test_empty_baseline_for_a_known_gene_is_stored_with_a_note(self):
        self.mock.known_genes.add("BRCA1")
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(res.snap["variants"], {})
        self.assertIn("returned no records", res.stdout)

    def test_invalid_gene_fails_with_phrasesnotfound(self):
        self.seed(1)
        res = run(self.mock, today=D0, gene="ZZZNOTAGENE9")
        self.assert_failed_cleanly(res, "does not recognise part of the query")
        self.assertIn("ZZZNOTAGENE9", res.stderr)

    def test_valid_query_with_no_results_is_not_an_error(self):
        self.seed(2)
        snap = baseline(self.mock)
        res = run(self.mock, snap, today="2026-10-09")  # nothing modified in the window
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual((res.out["phase"], res.out["scanned"], res.out["total_matches"]), ("NO CHANGES", 0, 0))
        self.assertEqual(res.snap["checkpoint"], "2026-10-09")

    def test_partial_pagination_failure_stores_nothing(self):
        for uid in range(1, 901):
            self.mock.add(uid)
        pages = {"n": 0}

        def fail_from_second_page(path):
            if "esearch" in path:
                pages["n"] += 1
                return 500 if pages["n"] >= 2 else None
            return None
        self.mock._next_fault = fail_from_second_page  # page 1 succeeds, page 2 never does
        res = run(self.mock, today=D0, env={"EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(res, "after 2 attempts: HTTP 500")
        self.assertEqual(len(self.mock.requests("esummary")), 0, "nothing is summarised from a partial result")

    def test_missing_esummary_records_fail_the_run(self):
        self.seed(3)
        self.mock.drop_in_summary.add("2")
        res = run(self.mock, today=D0)
        self.assert_failed_cleanly(res, "did not return 1 of 3 requested records")

    def test_duplicate_ids_across_pages_are_deduplicated(self):
        for uid in range(1, 501):
            self.mock.add(uid)
        self.mock.page_overlap = True
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(res.snap["variants"]), 500)
        requested = ",".join(r["params"]["id"] for r in self.mock.requests("esummary")).split(",")
        self.assertEqual(len(requested), len(set(requested)), "an id was summarised twice")

    def test_search_that_silently_loses_ids_is_detected(self):
        self.seed(5)
        self.mock.hide.add("3")  # count says 5, the pages list 4
        res = run(self.mock, today=D0)
        self.assert_failed_cleanly(res, "paging was incomplete")

    def test_unsummarisable_record_fails_the_baseline(self):
        self.seed(2)
        self.mock.error_records.add("2")
        res = run(self.mock, today=D0)
        self.assert_failed_cleanly(res, "could not summarise 1 record(s) (first: 2)")


# ----------------------------------------------------------------------------- incremental
class IncrementalTests(MockCase):
    def setUp(self):
        super().setUp()
        self.mock.add(1, cls="Pathogenic", mdat="2026-09-01")
        self.mock.add(2, cls="Uncertain significance", mdat="2026-09-01")
        self.mock.add(3, cls="Benign", mdat="2026-09-01")
        self.snap = baseline(self.mock, D0)
        self.mock.log.clear()  # tests below assert only on the incremental run's requests

    def term(self):
        return self.mock.requests("esearch")[-1]["params"]["term"]

    def test_old_variant_whose_classification_changes_is_reported(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(res.out["mode"], "INCREMENTAL")
        self.assertEqual(self.term(), "BRCA1[gene] AND 2026/09/24:2026/10/10[mdat]")
        self.assertEqual([r["params"]["id"] for r in self.mock.requests("esummary")], ["2"])
        self.assertEqual((res.out["reclassified_count"], res.out["new_count"], res.out["scanned"]), (1, 0, 1))
        self.assertIn("Uncertain significance -&gt; Likely pathogenic", res.out["slack_text"])
        self.assertIn("https://www.ncbi.nlm.nih.gov/clinvar/variation/2/", res.out["slack_text"])
        self.assertEqual(res.variants["2"][0], "Likely pathogenic")
        self.assertEqual(res.variants["1"][0], "Pathogenic")  # untouched entries survive
        self.assertEqual((res.snap["checkpoint"], res.snap["reconciled"]), ("2026-10-09", D0))

    def test_new_variant_is_reported_separately(self):
        self.mock.add(4, cls="Likely pathogenic", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((res.out["new_count"], res.out["reclassified_count"]), (1, 0))
        self.assertIn("*New variants*", res.out["slack_text"])
        self.assertNotIn("*Classification changes*", res.out["slack_text"])
        self.assertIn("4", res.variants)

    def test_unchanged_variant_in_the_window_is_not_reported(self):
        self.mock.change(1, mdat="2026-10-05")  # touched by NCBI, classification identical
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((res.out["phase"], res.out["scanned"], res.out["slack_text"]), ("NO CHANGES", 1, ""))

    def test_window_boundaries_follow_checkpoint_minus_overlap(self):
        for uid, day in ((10, "2026-09-24"), (11, "2026-09-23"), (12, "2026-10-09"), (13, "2026-10-10")):
            self.mock.add(uid, cls="Benign", mdat="2026-09-01")
        snap = baseline(self.mock, D0)
        for uid, day in ((10, "2026-09-24"), (11, "2026-09-23"), (12, "2026-10-09"), (13, "2026-10-10")):
            self.mock.change(uid, cls="Pathogenic", mdat=day)
        res = run(self.mock, snap, today="2026-10-09")
        reported = {r["variation_id"] for r in res.report["reclassified"]}
        # inclusive start (checkpoint - 7 days), end = today + 1; one day before the start is outside by design
        self.assertEqual(reported, {"10", "12", "13"})

    def test_checkpoint_advances_only_in_the_output_of_a_successful_run(self):
        self.mock.fault("esearch", 500, times=99)
        res = run(self.mock, self.snap, today="2026-10-09", env={"EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(res, "HTTP 500")
        self.assertEqual(json.loads(Path(res.work, "snapshot.json").read_text())["checkpoint"], D0)

    def test_failed_run_then_retry_does_not_lose_the_change(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.fault("esummary", 503, times=99)
        failed = run(self.mock, self.snap, today="2026-10-09", env={"EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(failed, "HTTP 503")
        self.mock.faults.clear()
        again = run(self.mock, self.snap, today="2026-10-10")  # same stored snapshot, later day, wider window
        self.assertEqual(again.code, 0, again.stderr)
        self.assertEqual(again.out["reclassified_count"], 1)

    def test_overlapping_windows_report_a_change_once(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        first = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual(first.out["reclassified_count"], 1)
        second = run(self.mock, first.snap, today="2026-10-12")  # window 10-02..10-13 still includes record 2
        self.assertEqual(second.code, 0, second.stderr)
        self.assertEqual(second.out["scanned"], 1)
        self.assertEqual((second.out["reclassified_count"], second.out["phase"]), (0, "NO CHANGES"))

    def test_repeating_the_same_window_gives_the_same_result(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        a = run(self.mock, self.snap, today="2026-10-09")
        b = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual(a.out["slack_text"].split("\n", 1)[1], b.out["slack_text"].split("\n", 1)[1])
        self.assertEqual(json.loads(a.snap_text)["variants"], json.loads(b.snap_text)["variants"])

    def test_missing_classification_is_not_a_change_and_keeps_the_old_value(self):
        self.mock.change(1, cls="", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((res.out["reclassified_count"], res.out["missing_count"], res.out["alertable"]), (0, 1, False))
        self.assertEqual(res.variants["1"][0], "Pathogenic")
        self.assertIn("WARNING 1 has no classification", res.stdout)

    def test_first_classification_is_reported_as_newly_classified(self):
        self.mock.add(5, cls="", mdat="2026-09-01")
        snap = baseline(self.mock, D0)
        self.mock.change(5, cls="Likely benign", mdat="2026-10-08")
        res = run(self.mock, snap, today="2026-10-09")
        self.assertEqual((res.out["newly_classified_count"], res.out["reclassified_count"]), (1, 0))
        self.assertIn("*Newly classified*", res.out["slack_text"])

    def test_conflicting_classification_is_flagged(self):
        self.mock.change(1, cls="Conflicting classifications of pathogenicity", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertIn("conflicting submissions", res.out["slack_text"])

    def test_review_status_only_change_is_counted_but_not_alerted(self):
        self.mock.change(1, review="reviewed by expert panel", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((res.out["review_only_count"], res.out["alertable"]), (1, False))

    def test_classification_filter_limits_what_is_reported(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.change(3, cls="Likely benign", mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09", classifications=["Likely pathogenic"])
        self.assertEqual(res.out["reclassified_count"], 1)
        self.assertNotIn("Benign", res.out["slack_text"])

    def test_new_variants_can_be_muted(self):
        self.mock.add(4, mdat="2026-10-08")
        res = run(self.mock, self.snap, today="2026-10-09", alert_on_new_variants=False)
        self.assertEqual((res.out["new_count"], res.out["alertable"], res.out["slack_text"]), (1, False, ""))

    def test_extra_query_is_anded_into_the_term(self):
        snap = baseline(self.mock, D0, extra_query="expert panel[Review status]")
        run(self.mock, snap, today="2026-10-09", extra_query="expert panel[Review status]")
        self.assertEqual(self.term(), "BRCA1[gene] AND (expert panel[Review status]) AND 2026/09/24:2026/10/10[mdat]")

    def test_missing_checkpoint_stops_the_run(self):
        broken = dict(self.snap)
        del broken["checkpoint"]
        self.assert_failed_cleanly(run(self.mock, broken, today="2026-10-09"), "checkpoint is not a date")

    def test_checkpoint_in_the_future_stops_the_run(self):
        broken = dict(self.snap, checkpoint="2027-01-01")
        self.assert_failed_cleanly(run(self.mock, broken, today="2026-10-09"), "checkpoint is in the future")


# ----------------------------------------------------------------------------- reconciliation and snapshots
class ReconciliationTests(MockCase):
    def setUp(self):
        super().setUp()
        for uid in (1, 2, 3):
            self.mock.add(uid, cls="Uncertain significance", mdat="2026-09-01")
        self.snap = baseline(self.mock, D0)

    def test_full_scan_is_due_after_reconcile_days_and_not_before(self):
        self.assertEqual(run(self.mock, self.snap, today="2026-10-28").out["mode"], "INCREMENTAL")  # day 27
        due = run(self.mock, self.snap, today="2026-10-29")                                           # day 28
        self.assertEqual(due.out["mode"], "FULL")
        self.assertIn("full scan due (28 days", due.stdout)

    def test_reconciliation_finds_a_change_the_date_filter_missed(self):
        self.mock.change(2, cls="Likely pathogenic")  # classification changed but mdat NOT updated: filter blind spot
        weekly = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((weekly.out["mode"], weekly.out["reclassified_count"]), ("INCREMENTAL", 0))
        full = run(self.mock, weekly.snap, today="2026-11-05")
        self.assertEqual((full.out["mode"], full.out["reclassified_count"], full.out["changes_outside_date_window"]), ("FULL", 1, 1))
        self.assertEqual(full.out["date_window_comparison"], "COMPARED")
        self.assertIn("RECONCILIATION: 1 of 1 change(s)", full.stdout)
        self.assertEqual(full.out["scanned"], 3)

    def test_successful_reconciliation_replaces_the_snapshot_and_dates(self):
        self.mock.change(3, cls="Benign")
        full = run(self.mock, self.snap, today="2026-11-05")
        self.assertEqual((full.snap["checkpoint"], full.snap["reconciled"]), ("2026-11-05", "2026-11-05"))
        self.assertEqual(full.variants["3"][0], "Benign")

    def test_failed_reconciliation_keeps_the_old_snapshot(self):
        self.mock.fault("esummary", 500, times=99)
        res = run(self.mock, self.snap, today="2026-11-05", env={"EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(res, "HTTP 500")
        self.assertEqual(json.loads(Path(res.work, "snapshot.json").read_text()), self.snap)

    def test_full_scan_returning_nothing_for_a_populated_gene_fails(self):
        self.mock.db.clear()
        self.mock.known_genes.add("BRCA1")
        res = run(self.mock, self.snap, today="2026-11-05")
        self.assert_failed_cleanly(res, "Refusing to replace it")

    def test_variants_missing_from_a_full_scan_are_kept_and_noted(self):
        del self.mock.db["3"]
        res = run(self.mock, self.snap, today="2026-11-05")
        self.assertEqual(res.code, 0, res.stderr)
        self.assertIn("3", res.snap["variants"])
        self.assertIn("1 snapshot records were not returned", res.stdout)

    def test_schema_1_snapshot_is_migrated_and_compared(self):
        v1 = {"schema": 1, "gene": "BRCA1", "query": "", "updated": "2026-10-01T00:00:00Z", "variants": {
            "BRCA1:1": {"variation_id": "1", "classification": "Uncertain significance", "review_status": "x"},
            "BRCA1:2": {"variation_id": "2", "classification": "Benign", "review_status": "x"}}}
        self.mock.change(2, cls="Likely pathogenic")
        res = run(self.mock, v1, today="2026-10-09")
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual((res.out["mode"], res.out["baseline"], res.out["reclassified_count"], res.out["new_count"]),
                         ("FULL", False, 1, 1))
        self.assertIn("migrating snapshot schema 1", res.stdout)
        self.assertEqual(res.snap["schema"], 2)

    def test_unsupported_schema_stops_the_run(self):
        self.assert_failed_cleanly(run(self.mock, dict(self.snap, schema=3)), "unsupported schema")

    def test_corrupted_snapshots_stop_the_run(self):
        self.assert_failed_cleanly(run(self.mock, "{not json"), "not valid JSON")
        bad_index = dict(self.snap, variants={"1": [9, 0]})
        self.assert_failed_cleanly(run(self.mock, bad_index), "corrupted")
        self.assert_failed_cleanly(run(self.mock, dict(self.snap, variants=[])), "corrupted")

    def test_snapshot_for_another_gene_or_query_stops_the_run(self):
        self.assert_failed_cleanly(run(self.mock, dict(self.snap, gene="TP53")), "belongs to another gene")
        self.assert_failed_cleanly(run(self.mock, self.snap, extra_query="x[y]"), "different extra query")

    def test_missing_snapshot_stops_when_required(self):
        res = run(self.mock, "", fail_if_snapshot_missing=True)
        self.assert_failed_cleanly(res, "fail_if_snapshot_missing is on")


class ReconciliationMetricTests(MockCase):
    """changes_outside_date_window: of the changes a scheduled full scan finds, how many an incremental query would NOT have returned."""

    def setUp(self):
        super().setUp()
        for uid in (1, 2, 3):
            self.mock.add(uid, cls="Uncertain significance", mdat="2026-09-01")
        snap = baseline(self.mock, D0)
        for day in ("2026-10-08", "2026-10-15", "2026-10-22"):  # three ordinary weekly incremental runs
            res = run(self.mock, snap, today=day)
            assert res.code == 0, res.stderr
            snap = res.snap
        self.snap = snap  # checkpoint 2026-10-22, last full scan 2026-10-01
        self.due = "2026-10-29"  # 28 days after the last full scan: the full scan replaces that week's incremental run
        self.mock.log.clear()

    def mdat_terms(self):
        return [r["params"]["term"] for r in self.mock.requests("esearch") if "[mdat]" in r["params"]["term"]]

    def test_a_change_the_date_filter_would_have_returned_is_not_counted(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-27")
        res = run(self.mock, self.snap, today=self.due)
        self.assertEqual((res.out["mode"], res.out["reclassified_count"]), ("FULL", 1))
        self.assertEqual((res.out["date_window_comparison"], res.out["changes_outside_date_window"]), ("COMPARED", 0))
        self.assertIn("RECONCILIATION: 0 of 1 change(s)", res.stdout)
        # the same data as an incremental run finds the same change, which is why it must not count as a miss
        inc = run(self.mock, self.snap, today=self.due, reconcile_days=365)
        self.assertEqual((inc.out["mode"], inc.out["reclassified_count"]), ("INCREMENTAL", 1))

    def test_a_change_outside_the_window_is_counted(self):
        self.mock.change(2, cls="Likely pathogenic")  # classification changed, modification date not updated
        res = run(self.mock, self.snap, today=self.due)
        self.assertEqual((res.out["reclassified_count"], res.out["changes_outside_date_window"]), (1, 1))
        self.assertIn("RECONCILIATION: 1 of 1 change(s)", res.stdout)
        inc = run(self.mock, self.snap, today=self.due, reconcile_days=365)
        self.assertEqual(inc.out["reclassified_count"], 0)  # an incremental run would have missed it

    def test_only_the_outside_changes_are_counted_in_a_mixed_run(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-27")  # inside
        self.mock.change(3, cls="Benign")                                # outside
        self.mock.add(4, cls="Pathogenic", mdat="2026-10-27")            # new variant, inside
        self.mock.add(5, cls="Pathogenic", mdat="2026-09-02")            # new variant, outside
        res = run(self.mock, self.snap, today=self.due)
        self.assertEqual((res.out["reclassified_count"], res.out["new_count"]), (2, 2))
        self.assertEqual(res.out["changes_outside_date_window"], 2)
        self.assertIn("RECONCILIATION: 2 of 4 change(s)", res.stdout)

    def test_the_comparison_uses_the_incremental_window_and_only_asks_for_ids(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-27")
        res = run(self.mock, self.snap, today=self.due)
        self.assertEqual(self.mdat_terms(), ["BRCA1[gene] AND 2026/10/15:2026/10/30[mdat]"])  # checkpoint 10-22 minus 7 days, to tomorrow
        summaries = ",".join(r["params"]["id"] for r in self.mock.requests("esummary")).split(",")
        self.assertEqual(sorted(summaries), ["1", "2", "3"], "the window query must not trigger extra ESummary calls")
        self.assertEqual(res.code, 0, res.stderr)

    def test_changes_out_of_scope_are_not_counted(self):
        self.mock.change(2, cls="Likely pathogenic")
        res = run(self.mock, self.snap, today=self.due, classifications=["Pathogenic"])
        self.assertEqual((res.out["reclassified_count"], res.out["changes_outside_date_window"]), (0, 0))

    def test_initial_baseline_has_no_comparison(self):
        mock = MockNCBI()
        self.addCleanup(mock.close)
        mock.add(1)
        res = run(mock, today=D0)
        self.assertEqual((res.out["date_window_comparison"], res.out["changes_outside_date_window"]), ("NOT APPLICABLE", None))
        self.assertNotIn("RECONCILIATION", res.stdout)
        self.assertEqual([r for r in mock.requests("esearch") if "[mdat]" in r["params"]["term"]], [])

    def test_schema_migration_has_no_comparison(self):
        v1 = {"schema": 1, "gene": "BRCA1", "query": "", "variants": {
            "BRCA1:1": {"variation_id": "1", "classification": "Benign", "review_status": "x"}}}
        res = run(self.mock, v1, today=self.due)
        self.assertEqual((res.out["mode"], res.out["date_window_comparison"], res.out["changes_outside_date_window"]),
                         ("FULL", "NOT APPLICABLE", None))
        self.assertEqual(self.mdat_terms(), [])

    def test_incremental_runs_have_no_comparison(self):
        res = run(self.mock, self.snap, today="2026-10-23")
        self.assertEqual((res.out["mode"], res.out["date_window_comparison"], res.out["changes_outside_date_window"]),
                         ("INCREMENTAL", "NOT APPLICABLE", None))

    def test_a_failed_comparison_is_reported_as_unavailable_not_as_a_number(self):
        self.mock.change(2, cls="Likely pathogenic")

        def fail_window_query(path):
            if "esearch" in path and "[mdat]" in self.mock.log[-1]["params"].get("term", ""):
                return 500
            return None
        self.mock._next_fault = fail_window_query
        res = run(self.mock, self.snap, today=self.due, env={"EUTILS_ATTEMPTS": "2"})
        self.assertEqual(res.code, 0, res.stderr)  # the diagnostic must not fail an otherwise good full scan
        self.assertEqual((res.out["date_window_comparison"], res.out["changes_outside_date_window"]), ("UNAVAILABLE", None))
        self.assertIn("WARNING the date-window comparison failed", res.stdout)
        self.assertNotIn("RECONCILIATION:", res.stdout)
        self.assertEqual(res.out["reclassified_count"], 1)
        self.assertEqual(res.snap["reconciled"], self.due)
        self.assertEqual(res.report["changes_outside_date_window"], None)


class RecordFailureTests(MockCase):
    """A record ESummary cannot summarise fails the run (option A): the snapshot and checkpoint must not move."""

    def setUp(self):
        super().setUp()
        for uid in (1, 2, 3):
            self.mock.add(uid, cls="Uncertain significance", mdat="2026-09-01")
        self.snap = baseline(self.mock, D0)
        self.mock.log.clear()

    def stored(self, res):
        return json.loads(Path(res.work, "snapshot.json").read_text())

    def test_failure_keeps_the_old_snapshot_and_checkpoint(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.error_records.add("2")
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assert_failed_cleanly(res, "could not summarise 1 record(s) (first: 2)")
        self.assertIsNone(res.report)
        self.assertEqual(self.stored(res), self.snap)
        self.assertEqual(self.stored(res)["checkpoint"], D0)

    def test_a_later_successful_retry_detects_the_change(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.error_records.add("2")
        self.assertNotEqual(run(self.mock, self.snap, today="2026-10-09").code, 0)
        self.mock.error_records.clear()
        retry = run(self.mock, self.snap, today="2026-10-10")  # the stored snapshot is still the old one
        self.assertEqual(retry.code, 0, retry.stderr)
        self.assertEqual(retry.out["reclassified_count"], 1)
        self.assertEqual([r["variation_id"] for r in retry.report["reclassified"]], ["2"])
        self.assertEqual(retry.snap["checkpoint"], "2026-10-10")

    def test_repeated_failures_widen_the_window_instead_of_losing_the_change(self):
        self.mock.change(2, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.error_records.add("2")
        for day in ("2026-10-09", "2026-10-16", "2026-10-23"):
            self.assertNotEqual(run(self.mock, self.snap, today=day).code, 0)
        self.mock.error_records.clear()
        # without the fix the checkpoint would have advanced to 10-16 or later and the 7-day overlap would no longer reach 10-08
        final = run(self.mock, self.snap, today="2026-10-24")
        self.assertEqual(final.out["reclassified_count"], 1)

    def test_an_unsummarisable_record_fails_a_full_scan_too(self):
        self.mock.error_records.add("3")
        res = run(self.mock, self.snap, today="2026-11-05")
        self.assert_failed_cleanly(res, "could not summarise 1 record(s) (first: 3)")
        self.assertEqual(self.stored(res), self.snap)


class SnapshotValidationTests(MockCase):
    def setUp(self):
        super().setUp()
        self.seed(2)

    def test_present_but_unusable_snapshots_never_become_a_new_baseline(self):
        for raw in ("[]", "0", "false", "null", '""', "{}", '"text"', "[1, 2]", "1"):
            with self.subTest(stored=raw):
                res = run(self.mock, raw, today=D0)
                self.assert_failed_cleanly(res, "unexpected structure")
                self.assertEqual(res.out, {})

    def test_the_same_values_fail_even_when_a_snapshot_is_required(self):
        res = run(self.mock, "[]", today=D0, fail_if_snapshot_missing=True)
        self.assert_failed_cleanly(res, "unexpected structure")
        self.assertNotIn("No stored snapshot", res.stderr)

    def test_an_empty_or_blank_value_is_a_genuinely_missing_snapshot(self):
        for raw in ("", "   \n  "):
            with self.subTest(stored=repr(raw)):
                res = run(self.mock, raw, today=D0)
                self.assertEqual(res.code, 0, res.stderr)
                self.assertTrue(res.out["baseline"])
        self.assert_failed_cleanly(run(self.mock, "", today=D0, fail_if_snapshot_missing=True), "No stored snapshot")

    def test_objects_with_the_wrong_structure_are_rejected(self):
        good = baseline(self.mock, D0)
        for label, bad in (("no variants", {"gene": "BRCA1"}),
                           ("variants not an object", dict(good, variants=[])),
                           ("missing classes table", {k: v for k, v in good.items() if k != "classes"}),
                           ("bad index", dict(good, variants={"1": [99, 0]})),
                           ("wrong gene", dict(good, gene="TP53")),
                           ("unsupported schema", dict(good, schema=3)),
                           ("no schema", {k: v for k, v in good.items() if k != "schema"})):
            with self.subTest(case=label):
                res = run(self.mock, bad, today="2026-10-09")
                self.assertNotEqual(res.code, 0, label)
                self.assertIsNone(res.snap_text)

    def test_a_valid_schema_1_snapshot_is_still_migrated(self):
        v1 = {"schema": 1, "gene": "BRCA1", "query": "", "variants": {
            "BRCA1:1": {"variation_id": "1", "classification": "Benign", "review_status": "x"}}}
        res = run(self.mock, v1, today="2026-10-09")
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual((res.snap["schema"], res.out["baseline"], res.out["reclassified_count"]), (2, False, 1))


# ----------------------------------------------------------------------------- API behaviour
class ApiTests(MockCase):
    def test_http_429_is_retried(self):
        self.seed(2)
        self.mock.fault("esearch", 429, times=1)
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esearch")), 2)
        self.assertIn("retry 1 of 3", res.stdout)

    def test_http_500_then_200_succeeds(self):
        self.seed(2)
        self.mock.fault("esummary", 500, times=1)
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esummary")), 2)

    def test_retry_exhaustion_fails_after_the_configured_attempts(self):
        self.seed(2)
        self.mock.fault("esearch", 500, times=99)
        res = run(self.mock, today=D0, env={"EUTILS_ATTEMPTS": "3"})
        self.assert_failed_cleanly(res, "failed after 3 attempts: HTTP 500")
        self.assertEqual(len(self.mock.requests("esearch")), 3)

    def test_client_errors_are_not_retried(self):
        self.seed(2)
        self.mock.fault("esearch", 400, times=99)
        res = run(self.mock, today=D0)
        self.assert_failed_cleanly(res, "failed: HTTP 400")
        self.assertEqual(len(self.mock.requests("esearch")), 1)

    # ---- NCBI may answer HTTP 200 with a rate-limit error in the JSON body
    RATE = {"error": "API rate limit exceeded", "api-key": SECRET, "count": "4", "limit": "3"}

    def test_http_200_rate_limit_body_is_retried_on_esearch_until_it_succeeds(self):
        self.seed(2)
        self.mock.fault("esearch", dict(self.RATE), times=2)
        res = run(self.mock, today=D0, env={"NCBI_API_KEY": SECRET})
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esearch")), 3)
        self.assertIn("rate limit reported in the response body", res.stdout)
        self.assertNotIn(SECRET, res.stdout + res.stderr, "the response body (which echoes the key) must never be printed")

    def test_http_200_rate_limit_body_is_retried_on_esummary(self):
        self.seed(2)
        self.mock.fault("esummary", {"error": "Too many requests, slow down"}, times=1)
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esummary")), 2)
        self.assertEqual(len(res.snap["variants"]), 2)

    def test_rate_limit_error_inside_esearchresult_is_retried(self):
        self.seed(2)
        self.mock.fault("esearch", {"esearchresult": {"ERROR": "API rate limit exceeded"}}, times=1)
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertEqual(len(self.mock.requests("esearch")), 2)

    def test_rate_limit_body_retries_are_bounded(self):
        self.seed(2)
        self.mock.fault("esearch", dict(self.RATE), times=99)
        res = run(self.mock, today=D0, env={"NCBI_API_KEY": SECRET, "EUTILS_ATTEMPTS": "3"})
        self.assert_failed_cleanly(res, "failed after 3 attempts: rate limit reported in the response body")
        self.assertEqual(len(self.mock.requests("esearch")), 3)

    def test_retry_after_on_a_200_rate_limit_body_is_honoured(self):
        self.seed(2)
        self.mock.fault("esearch", {"__body__": {"error": "API rate limit exceeded"}, "__headers__": {"Retry-After": "2"}}, times=1)
        started = time.monotonic()
        res = run(self.mock, today=D0)  # EUTILS_BACKOFF=0, so any wait comes from Retry-After
        self.assertEqual(res.code, 0, res.stderr)
        self.assertGreaterEqual(time.monotonic() - started, 1.8)

    def test_a_non_numeric_retry_after_is_ignored(self):
        self.seed(2)
        date_form = "Wed, 21 Oct 2026 07:28:00 GMT"
        self.mock.fault("esearch", {"__body__": {"error": "API rate limit exceeded"}, "__headers__": {"Retry-After": date_form}}, times=1)
        started = time.monotonic()
        res = run(self.mock, today=D0)
        self.assertEqual(res.code, 0, res.stderr)
        self.assertLess(time.monotonic() - started, 5)  # ignoring the header costs ~0 s of waiting; honouring a long one would exceed this

    def test_other_error_bodies_stay_fatal_and_are_not_retried(self):
        self.seed(2)
        self.mock.fault("esummary", {"error": "Invalid uid list"}, times=99)
        res = run(self.mock, today=D0)
        self.assert_failed_cleanly(res, "ESummary returned an error: Invalid uid list")
        self.assertEqual(len(self.mock.requests("esummary")), 1)

    def test_timeout_is_retried_then_fails(self):
        self.seed(2)
        self.mock.fault("esearch", "sleep", times=99)
        res = run(self.mock, today=D0, env={"EUTILS_TIMEOUT": "0.4", "EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(res, "network error")

    def test_malformed_json_is_retried_then_fails(self):
        self.seed(2)
        self.mock.fault("esearch", "badjson", times=99)
        self.assert_failed_cleanly(run(self.mock, today=D0, env={"EUTILS_ATTEMPTS": "2"}), "response is not valid JSON")

    def test_malformed_json_once_then_ok(self):
        self.seed(2)
        self.mock.fault("esummary", "badjson", times=1)
        self.assertEqual(run(self.mock, today=D0).code, 0)

    def test_unexpected_response_schema_fails(self):
        self.seed(2)
        self.mock.fault("esearch", {"unexpected": True}, times=99)
        self.assert_failed_cleanly(run(self.mock, today=D0), "API format may have changed")

    def test_esearch_error_field_fails(self):
        self.seed(2)
        self.mock.fault("esearch", {"esearchresult": {"ERROR": "Invalid db name specified"}}, times=99)
        self.assert_failed_cleanly(run(self.mock, today=D0), "ESearch returned an error")

    def test_ignored_phrases_in_the_query_fail(self):
        self.seed(2)
        body = {"esearchresult": {"count": "0", "idlist": [], "warninglist": {"phrasesignored": ["foo"], "quotedphrasesnotfound": []}}}
        self.mock.fault("esearch", body, times=99)
        self.assert_failed_cleanly(run(self.mock, today=D0), "ignored part of the query")

    def test_esummary_error_fails(self):
        self.seed(2)
        self.mock.fault("esummary", {"error": "Invalid uid"}, times=99)
        self.assert_failed_cleanly(run(self.mock, today=D0), "ESummary returned an error")

    def test_summary_batches_are_at_most_200_ids(self):
        self.seed(200)
        run(self.mock, today=D0)
        self.assertEqual([len(r["params"]["id"].split(",")) for r in self.mock.requests("esummary")], [200])
        self.mock.log.clear()
        self.mock.add(201)
        run(self.mock, today=D0)
        self.assertEqual([len(r["params"]["id"].split(",")) for r in self.mock.requests("esummary")], [200, 1])

    def test_email_is_url_encoded(self):
        self.seed(1)
        run(self.mock, today=D0, contact_email="a+b@lab.org")
        raw = self.mock.requests("esearch")[0]["raw"]
        self.assertIn("email=a%2Bb%40lab.org", raw)
        self.assertEqual(self.mock.requests("esearch")[0]["params"]["email"], "a+b@lab.org")

    def test_api_key_is_sent_but_never_printed(self):
        self.seed(2)
        ok = run(self.mock, today=D0, env={"NCBI_API_KEY": SECRET})
        self.assertIn("api_key=" + SECRET, self.mock.requests("esearch")[0]["raw"])
        self.assertNotIn(SECRET, ok.stdout + ok.stderr)
        self.mock.fault("esearch", 500, times=99)
        bad = run(self.mock, today=D0, env={"NCBI_API_KEY": SECRET, "EUTILS_ATTEMPTS": "2"})
        self.assert_failed_cleanly(bad, "HTTP 500")

    def test_no_api_key_parameter_when_none_is_configured(self):
        self.seed(1)
        run(self.mock, today=D0)
        self.assertNotIn("api_key", self.mock.requests("esearch")[0]["raw"])

    def test_requests_respect_the_minimum_interval(self):
        self.seed(1)
        run(self.mock, today=D0, env={"EUTILS_MIN_INTERVAL": "0.3"})
        times = [r["t"] for r in self.mock.log]
        self.assertGreaterEqual(len(times), 2)
        for a, b in zip(times, times[1:]):
            self.assertGreaterEqual(b - a, 0.2)  # 0.3 s configured; tolerance for timer granularity


# ----------------------------------------------------------------------------- notifications
class NotificationTests(MockCase):
    def setUp(self):
        super().setUp()
        self.mock.add(1, cls="Uncertain significance", mdat="2026-09-01")
        self.snap = baseline(self.mock, D0)

    def test_untrusted_text_is_escaped_for_slack(self):
        self.mock.add(2, cls="Likely <b>pathogenic</b> & more", mdat="2026-10-08",
                      title="NM_000546.6(TP53):c.743G>A <script> & (p.Arg248Gln)", review="a > b")
        res = run(self.mock, self.snap, today="2026-10-09")
        text = res.out["slack_text"]
        self.assertIn("c.743G&gt;A &lt;script&gt; &amp; (p.Arg248Gln)", text)
        self.assertIn("Likely &lt;b&gt;pathogenic&lt;/b&gt; &amp; more", text)
        self.assertIn("[a &gt; b]", text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("<b>", text)
        self.assertIn("<https://www.ncbi.nlm.nih.gov/clinvar/variation/2/|VCV000000002.1>", text)  # real link markup kept

    def test_reclassification_text_has_both_values_and_the_disclaimer(self):
        self.mock.change(1, cls="Likely pathogenic", mdat="2026-10-08")
        text = run(self.mock, self.snap, today="2026-10-09").out["slack_text"]
        self.assertIn("Uncertain significance -&gt; Likely pathogenic", text)
        self.assertIn("not clinical advice", text)
        self.assertIn("incremental scan", text)

    def test_no_changes_means_no_message(self):
        res = run(self.mock, self.snap, today="2026-10-09")
        self.assertEqual((res.out["phase"], res.out["slack_text"], res.out["alertable"]), ("NO CHANGES", "", False))

    # ---- size limit: totals, priority, an explicit omitted count, escaping preserved
    @staticmethod
    def rows(text):
        return [ln for ln in text.splitlines() if ln.startswith("- <")]

    @staticmethod
    def omitted(text):
        found = re.findall(r"^(\d+) additional change\(s\) omitted; see the execution logs\.$", text, re.M)
        return int(found[0]) if found else 0

    def long_scenario(self, reclassified=40, new=30, title="<A&>" * 100):
        for uid in range(1001, 1001 + reclassified):
            self.mock.add(uid, cls="Uncertain significance", mdat="2026-09-01", title=title)
        snap = baseline(self.mock, D0)
        for uid in range(1001, 1001 + reclassified):
            self.mock.change(uid, cls="Likely pathogenic", mdat="2026-10-08")
        for uid in range(2001, 2001 + new):
            self.mock.add(uid, cls="Pathogenic", mdat="2026-10-08", title=title)
        return run(self.mock, snap, today="2026-10-09")

    def test_a_long_message_states_the_totals_and_exactly_how_many_changes_were_omitted(self):
        res = self.long_scenario()
        text = res.out["slack_text"]
        self.assertEqual(res.code, 0, res.stderr)
        self.assertLessEqual(len(text), 3900)
        self.assertIn("Totals: 40 classification changes, 30 new variants, 0 newly classified", text)
        shown = len(self.rows(text))
        self.assertGreater(shown, 0)
        self.assertEqual(self.omitted(text), 70 - shown, "omitted count must equal reportable changes minus rows shown")
        self.assertTrue(text.splitlines()[-1].startswith("_Research monitoring only"))
        self.assertIn("additional change(s) omitted", text.splitlines()[-2])
        self.assertNotIn("message shortened", text)

    def test_reclassifications_are_listed_before_anything_else_and_are_never_displaced_by_new_variants(self):
        res = self.long_scenario(reclassified=8, new=40, title="T" * 150)
        text = res.out["slack_text"]
        lines = text.splitlines()
        self.assertIn("*New variants*", lines, "the scenario must leave room for some new variants")
        first_new = lines.index("*New variants*")
        reclass_rows = [ln for ln in lines[:first_new] if ln.startswith("- <")]
        self.assertEqual(len(reclass_rows), 8, "all 8 reclassifications must be shown before any new variant is")
        self.assertLess(lines.index("*Classification changes*"), first_new)
        self.assertGreater(self.omitted(text), 0)
        self.assertEqual(self.omitted(text), 48 - len(self.rows(text)))

    def test_escaping_survives_truncation(self):
        text = self.long_scenario().out["slack_text"]
        self.assertIn("&lt;A&amp;&gt;", text)
        self.assertNotIn("<A&>", text)
        self.assertFalse(re.search(r"&(?!amp;|lt;|gt;)", text), "a bare ampersand slipped through")
        for row in self.rows(text):
            self.assertTrue(row.startswith("- <https://www.ncbi.nlm.nih.gov/clinvar/variation/"), row[:60])

    def test_very_long_titles_are_clipped_before_they_are_escaped(self):
        self.mock.add(500, cls="Pathogenic", mdat="2026-10-08", title="X" * 500)
        text = run(self.mock, self.snap, today="2026-10-09").out["slack_text"]
        self.assertIn("X" * 160 + "...", text)
        self.assertNotIn("X" * 161, text)

    def test_the_per_section_line_cap_is_counted_as_omitted(self):
        for uid in range(100, 130):
            self.mock.add(uid, cls="Pathogenic", mdat="2026-10-08")
        text = run(self.mock, self.snap, today="2026-10-09").out["slack_text"]
        self.assertIn("Totals: 0 classification changes, 30 new variants, 0 newly classified", text)
        self.assertEqual(len(self.rows(text)), 15)
        self.assertEqual(self.omitted(text), 15)

    def test_a_message_that_fits_has_totals_and_no_omitted_line(self):
        self.mock.change(1, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.add(2, cls="Benign", mdat="2026-10-08")
        text = run(self.mock, self.snap, today="2026-10-09").out["slack_text"]
        self.assertIn("Totals: 1 classification changes, 1 new variants, 0 newly classified", text)
        self.assertEqual(self.omitted(text), 0)
        self.assertNotIn("omitted", text)

    def test_totals_leave_out_new_variants_when_they_are_muted(self):
        self.mock.change(1, cls="Likely pathogenic", mdat="2026-10-08")
        self.mock.add(2, cls="Benign", mdat="2026-10-08")
        text = run(self.mock, self.snap, today="2026-10-09", alert_on_new_variants=False).out["slack_text"]
        self.assertIn("Totals: 1 classification changes", text)
        self.assertNotIn("new variants", text)

    def test_the_execution_log_and_report_keep_every_change_the_message_omits(self):
        res = self.long_scenario(reclassified=8, new=40, title="T" * 150)
        self.assertEqual(len([ln for ln in res.stdout.splitlines() if ln.strip().startswith("reclassified ")]), 8)
        self.assertEqual(len([ln for ln in res.stdout.splitlines() if ln.strip().startswith("new variant ")]), 40)
        self.assertEqual((len(res.report["reclassified"]), len(res.report["new_variants"])), (8, 40))

    def test_baseline_never_alerts(self):
        mock = MockNCBI()
        self.addCleanup(mock.close)
        mock.add(1)
        res = run(mock, today=D0)
        self.assertEqual((res.out["alertable"], res.out["slack_text"]), (False, ""))


# ----------------------------------------------------------------------------- the script's restricted environment
class ScriptEnvironmentTests(unittest.TestCase):
    CANARIES = {"CANARY_SECRET_TOKEN": "canary-1", "SLACK_WEBHOOK_URL": "https://hooks.example.invalid/x",
                "GITHUB_TOKEN": "canary-2", "HTTPS_PROXY": "http://127.0.0.1:9", "KESTRA_SECRET_PROD": "canary-3",
                "NCBI_API_KEY": "canary-real-looking-key"}
    STUB = SimpleNamespace(url="http://127.0.0.1:45678/")

    def test_only_allowlisted_and_explicit_test_variables_are_passed_on(self):
        with unittest_mock.patch.dict(os.environ, self.CANARIES):
            env = script_environment(self.STUB, "2026-10-16")
        self.assertLessEqual(set(env), set(INHERITED_ENV) | set(TEST_ENV))
        for name in self.CANARIES:
            if name != "NCBI_API_KEY":
                self.assertNotIn(name, env)
        self.assertEqual(env["NCBI_API_KEY"], "", "a key in the parent environment must never reach the script")
        self.assertEqual(env["EUTILS_BASE_URL"], self.STUB.url)

    def test_a_real_child_process_sees_nothing_else(self):
        with unittest_mock.patch.dict(os.environ, self.CANARIES):
            env = script_environment(self.STUB, "2026-10-16", {"EUTILS_ATTEMPTS": "2"})
            out = subprocess.run([sys.executable, "-I", "-c", "import json, os; print(json.dumps(sorted(os.environ)))"],
                                 capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        seen = set(json.loads(out.stdout))
        self.assertLessEqual(seen, set(INHERITED_ENV) | set(TEST_ENV) | {"EUTILS_ATTEMPTS"})
        self.assertFalse(seen & {"CANARY_SECRET_TOKEN", "SLACK_WEBHOOK_URL", "GITHUB_TOKEN", "HTTPS_PROXY", "KESTRA_SECRET_PROD"})

    def test_the_script_can_never_be_pointed_away_from_the_loopback_mock(self):
        with self.assertRaises(AssertionError):
            script_environment(self.STUB, "2026-10-16", {"EUTILS_BASE_URL": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"})
        with self.assertRaises(AssertionError):
            script_environment(SimpleNamespace(url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"), "2026-10-16")


# ----------------------------------------------------------------------------- static checks on the flow
class StaticFlowTests(unittest.TestCase):
    flow = load_flow()
    text = FLOW.read_text(encoding="utf-8")

    def tasks(self, tasks=None):
        for t in (self.flow["tasks"] if tasks is None else tasks):
            yield t
            for key in ("tasks", "then", "else"):
                yield from self.tasks(t.get(key) or [])

    def loop_ids(self):
        return [t["id"] for t in next(t for t in self.flow["tasks"] if t["id"] == "watch_genes")["tasks"]]

    def test_id_matches_filename_and_namespace_set(self):
        self.assertEqual(self.flow["id"], FLOW.stem)
        self.assertTrue(self.flow["namespace"])

    def test_concurrency_is_limited_to_one_execution(self):
        self.assertEqual(self.flow["concurrency"], {"limit": 1})

    def test_required_extend_fields_and_repository_rules(self):
        ext = self.flow["extend"]
        for key in ("title", "metaTitle", "shortDescription", "description", "metaDescription", "tags", "subTags", "ee", "demo"):
            self.assertIn(key, ext)
        self.assertLessEqual(len(ext["metaDescription"]), 160)
        allowed = {"Data", "Infrastructure", "Business", "Cloud", "Core", "AI", "Getting Started", "System"}
        self.assertTrue(set(ext["tags"]) <= allowed)
        self.assertLessEqual(len(ext["tags"]), 3)
        self.assertNotIn("Getting Started", ext["tags"])
        self.assertIs(ext["demo"], False)
        self.assertIs(ext["ee"], False)
        data_subtags = {"Testing & Quality", "dbt Transformations", "ELT & Warehouse Loading", "ELT & Data Landing",
                        "Day 2 Operations", "Data Warehouses", "Monitoring & Alerting", "Realtime & Streaming",
                        "Spark & Big Data", "File & Data Processing Basics"}
        self.assertTrue(set(ext["subTags"]) <= data_subtags)

    def test_description_has_the_canonical_sections_and_a_sensible_length(self):
        doc = self.flow["extend"]["description"]
        headings = ["## How it works", "## What you get", "## Who it's for", "## Why orchestrate this with Kestra",
                    "## Prerequisites", "### Secrets", "## Quick start", "## How to extend", "## Links"]
        positions = [doc.index(h) for h in headings]
        self.assertEqual(positions, sorted(positions))
        self.assertGreaterEqual(len(doc), 1500)
        self.assertLessEqual(len(doc), 3000, "README asks for roughly 1,500 to 3,000 characters")

    def test_readme_minimums(self):
        types = [t["type"] for t in self.tasks()]
        runnable = [t for t in types if not t.endswith(("flow.Loop", "flow.If"))]
        self.assertGreaterEqual(len(runnable), 3)
        self.assertTrue(any(t.endswith(("flow.Loop", "flow.If")) for t in types))
        self.assertTrue(self.flow["triggers"])
        self.assertTrue(any(t.startswith(("io.kestra.plugin.scripts", "io.kestra.plugin.slack")) for t in types))

    def test_inputs(self):
        inputs = {i["id"]: i for i in self.flow["inputs"]}
        self.assertEqual(inputs["genes"]["defaults"], ["BRCA1", "BRCA2", "TP53", "EGFR"])
        self.assertEqual((inputs["overlap_days"]["defaults"], inputs["overlap_days"]["min"]), (7, 1))
        self.assertEqual((inputs["reconcile_days"]["defaults"], inputs["reconcile_days"]["min"]), (28, 1))
        self.assertEqual(inputs["overlap_days"]["type"], "INT")
        self.assertEqual(inputs["reconcile_days"]["type"], "INT")
        self.assertNotIn("max_records_per_gene", inputs, "the 200-record window was removed")
        for name, spec in inputs.items():
            self.assertTrue(spec.get("description"), name)
            if "defaults" in spec:  # Kestra 2.0.5 rejects required:false on an input with a default
                self.assertNotEqual(spec.get("required"), False, name)

    def test_contact_email_and_prefix_validators(self):
        inputs = {i["id"]: i for i in self.flow["inputs"]}
        pattern = re.compile(inputs["contact_email"]["validator"])
        for ok in ("", "me@lab.org", "a+b@lab.org"):
            self.assertTrue(pattern.match(ok), ok)
        for bad in ("a&b@x.org", "me@lab.org&api_key=x", "no at sign", "me@lab.org?x=1"):
            self.assertFalse(pattern.match(bad), bad)
        prefix = re.compile(inputs["state_key_prefix"]["validator"])
        self.assertTrue(prefix.match("clinvar_snapshot"))
        self.assertFalse(prefix.match("bad prefix/"))

    def test_gene_validation_runs_first_and_mirrors_the_expression(self):
        first = self.flow["tasks"][0]
        self.assertEqual((first["id"], first["type"]), ("validate_genes", "io.kestra.plugin.core.execution.Fail"))
        self.assertNotIn("matches", first["condition"])

        def invalid(genes):
            return re.sub(r"[A-Za-z0-9._@-]+", "g", ",".join(genes)).replace("g,", "") != "g"
        for ok in (["BRCA1"], ["HLA-A", "C1orf43"], ["TP53", "EGFR"]):
            self.assertFalse(invalid(ok), ok)
        for bad in ([""], ["BRCA1", ""], ["BRCA1 "], ["BRC A1"], ["BRCA1;DROP"], []):
            self.assertTrue(invalid(bad), bad)
        self.assertIn("[A-Za-z0-9._@-]+", first["condition"])

    def test_script_contains_no_pebble_delimiters(self):
        # Kestra renders the script property as a template, so these sequences would be interpreted.
        for token in ("{{", "{%", "{#"):
            self.assertNotIn(token, SCRIPT, token)

    def test_config_template_provides_every_key_the_script_reads(self):
        template = compare_task(self.flow)["inputFiles"]["config.json"]
        provided = set(re.findall(r'"(\w+)":', template))
        read = set(re.findall(r'cfg(?:\.get)?[\[(]"(\w+)"', SCRIPT))
        self.assertTrue(read, "no config keys found")
        self.assertEqual(read - provided, set())

    def test_secrets_are_only_read_through_secret_and_are_documented(self):
        used = set(re.findall(r"secret\('([A-Z_]+)'\)", self.text))
        self.assertEqual(used, {"NCBI_API_KEY", "SLACK_WEBHOOK_URL"})
        for name in used:
            self.assertIn("`%s`" % name, self.flow["extend"]["description"])
        self.assertNotIn("hooks.slack.com", self.text)
        self.assertNotIn("api_key=", self.text, "the key must not be spliced into a URL template")
        env = compare_task(self.flow)["env"]
        self.assertIn("secret('NCBI_API_KEY')", env["NCBI_API_KEY"])
        self.assertIn("use_ncbi_api_key", env["NCBI_API_KEY"])

    def test_no_http_tasks_remain_and_the_task_has_a_timeout(self):
        self.assertFalse([t for t in self.tasks() if "http" in t["type"]])
        task = compare_task(self.flow)
        self.assertIn("timeout", task)
        self.assertEqual(task["containerImage"], "python:3.12-slim")

    def test_snapshot_read_and_write_use_the_same_key_and_save_follows_the_alert(self):
        loop = next(t for t in self.flow["tasks"] if t["id"] == "watch_genes")["tasks"]
        read = next(t for t in loop if t["id"] == "read_snapshot")["key"]
        write = next(t for t in loop if t["id"] == "save_snapshot")["key"]
        self.assertEqual(read, write)
        self.assertIn("state_key_prefix", read)
        ids = self.loop_ids()
        self.assertEqual(len(ids), len(set(ids)))
        self.assertLess(ids.index("read_snapshot"), ids.index("compare_snapshots"))
        self.assertLess(ids.index("compare_snapshots"), ids.index("notify_changes"))
        self.assertLess(ids.index("notify_changes"), ids.index("save_snapshot"))

    def test_run_summary_does_not_join_with_a_literal_backslash_n(self):
        summary = next(t for t in self.flow["tasks"] if t["id"] == "run_summary")
        self.assertNotIn("join('\\n')", summary["message"])
        self.assertIn("{% for", summary["message"])

    def test_documentation_states_the_unverified_limits(self):
        doc = self.flow["extend"]["description"]
        self.assertIn("not yet validated", doc)
        self.assertIn("no KV size limit", doc)
        self.assertIn("overlap_days", doc)
        self.assertIn("reconcile_days", doc)

    def test_the_reconciliation_metric_is_named_and_described_accurately(self):
        self.assertNotIn("reconciliation_gap", self.text)
        self.assertNotIn("did not report", self.text)
        self.assertNotIn("what the date filter missed", self.text)
        self.assertIn("changes_outside_date_window", SCRIPT)
        self.assertIn("fall outside that date window", self.flow["extend"]["description"])
        reconcile = {i["id"]: i for i in self.flow["inputs"]}["reconcile_days"]["description"]
        self.assertIn("outside the date window an incremental run would have used", reconcile)

    def test_the_documentation_makes_no_unsupported_claims(self):
        # No Kestra version claim (none was verified), no pointer to trigger inputs the flow does not declare,
        # and no mention of test evidence from the previous design.
        for phrase in ("Kestra 2.0", "2.0.5", "tested on", "45 tests", "48 end-to-end", "trigger inputs", "end-to-end"):
            self.assertNotIn(phrase, self.text, phrase)
        self.assertNotIn("inputs", self.flow["triggers"][0], "the quick start must not rely on trigger inputs")

    def test_no_stale_references_to_skipped_records_or_the_removed_window_input(self):
        self.assertNotIn("was skipped", SCRIPT)
        self.assertNotIn("max_records_per_gene", self.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
