#!/usr/bin/env python3
"""Run the ClinVar monitor flow on a REAL Kestra server through its REST API and check what happened.

Standard library only. It talks to ONE Kestra server, which must be on loopback, with throwaway credentials taken from the
environment (KESTRA_USER, KESTRA_PASSWORD). Every printed line passes through redact(), which removes the password and its
encoded forms.

Evidence. Kestra 2.0 no longer returns task outputs from the execution endpoint, so nothing REQUIRED depends on them. Results come from:
  * the execution logs (the flow's own log lines: summary, loopOutputs line, failure reasons) of the parent AND of its loop sub-execution:
    each Loop iteration runs as a separate execution (kind LOOP, parentId = the parent), so read_snapshot, compare_snapshots, report_gene,
    notify_changes, save_snapshot and the script output live there, found by an execution search with an explicit kind filter,
  * the KV snapshot written by the flow,
  * the Docker daemon (container create events) and the pinned image's metadata, which are independent of Kestra's API,
  * the execution state, plus taskRunList states and task outputs when the server happens to return them (optional).
Only fields named in the Kestra docs or already used by the flow are read. Anything the server does not answer in the expected shape
is reported UNVERIFIED, never PASS.

Result handling (strict):
  * PASS / FAIL / UNVERIFIED / INFO per check; every UNVERIFIED also emits a GitHub Actions ::warning annotation.
  * CRITICAL checks must each end PASS. The job exits non-zero if any critical check is FAIL, UNVERIFIED or was never run:
        version (Kestra 2.0.x evidence)      docker_runner (the Docker daemon created the task containers)
        s1_success s1_slack s1_snapshot      s2_success s2_slack s2_checkpoint
        s3_failed s3_slack s3_no_snapshot_for_bad_gene s3_kv_unchanged      s4_failed s4_slack s4_kv_unchanged
    (the *_slack keys are the "no Slack task ran" evidence, the *_kv_unchanged keys the "snapshot preserved after a failure" evidence)
  * MINIMUM PASS COUNT: MIN_PASS (default below, override with --min-pass). It sits a few below the number of PASS results of a fully
    visible run, so a run that passes the critical checks but could confirm almost nothing else is still rejected.
  * Any FAIL also fails the job.

Scenarios (all with notify_slack=false and the small gene MAX; the copy sent to Kestra has the Blueprints-gallery `extend:` block removed
(Kestra rejects it) and its weekly schedule trigger disabled; the flow file itself is never written):
  S1 baseline run   S2 incremental run   S3 invalid gene fails   S4 changed extra query fails   S5 two executions at once
"""
import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

TERMINAL = {"SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED", "RETRIED"}
PREFIXES = ("/api/v1/main", "/api/v1")  # current docs use the "main" tenant; older examples have no prefix
EXPECTED_TASKS = ("validate_genes", "read_snapshot", "compare_snapshots", "report_gene", "notify_changes", "save_snapshot", "run_summary")
SLACK_TASKS = ("slack_alert", "slack_failure")
TRIGGER_MARKER = "    type: io.kestra.plugin.core.trigger.Schedule\n"
EXPECTED_SCRIPT_RUNS = 6  # compare_snapshots runs: S1, S2, S3, S4, and the two executions of S5
MIN_PASS = 30
CRITICAL = ("version", "docker_runner",
            "s1_success", "s1_slack", "s1_snapshot",
            "s2_success", "s2_slack", "s2_checkpoint",
            "s3_failed", "s3_slack", "s3_no_snapshot_for_bad_gene", "s3_kv_unchanged",
            "s4_failed", "s4_slack", "s4_kv_unchanged")
SUMMARY_RE = re.compile(
    r"(?P<gene>[\w.@-]+): (?P<phase>BASELINE|CHANGES|NO CHANGES) \((?P<mode>full|incremental)\)\. "
    r"scanned=(?P<scanned>\d+) of (?P<total>\d+) matching records, new=(?P<new>\d+), reclassified=(?P<reclassified>\d+), "
    r"newly_classified=(?P<newly>\d+), review_status_only=(?P<review>\d+), classification_missing=(?P<missing>\d+), "
    r"snapshot_size=(?P<size>\d+), requests=(?P<requests>\d+)")
SECRETS = []
LAST_REPORT = None  # the Report of the most recent main() call (read by the tests)


SENSITIVE_PATTERNS = (re.compile(r"https?://hooks\.slack\.com/\S+"), re.compile(r"(?i)\b(api[_-]?key|token|secret|password|authorization)\b(\s*[=:]\s*)\S+"),
                      re.compile(r"(?i)bearer\s+\S+"), re.compile(r"(?i)basic\s+[A-Za-z0-9+/=]{12,}"))


def redact(text):
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "***")
    for pattern in SENSITIVE_PATTERNS[:1] + SENSITIVE_PATTERNS[2:]:
        text = pattern.sub("***", text)
    return SENSITIVE_PATTERNS[1].sub(lambda m: m.group(1) + m.group(2) + "***", text)


def say(*parts):
    print(redact(" ".join(str(p) for p in parts)), flush=True)


def annotation_escape(text):
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def b64(text):
    return base64.b64encode(text.encode()).decode()


class Report:
    RANK = {"INFO": 0, "PASS": 1, "UNVERIFIED": 2, "FAIL": 3}

    def __init__(self):
        self.items = []
        self.by_key = {}

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

    def critical_problems(self):
        return [(k, self.by_key.get(k, "NOT RUN")) for k in CRITICAL if self.by_key.get(k) != "PASS"]

    def problems(self, min_pass):
        problems = []
        if self.count("FAIL"):
            problems.append("%d check(s) FAILED" % self.count("FAIL"))
        critical = self.critical_problems()
        if critical:
            problems.append("critical check(s) not established: " + ", ".join("%s=%s" % c for c in critical))
        if self.count("PASS") < min_pass:
            problems.append("only %d PASS results; the documented minimum is %d" % (self.count("PASS"), min_pass))
        return problems

    def finish(self, min_pass):
        for status, name, detail, _ in self.items:
            if status == "UNVERIFIED":
                say("::warning title=UNVERIFIED::" + annotation_escape("%s: %s" % (name, detail)))
        problems = self.problems(min_pass)
        for problem in problems:
            say("::error title=Integration run not accepted::" + annotation_escape(problem))
        return 1 if problems else 0

    def markdown(self, min_pass):
        problems = self.problems(min_pass)
        lines = ["## Kestra integration results", ""]
        lines.append("**Run accepted.**" if not problems else "**Run NOT accepted:** " + "; ".join(problems))
        lines += ["", "| Status | Check | Detail |", "|---|---|---|"]
        for status, name, detail, _ in self.items:
            lines.append("| %s | %s | %s |" % (status, name.replace("|", "/"), redact(detail).replace("|", "/").replace("\n", " ")[:300]))
        lines += ["", "PASS %d (minimum %d), FAIL %d, UNVERIFIED %d, INFO %d" % (self.count("PASS"), min_pass, self.count("FAIL"), self.count("UNVERIFIED"), self.count("INFO"))]
        return "\n".join(lines) + "\n"


class Client:
    def __init__(self, base, user, password, timeout=60):
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.prefix = None
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
            return status, json.loads(body.decode("utf-8")) if body else None, len(body)
        except ValueError:
            return status, None, len(body)


def docker(args, timeout=60):
    """Run the docker CLI (fixed argument lists, no shell). Returns stdout, or None when the command is unavailable or fails."""
    try:
        base = json.loads(os.environ.get("DRIVER_DOCKER_COMMAND", '["docker"]'))
        proc = subprocess.run(base + list(args), capture_output=True, text=True, timeout=timeout)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return proc.stdout if proc.returncode == 0 else None


def snippet(value, limit=300):
    return redact(re.sub(r"\s+", " ", value if isinstance(value, str) else json.dumps(value))[:limit])


def multipart(fields):
    boundary = "----ci" + uuid.uuid4().hex
    parts = ['--%s\r\nContent-Disposition: form-data; name="%s"\r\n\r\n%s\r\n' % (boundary, k, v) for k, v in fields.items()]
    return ("".join(parts) + "--%s--\r\n" % boundary).encode(), "multipart/form-data; boundary=%s" % boundary


def disable_trigger(flow_text):
    """The ONLY change made to the flow: its schedule trigger gets `disabled: true`."""
    text = flow_text.replace("\r\n", "\n")
    if text.count(TRIGGER_MARKER) != 1:
        raise ValueError("expected exactly one Schedule trigger line, found %d" % text.count(TRIGGER_MARKER))
    return text.replace(TRIGGER_MARKER, TRIGGER_MARKER + "    disabled: true\n")


def strip_gallery_metadata(flow_text):
    """Remove the top-level `extend:` block (Blueprints-gallery metadata that Kestra's flow model rejects). Only a column-0 `extend:` key
    and the indented or blank lines that follow it are removed; a nested key named `extend` is never touched."""
    lines = flow_text.replace("\r\n", "\n").split("\n")
    starts = [i for i, ln in enumerate(lines) if re.match(r"extend:(\s|$)", ln)]
    if len(starts) > 1:
        raise ValueError("expected at most one top-level extend: key, found %d" % len(starts))
    if not starts:
        return "\n".join(lines), 0
    start = starts[0]
    end = start + 1
    while end < len(lines) and (lines[end] == "" or lines[end][0] in " \t"):
        end += 1
    while start > 0 and lines[start - 1] == "":
        start -= 1  # drop the blank separator line(s) in front of the block
    kept = lines[:start] + lines[end:]
    return "\n".join(kept).rstrip("\n") + "\n", end - start


def prepare_deployment(flow_text):
    """The deployment copy: gallery metadata stripped, weekly trigger disabled. The file on disk is never written."""
    stripped, removed = strip_gallery_metadata(flow_text)
    return disable_trigger(stripped), removed

def flow_identity(flow_text):
    ns = re.search(r"^namespace:\s*(\S+)\s*$", flow_text, re.M)
    fid = re.search(r"^id:\s*(\S+)\s*$", flow_text, re.M)
    if not ns or not fid:
        raise ValueError("cannot read the flow id and namespace")
    return ns.group(1), fid.group(1)


def parse_time(value):
    if not value:
        return None
    text = re.sub(r"(\.\d{6})\d+", r"\1", str(value).replace("Z", "+00:00"))
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_summary(blob, gene):
    """The flow's own per-gene summary line, e.g. 'MAX: BASELINE (full). scanned=707 of 707 matching records, ... requests=6'."""
    for match in SUMMARY_RE.finditer(blob or ""):
        if match.group("gene").upper() == gene.upper():
            d = match.groupdict()
            return {k: (int(v) if v.isdigit() else v) for k, v in d.items()}
    return None


def state_of(execution):
    return ((execution or {}).get("state") or {}).get("current")


def histories(execution):
    return [(h.get("state"), parse_time(h.get("date"))) for h in ((execution or {}).get("state") or {}).get("histories") or []]


def tasks_by_id(execution):
    runs = (execution or {}).get("taskRunList")
    if not isinstance(runs, list):
        return None
    out = {}
    for run in runs:
        if isinstance(run, dict):
            out.setdefault(run.get("taskId"), []).append(run)
    return out


def task_state(execution, task_id):
    runs = (tasks_by_id(execution) or {}).get(task_id)
    return [((r.get("state") or {}).get("current")) for r in runs] if runs else []


def today_days():
    today = datetime.now(timezone.utc).date()
    return {str(today), str(today - timedelta(days=1))}


def group_logs(rows):
    """'taskId/LEVEL=count, ...' for log entries; no message text."""
    if rows is None:
        return "unavailable"
    counts = {}
    for row in rows:
        key = "%s/%s" % (row.get("taskId") or "-", row.get("level") or "-")
        counts[key] = counts.get(key, 0) + 1
    return ", ".join("%s=%d" % kv for kv in sorted(counts.items())) or "no entries"


class Evidence:
    """What one parent execution and its loop sub-execution(s) expose. `rows` is None when the parent's logs were unavailable;
    `children` is None when the sub-executions could not be listed; `problem` says why the view may be partial."""

    def __init__(self, parent, rows, children, problem):
        self.parent, self.rows, self.children, self.problem = parent, rows, children, problem

    @property
    def children_found(self):
        return self.children is not None and not self.problem and len(self.children) >= 1 and all(c["execution"] is not None for c in self.children)

    @property
    def complete(self):
        return self.rows is not None and self.children_found and all(c["rows"] is not None for c in self.children)

    def all_rows(self):
        return list(self.rows or []) + [r for c in self.children or [] for r in c["rows"] or []]

    def blob(self):
        return "\n".join(str(r.get("message", "")) for r in self.all_rows())

class Driver:
    def __init__(self, client, report, poll):
        self.client, self.report, self.poll = client, report, poll

    # ---------------------------------------------------------------- server
    def wait_for_server(self, timeout):
        deadline = time.monotonic() + timeout
        rejected = 0
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

    def check_version(self, expected_prefix, container):
        """Critical. The configs endpoint is the ONLY authoritative source. The image label is informational: the official image
        labels itself "nightly" even for release tags, so it is never a PASS and never a contradiction."""
        name = "the server is Kestra %sx" % expected_prefix
        reported, problem = None, "the configs endpoint did not answer with HTTP 200 and a JSON object"
        for path in (self.client.prefix + "/configs", "/api/v1/configs"):
            status, data, _ = self.client.json("GET", path)
            if status == 200 and isinstance(data, dict):
                value = data.get("version")
                if isinstance(value, str) and value.strip():
                    reported = value.strip()
                    break
                problem = "the configs endpoint answered without a usable string 'version' field"
        label = docker(["inspect", container, "--format", '{{index .Config.Labels "org.opencontainers.image.version"}}'])
        label = (label or "").strip()
        self.report.info("image label org.opencontainers.image.version (informational only, never a version proof)", label if label and label != "<no value>" else "absent")
        if reported is None:
            self.report.unverified(name, problem, key="version")
            return
        release = re.fullmatch(re.escape(expected_prefix) + r"\d+(?:[-+][0-9A-Za-z.-]+)?", reported.lstrip("v"))
        self.report.check(name, bool(release), "configs endpoint reports %r" % reported, key="version")
    def docker_evidence(self, since, image):
        """Critical. The Docker daemon itself records which containers were created from the flow's image during the run."""
        until = int(time.time()) + 1
        out = docker(["events", "--since", str(int(since)), "--until", str(until), "--filter", "type=container", "--filter", "event=create",
                      "--filter", "image=" + image, "--format", "{{.ID}}"], timeout=120)
        label = "the Docker daemon created task containers from %s (the flow's Docker task runner ran the script)" % image
        if out is None:
            self.report.unverified(label, "the docker CLI did not answer (cannot read the daemon's container events)", key="docker_runner")
            return
        ids = [ln for ln in out.split() if ln]
        self.report.check(label, len(ids) >= 1, "%d container create event(s) since the run started" % len(ids), key="docker_runner")
        if 1 <= len(ids) < EXPECTED_SCRIPT_RUNS:
            self.report.info("fewer container creations than script runs", "%d seen, %d script runs expected" % (len(ids), EXPECTED_SCRIPT_RUNS))

    # ---------------------------------------------------------------- flow
    def deploy(self, flow_text):
        namespace, flow_id = flow_identity(flow_text)
        status, data, _ = self.client.json("POST", self.client.prefix + "/flows/validate", flow_text.encode(), "application/x-yaml")
        if status == 200 and isinstance(data, list):
            problems = [item for item in data if isinstance(item, dict) and item.get("constraints")]
            self.report.check("Kestra validates the flow YAML", not problems, snippet(problems) if problems else "%d item(s), no constraint violation" % len(data))
        elif status == 200:
            self.report.unverified("Kestra validates the flow YAML", "unexpected validation response shape: %s" % snippet(data))
        else:
            self.report.unverified("Kestra validates the flow YAML", "validate endpoint answered HTTP %s: %s" % (status, snippet(data)))
        status, data, _ = self.client.json("POST", self.client.prefix + "/flows", flow_text.encode(), "application/x-yaml")
        created = self.report.check("the flow is created (%s.%s)" % (namespace, flow_id), status == 200, "HTTP %s %s" % (status, "" if status == 200 else snippet(data)))
        if created:
            status, data, _ = self.client.json("GET", "%s/flows/%s/%s" % (self.client.prefix, namespace, flow_id))
            self.report.check("the stored flow can be read back", status == 200 and isinstance(data, dict) and data.get("id") == flow_id, "HTTP %s" % status)
        return namespace, flow_id, created

    # ---------------------------------------------------------------- executions
    def start(self, namespace, flow_id, inputs):
        body, ctype = multipart(inputs)
        status, data, _ = self.client.json("POST", "%s/executions/%s/%s" % (self.client.prefix, namespace, flow_id), body, ctype)
        if status in (200, 201) and isinstance(data, dict) and data.get("id"):
            return data["id"]
        self.report.check("an execution can be started", False, "HTTP %s %s" % (status, snippet(data)))
        return None

    def fetch(self, execution_id):
        status, raw = self.client.raw("GET", "%s/executions/%s" % (self.client.prefix, execution_id))
        try:
            return json.loads(raw.decode()), len(raw)
        except ValueError:
            return None, len(raw)

    def wait(self, execution_id, timeout=None):
        timeout = float(os.environ.get("DRIVER_EXEC_TIMEOUT", "900")) if timeout is None else timeout
        deadline = time.monotonic() + timeout
        last, size = None, 0
        while time.monotonic() < deadline:
            last, size = self.fetch(execution_id)
            if state_of(last) in TERMINAL:
                return last, size
            time.sleep(self.poll)
        return last, size

    def run(self, namespace, flow_id, inputs):
        execution_id = self.start(namespace, flow_id, inputs)
        if not execution_id:
            return None, 0
        return self.wait(execution_id)

    def logs(self, execution_id):
        """Log entries (dicts with a string 'message'), or None when the endpoint did not answer in that shape."""
        status, data, _ = self.client.json("GET", "%s/logs/%s" % (self.client.prefix, execution_id))
        if status != 200:
            return None
        rows = data if isinstance(data, list) else (data or {}).get("results") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return None
        rows = [r for r in rows if isinstance(r, dict)]
        return rows if any(isinstance(r.get("message"), str) for r in rows) else None

    @staticmethod
    def blob(rows):
        return "\n".join(str(r.get("message", "")) for r in (rows or []))

    def kv(self, namespace, key):
        status, data, _ = self.client.json("GET", "%s/namespaces/%s/kv/%s" % (self.client.prefix, namespace, urllib.parse.quote(key, safe="")))
        if status == 404:
            return "missing", None
        if status != 200 or not isinstance(data, dict) or data.get("value") is None:
            return "http %s" % status, None
        value = data["value"]
        return "ok", value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), sort_keys=True)

    # ---------------------------------------------------------------- loop sub-executions
    def children(self, parent_id):
        """Loop iterations run as separate sub-executions: kind LOOP, parentId = the parent execution (Kestra v2.0.6 source,
        Execution.loopExecution). The default execution search lists NORMAL executions only, so the kind filter is explicit.
        Returns (rows, problem); rows is None when the search did not answer in the expected shape."""
        query = urllib.parse.urlencode({"filters[kind][EQUALS]": "LOOP", "filters[parentId][EQUALS]": parent_id, "size": "100"})
        status, data, _ = self.client.json("GET", "%s/executions/search?%s" % (self.client.prefix, query))
        results = data.get("results") if isinstance(data, dict) else data if isinstance(data, list) else None
        if status != 200 or not isinstance(results, list):
            return None, "the execution search answered HTTP %s without a results list" % status
        rows = [r for r in results if isinstance(r, dict) and r.get("id") and r.get("parentId", parent_id) == parent_id]
        return rows, ""

    def gather(self, label, execution, expected_children=1):
        """The parent's logs plus the logs and task states of its loop sub-execution(s), then diagnostics (INFO)."""
        parent_id = execution.get("id")
        deadline = time.monotonic() + float(os.environ.get("DRIVER_EVIDENCE_TIMEOUT", "60"))
        rows, problem, failures = None, "", 0
        while True:
            rows, problem = self.children(parent_id)
            if rows is None:
                failures += 1
            elif len(rows) >= expected_children and all(state_of(self.fetch(r["id"])[0]) in TERMINAL for r in rows):
                break
            if failures >= 3 or time.monotonic() >= deadline:
                break
            time.sleep(self.poll)
        children = None
        if rows is not None:
            children = [{"id": r["id"], "execution": self.fetch(r["id"])[0], "rows": self.logs(r["id"])} for r in rows]
            if len(rows) != expected_children:
                problem = "found %d loop sub-execution(s), expected %d" % (len(rows), expected_children)
        evidence = Evidence(execution, self.logs(parent_id), children, problem)
        self.diagnostics(label, evidence, rows)
        return evidence

    def diagnostics(self, label, ev, search_rows):
        """INFO lines only: ids, states, counts and key names. No message bodies, no environment, no raw responses."""
        def states(execution):
            by_id = tasks_by_id(execution)
            return "absent" if by_id is None else ", ".join("%s=%s" % (t, "/".join(str(s) for s in task_state(execution, t))) for t in by_id) or "empty"
        self.report.info("%s diagnostics: parent taskRunList" % label, states(ev.parent))
        self.report.info("%s diagnostics: parent logs by taskId/level" % label, group_logs(ev.rows))
        self.report.info("%s diagnostics: execution search kind=LOOP parentId=%s" % (label, ev.parent.get("id")),
                         "unavailable: %s" % ev.problem if search_rows is None else "%d row(s); row keys %s" % (len(search_rows), sorted(search_rows[0])[:24] if search_rows else []))
        for child in ev.children or []:
            execution = child["execution"] or {}
            loop_run = execution.get("loopRun")
            self.report.info("%s diagnostics: sub-execution %s" % (label, child["id"]),
                             "kind=%s state=%s parentId=%s loopRun keys=%s; taskRunList: %s; logs by taskId/level: %s" % (
                                 execution.get("kind"), state_of(execution), execution.get("parentId"),
                                 sorted(loop_run)[:10] if isinstance(loop_run, dict) else None, states(execution), group_logs(child["rows"])))

    # ---------------------------------------------------------------- shared checks
    def slack_check(self, label, key, ev, failed):
        """Critical. PASS only when the evidence COVERS the nested tasks. Slack tasks live inside the Loop iteration (slack_alert) or
        the errors block (slack_failure); absence from a list that cannot show them proves nothing, so that case is UNVERIFIED."""
        name = "%s: no Slack task ran (notify_slack=false)" % label
        seen = {}
        for execution in [ev.parent] + [c["execution"] for c in ev.children or []]:
            by_id = tasks_by_id(execution)
            for task in SLACK_TASKS:
                if by_id and task in by_id:
                    seen.setdefault(task, []).extend(task_state(execution, task))
        log_ids = {r.get("taskId") for r in ev.all_rows() if r.get("taskId")}
        bad = {t: s for t, s in seen.items() if any(x != "SKIPPED" for x in s)}
        logged = sorted(log_ids & set(SLACK_TASKS))
        if bad or logged:
            self.report.check(name, False, "Slack task evidence found: states %s, log taskIds %s" % (bad, logged), key=key)
            return
        parent_marker = "log_failure" if failed else "run_summary"
        parent_ids = (tasks_by_id(ev.parent) or {}).keys() if tasks_by_id(ev.parent) is not None else None
        lists_cover = (parent_ids is not None and parent_marker in parent_ids and ev.children_found
                       and all("compare_snapshots" in (tasks_by_id(c["execution"]) or {}) for c in ev.children))
        parent_log_ids = {r.get("taskId") for r in ev.rows or []}
        logs_cover = (ev.rows is not None and parent_marker in parent_log_ids and ev.children_found
                      and all("compare_snapshots" in {r.get("taskId") for r in c["rows"] or []} for c in ev.children))
        if lists_cover:
            self.report.check(name, True, "task lists of the parent (contains %s) and of the loop sub-execution (contains compare_snapshots) show no Slack task" % parent_marker, key=key)
        elif logs_cover:
            self.report.check(name, True, "task-tagged logs of the parent (%s) and of the loop sub-execution (compare_snapshots) show no Slack task" % parent_marker, key=key)
        else:
            self.report.unverified(name, "the evidence does not cover the nested tasks (%s); absence of Slack tasks from a partial view is not proof"
                                   % (ev.problem or "task lists or task-tagged logs of the parent or the sub-execution are missing"), key=key)

    def task_states_check(self, label, ev):
        """All seven flow tasks ended SUCCESS across the parent AND the loop sub-execution (notify_changes is an If: SUCCESS or SKIPPED)."""
        name = "%s: all %d tasks ended SUCCESS (parent and loop sub-execution)" % (label, len(EXPECTED_TASKS))
        if tasks_by_id(ev.parent) is None or not ev.children_found or any(tasks_by_id(c["execution"]) is None for c in ev.children):
            self.report.unverified(name, ev.problem or "task lists of the parent or the sub-execution are not available")
            return
        merged = {"taskRunList": [run for e in [ev.parent] + [c["execution"] for c in ev.children] for run in e["taskRunList"] if isinstance(run, dict)]}
        bad = {}
        for task in EXPECTED_TASKS:
            states = task_state(merged, task)
            if states != ["SUCCESS"] and not (task == "notify_changes" and states in (["SUCCESS"], ["SKIPPED"])):
                bad[task] = states
        self.report.check(name, not bad, str(bad) if bad else "")

    def child_state_check(self, label, ev, expected, failing_task=None):
        name = "%s: the loop sub-execution ended %s%s" % (label, expected, (" in task %s" % failing_task) if failing_task else "")
        if not ev.children_found or any(c["execution"] is None for c in ev.children):
            self.report.unverified(name, ev.problem or "the sub-execution could not be read")
            return
        states = [state_of(c["execution"]) for c in ev.children]
        ok = states == [expected]
        if ok and failing_task:
            by_id = tasks_by_id(ev.children[0]["execution"])
            if by_id is None:
                self.report.unverified(name, "the sub-execution has no taskRunList")
                return
            ok = "FAILED" in task_state(ev.children[0]["execution"], failing_task)
        self.report.check(name, ok, "sub-execution states %s" % states)

    def text_check(self, name, needle, ev):
        """PASS when the text is in the logs; FAIL only when the logs are complete and it is absent; UNVERIFIED when the view is partial."""
        if needle in ev.blob():
            self.report.check(name, True)
        elif ev.complete:
            self.report.check(name, False, "not found in %d log entries (parent %d, sub-execution(s) %d)" % (len(ev.all_rows()), len(ev.rows or []), len(ev.all_rows()) - len(ev.rows or [])))
        else:
            self.report.unverified(name, ev.problem or "the logs of the parent or of the loop sub-execution are not available")

    def shape_info(self, execution, rows):
        runs = (execution or {}).get("taskRunList")
        self.report.info("shapes seen: execution keys", sorted((execution or {}).keys())[:20])
        self.report.info("shapes seen: taskRun keys", sorted(runs[0].keys())[:20] if isinstance(runs, list) and runs and isinstance(runs[0], dict) else "no taskRunList")
        self.report.info("shapes seen: log entry keys", sorted(rows[0].keys())[:20] if rows else "no log entries")
        carries = any(isinstance(r, dict) and r.get("outputs") for r in runs) if isinstance(runs, list) else False
        self.report.info("the execution JSON carries task outputs", "yes (used for information only)" if carries else "no (as documented for Kestra 2.0; nothing required depends on them)")

    def same_kv(self, label, key_label, ns, key, expected):
        status, value = self.kv(ns, key)
        name = "%s: the stored snapshot is byte-for-byte unchanged (checkpoint did not advance)" % label
        if expected is None or status not in ("ok", "missing"):
            self.report.unverified(name, "the KV value could not be read (%s) or no baseline value was available" % status, key=key_label)
        elif value is None:
            self.report.check(name, False, "the snapshot disappeared (%s)" % status, key=key_label)
        else:
            same = hashlib.sha256(value.encode()).hexdigest() == hashlib.sha256(expected.encode()).hexdigest()
            self.report.check(name, same, "KV lookup: %s" % status, key=key_label)

    # ---------------------------------------------------------------- scenarios
    def s1_baseline(self, ns, fid, gene, prefix):
        label = "S1 baseline"
        started = time.monotonic()
        ex, size = self.run(ns, fid, self.inputs(gene, prefix))
        self.report.check("%s: execution ends SUCCESS" % label, state_of(ex) == "SUCCESS", "state %s in %ds" % (state_of(ex), time.monotonic() - started), key="s1_success")
        if ex is None:
            return None
        ev = self.gather(label, ex)
        self.slack_check(label, "s1_slack", ev, failed=False)
        self.task_states_check(label, ev)
        self.child_state_check(label, ev, "SUCCESS")
        self.shape_info(ex, ev.rows)
        self.report.info("%s: size of the execution JSON" % label, "%d bytes" % size)
        blob = ev.blob()
        summary = parse_summary(blob, gene) if ev.rows is not None or ev.children_found else None
        if summary is None:
            self.report.unverified("%s: the script's summary line in the logs" % label, ev.problem or "no 'GENE: PHASE (mode). scanned=...' line found in %d log entries" % len(ev.all_rows()))
        else:
            self.report.check("%s: the logged summary shows a full-scan baseline" % label,
                              summary["phase"] == "BASELINE" and summary["mode"] == "full" and summary["scanned"] == summary["total"] > 0,
                              "phase=%s mode=%s scanned=%s of %s, %s request(s)" % (summary["phase"], summary["mode"], summary["scanned"], summary["total"], summary["requests"]))
        self.text_check("%s: the baseline sent no alert (the script's log line says so)" % label, "No alert is sent on the first run", ev)
        if ev.rows is None:
            self.report.unverified("%s: run_summary printed the gene line built with loopOutputs()" % label, "the parent's logs are not available")
        else:
            self.report.check("%s: run_summary printed the gene line built with loopOutputs()" % label, "ClinVar monitor finished" in self.blob(ev.rows) and ("%s: BASELINE" % gene) in self.blob(ev.rows))
        status, value = self.kv(ns, "%s_%s" % (prefix, gene.lower()))
        name = "%s: the snapshot is in the KV store as a schema 2 snapshot for %s with entries" % (label, gene)
        snap = None
        if status != "ok" and status != "missing":
            self.report.unverified(name, "KV lookup answered %s" % status, key="s1_snapshot")
        elif value is None:
            self.report.check(name, False, "KV lookup: %s" % status, key="s1_snapshot")
        else:
            try:
                snap = json.loads(value)
            except ValueError:
                snap = None
            count = len((snap or {}).get("variants") or {})
            self.report.check(name, bool(snap) and snap.get("schema") == 2 and snap.get("gene") == gene and count > 0,
                              "schema=%s gene=%s variants=%s" % ((snap or {}).get("schema"), (snap or {}).get("gene"), count), key="s1_snapshot")
            self.report.check("%s: checkpoint and reconciled dates are today (UTC)" % label, bool(snap) and snap.get("checkpoint") in today_days() and snap.get("reconciled") in today_days(),
                              "checkpoint=%s reconciled=%s" % ((snap or {}).get("checkpoint"), (snap or {}).get("reconciled")))
            self.report.info("%s: size of the KV value" % label, "%d bytes" % len(value))
            if summary is None:
                self.report.unverified("%s: the snapshot's entry count equals the script's reported snapshot_size" % label, "no logged summary to compare with (the count is not compared against anything unknown)")
            else:
                self.report.check("%s: the snapshot's entry count equals the script's reported snapshot_size" % label, count == summary["size"], "snapshot %d, logged %d" % (count, summary["size"]))
        return {"execution": ex, "kv": value}

    def s2_incremental(self, ns, fid, gene, prefix, first):
        label = "S2 incremental"
        ex, _ = self.run(ns, fid, self.inputs(gene, prefix))
        self.report.check("%s: execution ends SUCCESS" % label, state_of(ex) == "SUCCESS", "state %s" % state_of(ex), key="s2_success")
        if ex is None:
            return None
        ev = self.gather(label, ex)
        self.slack_check(label, "s2_slack", ev, failed=False)
        self.task_states_check(label, ev)
        self.child_state_check(label, ev, "SUCCESS")
        summary = parse_summary(ev.blob(), gene) if ev.rows is not None or ev.children_found else None
        if summary is None:
            self.report.unverified("%s: the logged summary shows an incremental scan" % label, ev.problem or "no summary line in %d log entries" % len(ev.all_rows()))
        else:
            self.report.check("%s: the logged summary shows an incremental scan" % label, summary["mode"] == "incremental" and summary["phase"] in ("NO CHANGES", "CHANGES"),
                              "phase=%s mode=%s scanned=%s of %s" % (summary["phase"], summary["mode"], summary["scanned"], summary["total"]))
            if ev.complete:
                self.report.check("%s: no reconciliation line (that is for scheduled full scans only)" % label, "RECONCILIATION:" not in ev.blob())
            else:
                self.report.unverified("%s: no reconciliation line (that is for scheduled full scans only)" % label, ev.problem or "the sub-execution's logs are not available")
        status, value = self.kv(ns, "%s_%s" % (prefix, gene.lower()))
        name = "%s: the checkpoint advanced and the full-scan date did not move" % label
        try:
            before, after = json.loads(first["kv"]), json.loads(value)
        except (TypeError, ValueError):
            before = after = None
        if status not in ("ok", "missing"):
            self.report.unverified(name, "KV lookup answered %s" % status, key="s2_checkpoint")
        elif not after or not before:
            self.report.check(name, False, "the KV snapshot is not readable as JSON after the second run (%s)" % status, key="s2_checkpoint")
        else:
            self.report.check(name, after.get("checkpoint") in today_days() and after.get("reconciled") == before.get("reconciled"),
                              "checkpoint=%s reconciled=%s" % (after.get("checkpoint"), after.get("reconciled")), key="s2_checkpoint")
        return value

    def s3_invalid_gene(self, ns, fid, gene, prefix, kv_before):
        label = "S3 invalid gene"
        bad = "ZZZNOTAGENE9"
        ex, _ = self.run(ns, fid, self.inputs(bad, prefix))
        self.report.check("%s: execution ends FAILED" % label, state_of(ex) == "FAILED", "state %s" % state_of(ex), key="s3_failed")
        ev = self.gather(label, ex) if ex else None
        if ev is not None:
            self.slack_check(label, "s3_slack", ev, failed=True)
        else:
            self.report.unverified("%s: no Slack task ran (notify_slack=false)" % label, "there is no execution to inspect", key="s3_slack")
        status, value = self.kv(ns, "%s_%s" % (prefix, bad.lower()))
        name = "%s: no snapshot was created for the invalid gene" % label
        if status == "missing":
            self.report.check(name, True, "KV lookup: missing", key="s3_no_snapshot_for_bad_gene")
        elif status == "ok":
            self.report.check(name, False, "a snapshot exists for the invalid gene", key="s3_no_snapshot_for_bad_gene")
        else:
            self.report.unverified(name, "KV lookup answered %s" % status, key="s3_no_snapshot_for_bad_gene")
        self.same_kv(label, "s3_kv_unchanged", ns, "%s_%s" % (prefix, gene.lower()), kv_before)
        if ev is not None:
            self.child_state_check(label, ev, "FAILED", failing_task="compare_snapshots")
            self.text_check("%s: the logs state that ClinVar does not recognise the query" % label, "does not recognise part of the query", ev)
            self.text_check("%s: the errors block logged the failure (log_failure)" % label, "ClinVar monitor FAILED in flow", ev)

    def s4_query_mismatch(self, ns, fid, gene, prefix, kv_before):
        label = "S4 changed extra query"
        ex, _ = self.run(ns, fid, self.inputs(gene, prefix, extra_query="foo[bar]"))
        self.report.check("%s: execution ends FAILED" % label, state_of(ex) == "FAILED", "state %s" % state_of(ex), key="s4_failed")
        ev = self.gather(label, ex) if ex else None
        if ev is not None:
            self.slack_check(label, "s4_slack", ev, failed=True)
        else:
            self.report.unverified("%s: no Slack task ran (notify_slack=false)" % label, "there is no execution to inspect", key="s4_slack")
        self.same_kv(label, "s4_kv_unchanged", ns, "%s_%s" % (prefix, gene.lower()), kv_before)
        if ev is not None:
            self.child_state_check(label, ev, "FAILED", failing_task="compare_snapshots")
            self.text_check("%s: the logs name the different extra query" % label, "different extra query", ev)
    def s5_concurrency(self, ns, fid, gene, prefix):
        label = "S5 concurrency"
        ids = [None, None]

        def launch(i):
            ids[i] = self.start(ns, fid, self.inputs(gene, prefix))
        threads = [threading.Thread(target=launch, args=(i,)) for i in (0, 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if not all(ids):
            self.report.check("%s: both executions were started" % label, False, str(ids))
            return
        runs = [self.wait(i)[0] for i in ids]
        self.report.check("%s: both executions end SUCCESS" % label, all(state_of(r) == "SUCCESS" for r in runs), str([state_of(r) for r in runs]))
        queued = [any(s == "QUEUED" for s, _ in histories(r)) for r in runs]
        spans = []
        for r in runs:
            h = histories(r)
            spans.append((next((d for s, d in h if s == "RUNNING"), None), h[-1][1] if h else None))
        if all(a and b for a, b in spans):
            overlap = spans[0][0] < spans[1][1] and spans[1][0] < spans[0][1]
            self.report.check("%s: the two executions did not run at the same time (limit 1)" % label, not overlap, "spans %s" % [(str(a), str(b)) for a, b in spans])
        else:
            self.report.unverified("%s: non-overlap of the two runs" % label, "state histories lack RUNNING/end timestamps")
        if any(queued):
            self.report.check("%s: one execution was held in state QUEUED" % label, True, "queued flags %s" % queued)
        else:
            self.report.unverified("%s: a QUEUED state in the history" % label, "no QUEUED entry seen (the first execution may have finished before the second was created)")

    @staticmethod
    def inputs(gene, prefix, extra_query=""):
        return {"genes": json.dumps([gene]), "notify_slack": "false", "state_key_prefix": prefix, "extra_query": extra_query}


def main(argv=None):
    global LAST_REPORT
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--gene", default="MAX")
    parser.add_argument("--expected-version-prefix", default="2.0.")
    parser.add_argument("--container", default="kestra", help="name of the Kestra container (image label evidence)")
    parser.add_argument("--flow-image", default="python:3.12-slim", help="the flow's containerImage (Docker daemon event evidence)")
    parser.add_argument("--min-pass", type=int, default=MIN_PASS, help="documented minimum number of PASS results (default %d)" % MIN_PASS)
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

    report = LAST_REPORT = Report()
    poll = float(os.environ.get("DRIVER_POLL_SECONDS", "3"))
    driver = Driver(Client(args.base_url, user, password), report, poll)
    started_at = time.time()
    try:
        flow_text = Path(args.flow).read_text(encoding="utf-8")
        deployed, removed = prepare_deployment(flow_text)
        report.info("deployment copy differs from the flow file in two ways only", "top-level extend: block removed (%d lines, gallery metadata) and 'disabled: true' added to the schedule trigger; %d -> %d lines" % (removed, flow_text.count("\n"), deployed.count("\n")))
        if not driver.wait_for_server(args.startup_timeout):
            report.check("the Kestra server became ready", False, "no authenticated API answer within %ds" % args.startup_timeout)
        else:
            report.check("the Kestra server became ready", True, "API prefix %s" % driver.client.prefix)
            driver.check_version(args.expected_version_prefix, args.container)
            namespace, flow_id, created = driver.deploy(deployed)
            if created:
                prefix = "clinvar_ci_" + uuid.uuid4().hex[:8]
                first = driver.s1_baseline(namespace, flow_id, args.gene, prefix)
                if first and first["kv"]:
                    second_kv = driver.s2_incremental(namespace, flow_id, args.gene, prefix, first)
                    baseline_kv = second_kv or first["kv"]
                    driver.s3_invalid_gene(namespace, flow_id, args.gene, prefix, baseline_kv)
                    driver.s4_query_mismatch(namespace, flow_id, args.gene, prefix, baseline_kv)
                    driver.s5_concurrency(namespace, flow_id, args.gene, prefix)
                else:
                    report.info("S2 to S5 skipped", "S1 did not leave a snapshot to build on")
                driver.docker_evidence(started_at, args.flow_image)
    except Exception as exc:  # report, never a bare traceback; the checks already printed stay valid
        report.check("the driver finished without an internal error", False, "%s: %s" % (type(exc).__name__, exc))
    say("")
    say("PASS %d (minimum %d) | FAIL %d | UNVERIFIED %d | INFO %d" % (report.count("PASS"), args.min_pass, report.count("FAIL"), report.count("UNVERIFIED"), report.count("INFO")))
    code = report.finish(args.min_pass)
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(report.markdown(args.min_pass))
    return code


if __name__ == "__main__":
    sys.exit(main())
