#!/usr/bin/env python3
"""Run the ClinVar monitor flow on a REAL Kestra server through its REST API and check what happened.

Standard library only. It talks to ONE Kestra server, which must be on loopback, with throwaway credentials taken from the
environment (KESTRA_USER, KESTRA_PASSWORD). Every printed line passes through redact(), which removes the password and its
encoded forms.

Evidence. Kestra 2.0 no longer returns task outputs from the execution endpoint, so nothing REQUIRED depends on them. Results come from:
  * the execution logs (the flow's own log lines: summary, loopOutputs line, failure reasons),
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

Scenarios (all with notify_slack=false and the small gene MAX; the flow is deployed unchanged except that its weekly schedule trigger
is disabled in the copy sent to Kestra):
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
        """Critical. Evidence: the configs endpoint and/or the image label of the running container. Any contradiction is a FAIL."""
        sources = {}
        for path in (self.client.prefix + "/configs", "/api/v1/configs"):
            status, data, _ = self.client.json("GET", path)
            if status == 200 and isinstance(data, dict) and data.get("version"):
                sources["API " + path] = str(data["version"])
                break
        label = docker(["inspect", container, "--format", '{{index .Config.Labels "org.opencontainers.image.version"}}'])
        if label and label.strip() and label.strip() != "<no value>":
            sources["image label"] = label.strip()
        if not sources:
            self.report.unverified("the server is Kestra %sx" % expected_prefix, "neither the configs endpoint nor the image label could be read", key="version")
            return
        ok = all(v.lstrip("v").startswith(expected_prefix) for v in sources.values())
        self.report.check("the server is Kestra %sx" % expected_prefix, ok, "; ".join("%s = %s" % kv for kv in sources.items()), key="version")

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

    # ---------------------------------------------------------------- shared checks
    def slack_check(self, label, key, execution, rows):
        """Critical: evidence that no Slack task ran. From taskRunList states if returned, else from task-tagged log entries."""
        name = "%s: no Slack task ran (notify_slack=false)" % label
        by_id = tasks_by_id(execution)
        if by_id is not None:
            ran = {t: task_state(execution, t) for t in SLACK_TASKS if t in by_id}
            bad = {t: s for t, s in ran.items() if any(x != "SKIPPED" for x in s)}
            self.report.check(name, not bad, "from taskRunList; Slack tasks listed: %s" % (ran or "none"), key=key)
            return
        ids = {r.get("taskId") for r in (rows or []) if r.get("taskId")}
        if "compare_snapshots" in ids:
            bad = ids & set(SLACK_TASKS)
            self.report.check(name, not bad, "from log entries tagged with taskId (seen: %s)" % sorted(ids), key=key)
            return
        self.report.unverified(name, "neither a taskRunList nor task-tagged log entries are available", key=key)

    def task_states_check(self, label, execution):
        """Optional: all flow tasks SUCCESS, only when the server returns a taskRunList."""
        by_id = tasks_by_id(execution)
        if by_id is None:
            self.report.info("%s: per-task states" % label, "the execution JSON has no taskRunList; optional, nothing required depends on it")
            return
        bad = {t: task_state(execution, t) for t in EXPECTED_TASKS if task_state(execution, t) != ["SUCCESS"]}
        self.report.check("%s: all %d tasks ended SUCCESS" % (label, len(EXPECTED_TASKS)), not bad, str(bad) if bad else "")

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
        rows = self.logs(ex.get("id"))
        self.slack_check(label, "s1_slack", ex, rows)
        self.task_states_check(label, ex)
        self.shape_info(ex, rows)
        self.report.info("%s: size of the execution JSON" % label, "%d bytes" % size)
        summary = parse_summary(self.blob(rows), gene) if rows is not None else None
        if rows is None:
            self.report.unverified("%s: the script's summary line and the loopOutputs line in the logs" % label, "the logs endpoint did not answer in the expected shape")
        elif summary is None:
            self.report.unverified("%s: the script's summary line in the logs" % label, "no 'GENE: PHASE (mode). scanned=...' line found in %d log entries" % len(rows))
        else:
            self.report.check("%s: the logged summary shows a full-scan baseline" % label,
                              summary["phase"] == "BASELINE" and summary["mode"] == "full" and summary["scanned"] == summary["total"] > 0,
                              "phase=%s mode=%s scanned=%s of %s, %s request(s)" % (summary["phase"], summary["mode"], summary["scanned"], summary["total"], summary["requests"]))
            blob = self.blob(rows)
            self.report.check("%s: the baseline sent no alert (log says so)" % label, "No alert is sent on the first run" in blob)
            self.report.check("%s: run_summary printed the gene line built with loopOutputs()" % label, "ClinVar monitor finished" in blob and ("%s: BASELINE" % gene) in blob)
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
        rows = self.logs(ex.get("id"))
        self.slack_check(label, "s2_slack", ex, rows)
        self.task_states_check(label, ex)
        if rows is None:
            self.report.unverified("%s: the logged summary shows an incremental scan" % label, "the logs endpoint did not answer in the expected shape")
        else:
            summary = parse_summary(self.blob(rows), gene)
            if summary is None:
                self.report.unverified("%s: the logged summary shows an incremental scan" % label, "no summary line in %d log entries" % len(rows))
            else:
                self.report.check("%s: the logged summary shows an incremental scan" % label, summary["mode"] == "incremental" and summary["phase"] in ("NO CHANGES", "CHANGES"),
                                  "phase=%s mode=%s scanned=%s of %s" % (summary["phase"], summary["mode"], summary["scanned"], summary["total"]))
                self.report.check("%s: no reconciliation line (that is for scheduled full scans only)" % label, "RECONCILIATION:" not in self.blob(rows))
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
        rows = self.logs(ex.get("id")) if ex else None
        if ex is not None:
            self.slack_check(label, "s3_slack", ex, rows)
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
        if rows is None:
            self.report.unverified("%s: the failure reason and the error handler in the logs" % label, "the logs endpoint did not answer in the expected shape")
        else:
            blob = self.blob(rows)
            self.report.check("%s: the logs state that ClinVar does not recognise the query" % label, "does not recognise part of the query" in blob)
            self.report.check("%s: the errors block logged the failure (log_failure)" % label, "ClinVar monitor FAILED in flow" in blob)

    def s4_query_mismatch(self, ns, fid, gene, prefix, kv_before):
        label = "S4 changed extra query"
        ex, _ = self.run(ns, fid, self.inputs(gene, prefix, extra_query="foo[bar]"))
        self.report.check("%s: execution ends FAILED" % label, state_of(ex) == "FAILED", "state %s" % state_of(ex), key="s4_failed")
        rows = self.logs(ex.get("id")) if ex else None
        if ex is not None:
            self.slack_check(label, "s4_slack", ex, rows)
        else:
            self.report.unverified("%s: no Slack task ran (notify_slack=false)" % label, "there is no execution to inspect", key="s4_slack")
        self.same_kv(label, "s4_kv_unchanged", ns, "%s_%s" % (prefix, gene.lower()), kv_before)
        if rows is None:
            self.report.unverified("%s: the failure reason in the logs" % label, "the logs endpoint did not answer in the expected shape")
        else:
            self.report.check("%s: the logs name the different extra query" % label, "different extra query" in self.blob(rows))

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
        deployed = disable_trigger(flow_text)
        report.info("the flow is deployed unchanged except for one added line", "'disabled: true' on the schedule trigger; %d -> %d lines" % (flow_text.count("\n"), deployed.count("\n")))
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
