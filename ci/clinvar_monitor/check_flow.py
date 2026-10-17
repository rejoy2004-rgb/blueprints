#!/usr/bin/env python3
"""Static checks for the ClinVar variant reclassification monitor flow, and for the CI workflow that runs them.

Structure-aware: the YAML is parsed, never grepped. The embedded Python script is compiled but NEVER executed.
No network access, no secrets, no dependency other than PyYAML.

usage: check_flow.py --flow FLOW.yaml [--flows-dir DIR] [--workflow WORKFLOW.yml]
Exit status 0 only if every check passes.
"""
import argparse
import re
import sys
from pathlib import Path

import yaml

ALLOWED_SECRETS = {"NCBI_API_KEY", "SLACK_WEBHOOK_URL"}
EXTEND_FIELDS = ("title", "metaTitle", "shortDescription", "metaDescription", "description", "tags", "subTags", "ee", "demo")
CREDENTIAL_PATTERNS = {
    "Slack webhook URL": r"hooks\.slack\.com/services/[A-Za-z0-9/]{8,}",
    "Slack token": r"\bxox[abprs]-[A-Za-z0-9-]{10,}",
    "AWS access key id": r"\bAKIA[0-9A-Z]{16}\b",
    "GitHub token": r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})\b",
    "private key block": r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    "credential assigned a literal value": r"(?i)(?<![A-Za-z0-9_])(?:api[_-]?key|token|password|passwd|secret)(?![A-Za-z0-9_])\s*[:=]\s*['\"]?[A-Za-z0-9/+_=-]{16,}",
}
PEBBLE_TOKENS = ("{{", "{%", "{#")  # Kestra renders the script property as a template
SAFE_RUN_EXPRESSIONS = {"github.workspace"}


class UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys (PyYAML silently keeps the last one)."""


def _unique_mapping(loader, node, deep=False):
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.constructor.ConstructorError(None, None, "duplicate key %r" % (key,), key_node.start_mark)
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep)


UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


class Report:
    def __init__(self):
        self.failures = 0

    def check(self, name, ok, detail=""):
        print("%s  %s%s" % ("PASS" if ok else "FAIL", name, (" -- " + str(detail)) if detail else ""))
        if not ok:
            self.failures += 1
        return ok


def load(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=UniqueKeyLoader)


def walk_tasks(tasks, path="tasks"):
    """Yield (path, task) for every task, descending into Loop/If/Parallel bodies."""
    for index, task in enumerate(tasks or []):
        here = "%s[%d]" % (path, index)
        yield here, task
        for key in ("tasks", "then", "else", "finally"):
            if isinstance(task.get(key), list):
                yield from walk_tasks(task[key], "%s.%s" % (here, key))


def walk_scalars(node, path=""):
    """Yield (path, string) for every string in a parsed YAML document, keys included."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield path + "/" + str(key), str(key)
            yield from walk_scalars(value, path + "/" + str(key))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from walk_scalars(value, "%s[%d]" % (path, index))
    elif isinstance(node, str):
        yield path, node


def check_flow(report, flow_path, flows_dir):
    path = Path(flow_path)
    if not report.check("flow file exists", path.is_file(), str(path)):
        return
    try:
        flow = load(path)
    except yaml.YAMLError as exc:
        report.check("flow YAML parses with no duplicate keys", False, str(exc).splitlines()[0])
        return
    report.check("flow YAML parses with no duplicate keys", isinstance(flow, dict))
    if not isinstance(flow, dict):
        return

    report.check("id matches the filename", flow.get("id") == path.stem, "%r vs %r" % (flow.get("id"), path.stem))
    report.check("namespace is set", isinstance(flow.get("namespace"), str) and flow["namespace"].strip() != "")
    extend = flow.get("extend") if isinstance(flow.get("extend"), dict) else {}
    missing = [f for f in EXTEND_FIELDS if f not in extend]
    report.check("extend block has all 9 README fields", not missing, "missing: %s" % missing if missing else "")
    report.check("demo is false (contributed blueprints)", extend.get("demo") is False)

    tasks = list(walk_tasks(flow.get("tasks")))
    errors = list(walk_tasks(flow.get("errors"), "errors"))
    all_tasks = tasks + errors
    ids = [t.get("id") for _, t in all_tasks]
    report.check("task ids are unique", len(ids) == len(set(ids)), "duplicates: %s" % sorted({i for i in ids if ids.count(i) > 1}))
    bad_types = [(p, t.get("type")) for p, t in all_tasks if not (isinstance(t.get("type"), str) and t["type"].startswith("io.kestra.plugin."))]
    report.check("every task has an io.kestra.plugin.* type", not bad_types, str(bad_types) if bad_types else "")
    triggers = flow.get("triggers") or []
    report.check("at least one trigger is defined", len(triggers) >= 1)

    # plugin types: evidence from the rest of the repository, not proof (kestra.io/plugins is authoritative)
    if flows_dir:
        known = set()
        for other in Path(flows_dir).glob("*.yaml"):
            if other.resolve() == path.resolve():
                continue
            known.update(re.findall(r"^\s*(?:-\s+)?type:\s*(io\.kestra\.\S+)\s*$", other.read_text(encoding="utf-8", errors="replace"), re.M))
        used = {t.get("type") for _, t in all_tasks} | {t.get("type") for t in triggers if isinstance(t, dict)}
        unseen = sorted(u for u in used if u and u not in known)
        report.check("every plugin type is also used by other flows in the repository (existence evidence)", not unseen,
                     "used only here: %s" % unseen if unseen else "%d types, %d other flows scanned" % (len(used), len(list(Path(flows_dir).glob('*.yaml'))) - 1))

    # embedded Python: compile only
    scripts = [(p, t) for p, t in all_tasks if str(t.get("type", "")).endswith(".python.Script")]
    report.check("the flow has an embedded Python script task", len(scripts) >= 1)
    stdlib = getattr(sys, "stdlib_module_names", None)
    for where, task in scripts:
        script = task.get("script")
        name = "%s (%s)" % (task.get("id"), where)
        if not isinstance(script, str):
            report.check("script compiles: %s" % name, False, "no script string")
            continue
        try:
            tree = compile(script, "embedded:%s" % task.get("id"), "exec", dont_inherit=True, flags=0)  # compiled, never executed
            report.check("embedded Python compiles without being executed: %s" % name, tree is not None)
        except SyntaxError as exc:
            report.check("embedded Python compiles without being executed: %s" % name, False, "line %s: %s" % (exc.lineno, exc.msg))
            continue
        found = [tok for tok in PEBBLE_TOKENS if tok in script]
        report.check("script has no template delimiters Kestra would interpret: %s" % name, not found, str(found) if found else "")
        if stdlib is not None:
            import ast
            imported = set()
            for node in ast.walk(ast.parse(script)):
                if isinstance(node, ast.Import):
                    imported.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    imported.add(node.module.split(".")[0])
            report.check("script imports only the standard library (the container installs no packages): %s" % name,
                         imported <= stdlib, "non-stdlib: %s" % sorted(imported - stdlib) if imported - stdlib else "")

    # credentials: every scalar in the document, keys included
    hits = []
    for where, text in walk_scalars(flow):
        for label, pattern in CREDENTIAL_PATTERNS.items():
            if re.search(pattern, text):
                hits.append("%s at %s" % (label, where))
    report.check("no credential-looking literals anywhere in the flow", not hits, "; ".join(hits[:5]))

    # secrets: only where expected, always inside a template expression
    placed, stray = [], []
    for where, text in walk_scalars(flow):
        for match in re.finditer(r"secret\(\s*'([^']*)'\s*\)", text):
            placed.append((where, match.group(1)))
        if "secret(" in text and not re.search(r"\{\{[^}]*secret\(", text):
            stray.append(where)
    names = {n for _, n in placed}
    report.check("secrets referenced are only %s" % sorted(ALLOWED_SECRETS), names <= ALLOWED_SECRETS, "unexpected: %s" % sorted(names - ALLOWED_SECRETS) if names - ALLOWED_SECRETS else "")
    report.check("every secret() call sits inside a {{ }} expression", not stray, str(stray) if stray else "")
    report.check("no secret() call inside the embedded script", not any("/script" in w for w, _ in placed))
    slack = [t for _, t in all_tasks if str(t.get("type", "")).endswith("SlackIncomingWebhook")]
    report.check("every Slack task takes its webhook from {{ secret('SLACK_WEBHOOK_URL') }}",
                 all(re.fullmatch(r"\{\{\s*secret\('SLACK_WEBHOOK_URL'\)\s*\}\}", str(t.get("url", ""))) for t in slack),
                 "%d Slack task(s)" % len(slack))
    for _, task in scripts:
        env = task.get("env") or {}
        key = env.get("NCBI_API_KEY", "")
        ok = ("NCBI_API_KEY" not in env) or ("secret('NCBI_API_KEY')" in key and "use_ncbi_api_key" in key)
        report.check("the NCBI key reaches the script only through env and only when use_ncbi_api_key is set", ok)
        report.check("the script task passes no secret through other env variables", set(env) <= {"NCBI_API_KEY"}, str(sorted(env)))
    cron = [t.get("cron") for t in triggers if isinstance(t, dict) and "cron" in t]
    print("INFO  flow schedule(s): %s (informational: the production schedule belongs to the flow; CI never runs the flow)" % cron)


def check_workflow(report, workflow_path):
    path = Path(workflow_path)
    if not report.check("CI workflow file exists", path.is_file(), str(path)):
        return
    text = path.read_text(encoding="utf-8")
    try:
        wf = load(path)
    except yaml.YAMLError as exc:
        report.check("CI workflow YAML parses with no duplicate keys", False, " ".join(str(exc).split())[:160])
        return
    if not report.check("CI workflow YAML parses with no duplicate keys", isinstance(wf, dict)):
        return
    events = wf.get(True, wf.get("on"))  # PyYAML (YAML 1.1) reads the bare key `on` as boolean True
    names = set(events) if isinstance(events, (dict, list)) else {events}
    report.check("CI triggers are limited to push / pull_request / workflow_dispatch", names <= {"push", "pull_request", "workflow_dispatch"}, str(sorted(map(str, names))))
    report.check("CI has no schedule (no production-style recurring run)", "schedule" not in names)
    report.check("CI does not use pull_request_target / workflow_run / repository_dispatch", not (names & {"pull_request_target", "workflow_run", "repository_dispatch"}))
    report.check("workflow permissions are exactly contents: read", wf.get("permissions") == {"contents": "read"}, str(wf.get("permissions")))
    jobs = wf.get("jobs") or {}
    pins = []
    for job in jobs.values():
        report.check("job sets a timeout", "timeout-minutes" in job)
        for step in job.get("steps", []):
            if "uses" in step:
                pins.append(step["uses"])
    report.check("every action is pinned to a 40-character commit SHA", pins and all(re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", p) for p in pins), str(pins))
    report.check("no repository secrets are referenced", not re.search(r"secrets\.", text))
    report.check("no NCBI or Slack variable is configured", not re.search(r"NCBI_API_KEY|SLACK_WEBHOOK|hooks\.slack\.com", text))
    installs = [line.strip() for job in jobs.values() for step in job.get("steps", [])
                for line in str(step.get("run", "")).splitlines() if "pip install" in line]
    report.check("pip installs PyYAML from binary wheels only (--only-binary=:all:) with a pinned version",
                 bool(installs) and all("--only-binary=:all:" in line and re.search(r"==\d", line) for line in installs),
                 "%d pip install line(s)" % len(installs))
    unsafe = []
    for job in jobs.values():
        for step in job.get("steps", []):
            for expr in re.findall(r"\$\{\{\s*([^}]*?)\s*\}\}", step.get("run", "")):
                if expr not in SAFE_RUN_EXPRESSIONS:
                    unsafe.append(expr)
    report.check("run steps interpolate no untrusted ${{ }} expressions (script-injection guard)", not unsafe, str(unsafe) if unsafe else "")
    report.check("no step calls the flow against live services (no curl/wget/gh api in run steps)",
                 not any(re.search(r"\b(curl|wget|gh api)\b", step.get("run", "")) for job in jobs.values() for step in job.get("steps", [])))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", required=True)
    parser.add_argument("--flows-dir")
    parser.add_argument("--workflow")
    args = parser.parse_args()
    report = Report()
    print("== flow checks")
    check_flow(report, args.flow, args.flows_dir)
    if args.workflow:
        print("== CI workflow checks")
        check_workflow(report, args.workflow)
    print("\n%s: %d check(s) failed" % ("FAILED" if report.failures else "OK", report.failures))
    return 1 if report.failures else 0


if __name__ == "__main__":
    sys.exit(main())
