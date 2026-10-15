#!/usr/bin/env python3
"""Report why a Spark app on YARN may get no resources, in plain words.

The report starts with the findings: each says what is wrong (or fine), what happens because of
it, and how to fix it. Then it shows what one Spark app asks YARN for, the queues, the nodes and
the settings behind the findings, with a short list of the terms at the end.

Run it on the primary node of an Amazon EMR cluster (or on a Hadoop edge node):

    python3 yarn_spark_settings.py              # config files + live data on EMR
    python3 yarn_spark_settings.py --no-live    # config files only
    python3 yarn_spark_settings.py --live --rm http://<resourcemanager-host>:8088
    python3 yarn_spark_settings.py --all        # also list every other property in the files
    python3 yarn_spark_settings.py --color never  # no colour (it is on only on a terminal anyway)
    python3 yarn_spark_settings.py --watch 30   # queues and apps, redrawn every 30 s (like yarn top)
    python3 yarn_spark_settings.py --queue etl  # first a one-screen answer: can a job run in queue etl?

The script needs Python 3.6 or later (EMR 6 and 7 have it) and uses only the standard library.

The script is read-only. It reads the config files and sends only HTTP GET requests to the
ResourceManager REST API (cluster metrics, scheduler, node list and the running and waiting apps),
and to the Spark UI of running Spark apps through the ResourceManager proxy. It does not submit apps,
change queues or settings, start YARN containers or write files.

Run as an EMR step (the output goes to the step's stdout log). The step streams the script into
python3, so nothing is written on the node. The step runs on the primary node, not in YARN, but
it takes a step slot: with a step concurrency of 1 it waits behind running steps, and other steps
wait for it (it takes a few seconds).

    aws s3 cp yarn_spark_settings.py s3://<bucket>/tools/
    aws emr add-steps --cluster-id j-XXXXXXXX --steps \\
      'Type=CUSTOM_JAR,Name=yarn-settings,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[bash,-c,"aws s3 cp s3://<bucket>/tools/yarn_spark_settings.py - | python3 -"]'
"""  # noqa: E501 - the EMR step command above must stay on one line to copy and paste it

import argparse
import datetime
import http.client
import json
import math
import os
import re
import socket
import ssl
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union, overload

# Python 3.6 has no list[str] or "X | None" in annotations, so the hints use the typing module.
Json = Dict[str, Any]  # one object from the ResourceManager or Spark REST API
Live = Dict[str, Any]  # the live data: base, metrics, queues, tree, nodes, apps
Balance = Dict[str, Any]  # the numbers of one queue's balance sheet (see queue_balance)
User = Dict[str, Any]  # one user's numbers in a queue (see queue_balance and user_in)
Why = Dict[str, Any]  # a plain explanation of one limit: title, what, calc, props, fix
Check = Tuple[bool, str, Why]  # one check before an app starts: ok, short text, explanation
Role = Union[None, str, Callable[[str], Optional[str]]]  # a colour role, or a function cell -> role

WIDTH = 100
EMR_INFO_DIR = os.environ.get("EMR_INFO_DIR", "/mnt/var/lib/info")

# --all hides the values of these properties, because the output can go to shared logs
SECRET_NAME = re.compile(r"secret|password|passwd|\.pass$|credential|token$|access\.key|secret\.key", re.I)


# ================================================================== reading files


class ConfigFile:
    """One config file: its path, its properties (or None) and a plain error message."""

    def __init__(self, key: str, path: str, title: str) -> None:
        self.key, self.path, self.title = key, path, title
        self.props: Optional[Dict[str, str]] = None
        self.error: Optional[str] = None
        self.option = ""  # the command-line option that sets the directory, for the error message

    @overload
    def get(self, name: str) -> Optional[str]: ...

    @overload
    def get(self, name: str, default: str) -> str: ...

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        if not self.props:
            return default
        return self.props.get(name, default)

    def has(self, name: str) -> bool:
        return self.props is not None and name in self.props


def _open_error(path: str, title: str, e: OSError) -> str:
    """Turn an OSError from opening a config file into a message that says what to do."""
    if isinstance(e, FileNotFoundError):
        return (
            "%s was not found at %s. If the config is somewhere else, set the directory with "
            "the option shown below or the matching *_CONF_DIR variable." % (title, path)
        )
    if isinstance(e, PermissionError):
        return (
            "No permission to read %s. Run the script as a user that can read it (on EMR, the "
            "hadoop user or a user in the hadoop group), or ask an admin to make it readable." % path
        )
    if isinstance(e, IsADirectoryError):
        return "%s is a directory, not a file." % path
    return "Cannot read %s: %s." % (path, e.strerror or e)


def read_hadoop_xml(cf: ConfigFile) -> None:
    """Fill cf.props from a Hadoop *-site.xml file, or set cf.error."""
    try:
        with open(cf.path, "rb") as f:
            root = ET.parse(f).getroot()
    except OSError as e:
        cf.error = _open_error(cf.path, cf.title, e)
        return
    except ET.ParseError as e:
        cf.error = (
            "%s is not valid XML (%s). The file may be cut off or edited by hand; "
            "check it with: xmllint --noout %s" % (cf.path, e, cf.path)
        )
        return
    props: Dict[str, str] = {}
    for prop in root.iter("property"):
        name = prop.findtext("name")
        if name is None:
            continue
        props[name.strip()] = (prop.findtext("value") or "").strip()
    cf.props = {k: expand_vars(v, props) for k, v in props.items()}


def read_spark_defaults(cf: ConfigFile) -> None:
    """Fill cf.props from spark-defaults.conf, or set cf.error."""
    try:
        with open(cf.path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError as e:
        cf.error = _open_error(cf.path, cf.title, e)
        return
    props: Dict[str, str] = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Java properties format: "key value", "key=value" or "key = value"
        parts = re.split(r"\s*[=:]\s*|\s+", line, maxsplit=1)
        props[parts[0]] = parts[1].strip() if len(parts) > 1 else ""
    cf.props = props


def expand_vars(value: str, props: Dict[str, str], depth: int = 0) -> str:
    """Expand ${name} with other properties or environment variables, like Hadoop does."""
    if depth > 10 or "${" not in value:
        return value

    def repl(m: "re.Match[str]") -> str:
        key = m.group(1)
        if key in props:
            return props[key]
        return os.environ.get(key, m.group(0))

    new = re.sub(r"\$\{([^}]+)\}", repl, value)
    return new if new == value else expand_vars(new, props, depth + 1)


def first_dir(*candidates: Optional[str]) -> str:
    for c in candidates:
        if c and os.path.isdir(c):
            return c
    return candidates[-1] or ""


def read_json_file(path: str, denied: List[str]) -> Optional[Json]:
    """Return the parsed JSON file, or None. Add the path to denied when we may not read it."""
    try:
        with open(path) as f:
            data = json.load(f)
    except PermissionError:
        denied.append(path)
        return None
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def read_emr_info() -> Optional[Json]:
    """Return what EMR says about this cluster and this node, or None if this is not an EMR node."""
    denied: List[str] = []
    instance = read_json_file(os.path.join(EMR_INFO_DIR, "instance.json"), denied)
    job_flow = read_json_file(os.path.join(EMR_INFO_DIR, "job-flow.json"), denied)
    extra = read_json_file(os.path.join(EMR_INFO_DIR, "extraInstanceData.json"), denied)
    if instance is None and job_flow is None and not denied:  # (files we may not read: still an EMR node)
        return None
    job_flow, extra, instance = job_flow or {}, extra or {}, instance or {}
    groups = [
        "%s: %s x %s"
        % (g.get("instanceRole", "?").lower(), g.get("requestedInstanceCount", "?"), g.get("instanceType", "?"))
        for g in job_flow.get("instanceGroups") or []
    ]
    return {
        "id": job_flow.get("jobFlowId", "?"),
        "release": extra.get("releaseLabel", ""),
        "groups": ", ".join(groups),
        "is_primary": instance.get("isMaster"),
        "error": (
            "No permission to read the EMR cluster information in %s, so the cluster ID and node "
            "role are unknown. The rest of the report is not affected." % EMR_INFO_DIR
        )
        if denied
        else None,
    }


# ================================================================== helpers


def to_mb(text: Any) -> Optional[int]:
    """Convert a Spark memory string (512m, 4g, 2048) to MB. Return None if unknown."""
    if text is None:
        return None
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*", str(text).lower())
    if not m:
        return None
    num, unit = float(m.group(1)), m.group(2)
    factor = {"k": 1.0 / 1024, "": 1.0, "m": 1.0, "g": 1024.0, "t": 1024.0 * 1024}[unit]
    # Spark reads a plain number for *.memory as MB
    return int(num * factor)


def to_float(text: Any) -> Optional[float]:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def mb(n: Optional[float]) -> str:
    return "-" if n is None else f"{int(n):,} MB"


def pct(fraction: Optional[float]) -> str:
    return "-" if fraction is None else "%.0f%%" % (fraction * 100)


def wrap(text: str, indent: str = "", later: Optional[str] = None) -> str:
    later = indent if later is None else later
    return textwrap.fill(
        text,
        width=WIDTH,
        initial_indent=indent,
        subsequent_indent=later,
        break_on_hyphens=False,
        break_long_words=False,
    )


# ================================================================== colour
# Colour is added after wrapping and padding, so it never changes the layout. It is on only when
# the output goes to a terminal (not to a file, a pipe or an EMR step log), unless --color says so.

_SGR = {
    "bold": "1",
    "dim": "2",
    "under": "4",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
}
# What each part of the report looks like. Explanations keep the terminal's own text colour,
# which reads well on dark and light themes.
ROLES = {
    "title": ("bold",),
    "section": ("bold", "blue"),
    "rule": ("dim",),
    "label": ("bold",),
    "file": ("cyan",),
    "prop": ("magenta",),
    "value": ("bold",),
    "default": ("dim",),
    "queue": ("bold",),
    "url": ("cyan", "under"),
    "PROBLEM": ("bold", "red"),
    "WARNING": ("red",),
    "NOTE": ("blue",),
    "OK": ("green",),
    "fix": ("bold", "green"),
    "error": ("bold", "red"),
    "good": ("green",),
    "bad": ("red",),
    # the frame around each group of sibling queues; groups next to each other get other colours
    "group0": ("cyan",),
    "group1": ("yellow",),
    "group2": ("magenta",),
    "group3": ("green",),
    "group4": ("blue",),
}
GROUP_ROLES = ["group0", "group1", "group2", "group3", "group4"]
COLOR = False


def use_color(choice: str) -> None:
    """Decide once whether to colour the output: auto, always or never."""
    global COLOR
    if choice == "always" or (choice == "auto" and os.environ.get("FORCE_COLOR")):
        COLOR = True
    elif choice == "never" or os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        COLOR = False
    else:
        COLOR = sys.stdout.isatty()


def paint(text: str, role: Optional[str]) -> str:
    """Return text in the style of a role, or unchanged when colour is off."""
    if not COLOR or not text or role not in ROLES:
        return text
    return "\033[%sm%s\033[0m" % (";".join(_SGR[n] for n in ROLES[role]), text)


# Inside explanations: config files and paths, property names, queues and memory values
_TOKENS = re.compile(
    r"(?P<url>https?://[^\s,;)]+[^\s,;.)])"
    r"|(?P<file>(?<![\w.])(?:/[\w.\-]+)+/?|\b[\w.\-]+\.(?:xml|conf|json)\b)"
    r"|(?P<prop>\b(?:yarn|spark|oozie|hadoop|mapreduce|fs)\.[\w.\-]*\w)"
    r"|(?P<queue>\broot(?:\.[\w\-]+)+)"
    r"|(?P<value>\b\d[\d,]*(?:\.\d+)? MB\b)"
)


def highlight(text: str) -> str:
    """Colour the file names, property names, queues and MB values inside a text."""
    if not COLOR:
        return text
    return _TOKENS.sub(lambda m: paint(m.group(0), m.lastgroup), text)


def section(title: str, note: str = "") -> None:
    print()
    print(paint(title.upper(), "section"))
    print(paint("-" * len(title), "rule"))
    if note:
        print(highlight(wrap(note)))
        print()


def _box_chars() -> Tuple[Dict[str, str], str, str]:
    """Frame characters and the separator dot: box drawing when the terminal can show them, else plain ASCII."""
    try:
        "┌─│└✓✗−◀·•".encode(sys.stdout.encoding or "ascii")
        return (
            {"top": "┌", "line": "─", "side": "│", "bottom": "└", "no": "✗", "minus": "−", "arrow": "◀", "bullet": "•"},
            "✓",
            "·",
        )
    except (UnicodeEncodeError, LookupError):
        return (
            {"top": "+", "line": "-", "side": "|", "bottom": "+", "no": "X", "minus": "-", "arrow": "<", "bullet": "*"},
            "OK",
            "-",
        )


BOX, MARK_OK, DOT = _box_chars()


def _bar_chars() -> Dict[str, Any]:
    """Characters for the gauges: eighths of a block when the terminal can show them, else ASCII."""
    try:
        "█▏▎▍▌▋▊▉░".encode(sys.stdout.encoding or "ascii")
        return {"full": "█", "parts": ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"], "empty": "░"}
    except (UnicodeEncodeError, LookupError):
        return {"full": "#", "parts": ["", "", "", "", "#", "#", "#", "#"], "empty": "."}


BAR = _bar_chars()


def column_widths(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[int]:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(str(c)))
    return widths


def table_row(cells: Sequence[Any], widths: Sequence[int], roles: Sequence[Role]) -> str:
    """One aligned table line, without the indent. roles: one role (or a function cell -> role) per column."""
    out = []
    for cell, width, role in zip(cells, widths, roles):
        text = str(cell)
        name = role(text) if callable(role) else role
        out.append(paint(text, name) + " " * (width - len(text)))
    return "  ".join(out)


def table(
    headers: Sequence[str], rows: Sequence[Sequence[Any]], indent: str = "  ", roles: Optional[Sequence[Role]] = None
) -> None:
    """Print an aligned table. roles: one role (or a function cell -> role) per column."""
    widths = column_widths(headers, rows)
    print((indent + table_row(headers, widths, ["label"] * len(headers))).rstrip())
    roles = roles or [None] * len(headers)
    for r in rows:
        print((indent + table_row(r, widths, roles)).rstrip())


# ================================================================== the ResourceManager


def rm_urls(yarn: ConfigFile, given: Optional[str]) -> List[str]:
    """Return the ResourceManager web addresses to try, in order."""
    if given:
        url = given.rstrip("/")
        return [url if "://" in url else "http://" + url]
    https = (yarn.get("yarn.http.policy") or "HTTP_ONLY").upper() == "HTTPS_ONLY"
    key = "yarn.resourcemanager.webapp.https.address" if https else "yarn.resourcemanager.webapp.address"
    port = "8090" if https else "8088"
    scheme = "https://" if https else "http://"
    addrs: List[str] = []
    ids = [i.strip() for i in (yarn.get("yarn.resourcemanager.ha.rm-ids") or "").split(",") if i.strip()]
    if (yarn.get("yarn.resourcemanager.ha.enabled") or "false").lower() == "true" and ids:
        for rm_id in ids:
            addr = yarn.get("%s.%s" % (key, rm_id))
            if not addr and yarn.get("yarn.resourcemanager.hostname.%s" % rm_id):
                addr = "%s:%s" % (yarn.get("yarn.resourcemanager.hostname.%s" % rm_id), port)
            if addr:
                addrs.append(addr)
    addr = yarn.get(key)
    if addr and not addr.startswith("0.0.0.0"):
        addrs.append(addr)
    addrs.append("%s:%s" % (yarn.get("yarn.resourcemanager.hostname", "localhost"), port))
    seen: Set[str] = set()
    result: List[str] = []
    for a in addrs:
        if a not in seen:
            seen.add(a)
            result.append(scheme + a)
    return result


class RMError(Exception):
    """A ResourceManager request failed. The message says why and what to do."""


# The ResourceManager is inside the VPC: never send these requests through an HTTP proxy.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _curl_negotiate(url: str) -> bytes:
    """GET with curl --negotiate (Kerberos/SPNEGO). Return the parsed JSON, or raise RMError."""
    try:
        out = subprocess.run(
            [
                "curl",
                "-sS",
                "--fail",
                "--noproxy",
                "*",
                "--negotiate",
                "-u",
                ":",
                "-H",
                "Accept: application/json",
                url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except FileNotFoundError:
        raise RMError(
            "The ResourceManager asks for Kerberos login (HTTP 401), and curl is not "
            "installed to log in with Kerberos. Install curl, run kinit, and try again."
        ) from None
    except subprocess.TimeoutExpired:
        raise RMError("The Kerberos login with curl did not answer within 20 seconds.") from None
    if out.returncode != 0:
        raise RMError(
            "The ResourceManager asks for Kerberos login (HTTP 401) and the login failed "
            "(%s). Run kinit (check with klist) and try again." % out.stderr.decode("utf-8", "replace").strip()
        )
    return out.stdout


def get_json(url: str, timeout: float = 15) -> Any:
    """GET a JSON document (read-only). Raise RMError with a plain message on failure."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with _OPENER.open(req, timeout=timeout) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 401:
            body = _curl_negotiate(url)
        elif e.code == 403:
            raise RMError(
                "The ResourceManager refused access (HTTP 403). The user running the script "
                "has no permission to read the REST API (yarn.admin.acl or the web UI ACLs)."
            ) from None
        elif e.code == 404:
            raise RMError(
                "HTTP 404 for %s. The address may not be a ResourceManager web address "
                "(port 8088, or 8090 with HTTPS)." % url
            ) from None
        else:
            raise RMError("The ResourceManager answered HTTP %s %s." % (e.code, e.reason)) from None
    except urllib.error.URLError as e:
        reason = e.reason
        if isinstance(reason, ConnectionRefusedError):
            raise RMError(
                "Connection refused. Nothing listens on that host and port: the ResourceManager "
                "may be down, or the port is wrong (8088, or 8090 with HTTPS)."
            ) from None
        if isinstance(reason, socket.gaierror):
            raise RMError(
                "Unknown host name. Check the host in --rm or in yarn-site.xml (yarn.resourcemanager.hostname)."
            ) from None
        if isinstance(reason, (socket.timeout, TimeoutError)):
            raise RMError(
                "No answer within 15 seconds. A firewall or security group probably blocks the "
                "port. On EMR, allow this host on port 8088 in the primary node's security group."
            ) from None
        if isinstance(reason, ssl.SSLError):
            raise RMError(
                "HTTPS failed (%s). The ResourceManager certificate may not be trusted by this host." % reason
            ) from None
        raise RMError("Cannot connect: %s." % reason) from None
    except (socket.timeout, TimeoutError):
        raise RMError("The ResourceManager stopped answering after 15 seconds.") from None
    except (http.client.HTTPException, ConnectionError) as e:
        raise RMError("The connection broke: %s." % e) from None
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:
        text = body.decode("utf-8", "replace")
        if "standby" in text.lower():
            raise RMError("This ResourceManager is the standby one. Give the active one with --rm.") from None
        raise RMError(
            "The answer is not JSON. The address may point to another web server, or to a login page of a proxy."
        ) from None


def walk_rm_tree(q: Json, path: str, out: Dict[str, Json]) -> None:
    """Every queue of the live data, parents too: path -> the queue from the REST API."""
    path = q.get("queuePath") or path
    out[path] = q
    children = (q.get("queues") or {}).get("queue")
    if isinstance(children, dict):  # some Hadoop versions send one child queue as an object
        children = [children]
    for c in children or []:  # older Hadoop has no queuePath: build the path from the parent
        walk_rm_tree(c, "%s.%s" % (path, c.get("queueName", "?")), out)


def fetch_live(bases: Iterable[str]) -> Tuple[Optional[Live], List[str]]:
    """Return the live data from the first ResourceManager that answers, plus the errors on the way."""
    errors: List[str] = []
    for base in bases:
        try:
            metrics = get_json(base + "/ws/v1/cluster/metrics")["clusterMetrics"]
            sched = get_json(base + "/ws/v1/cluster/scheduler")["scheduler"]["schedulerInfo"]
            nodes = (get_json(base + "/ws/v1/cluster/nodes").get("nodes") or {}).get("node") or []
            if isinstance(nodes, dict):  # one node sent as an object
                nodes = [nodes]
            tree: Dict[str, Json] = {}
            if sched.get("type") == "capacityScheduler":
                walk_rm_tree(sched, "root", tree)
            else:
                errors.append(
                    "%s: the scheduler is %s, not the Capacity Scheduler. The queue data is "
                    "skipped." % (base, sched.get("type", "unknown"))
                )
            queues = {p: q for p, q in tree.items() if not (q.get("queues") or {}).get("queue")}  # leaves
        except RMError as e:
            errors.append("%s: %s" % (base, e))
            continue
        except (KeyError, TypeError, AttributeError):
            errors.append("%s: the answer does not look like the ResourceManager REST API." % base)
            continue
        apps = fetch_apps(base, errors)
        return {"base": base, "metrics": metrics, "queues": queues, "tree": tree, "nodes": nodes, "apps": apps}, errors
    return None, errors


SPARK_STAGE_LOOKUPS = 20  # ask the Spark UI of at most this many running Spark apps


def fetch_apps(base: str, errors: List[str]) -> Optional[List[Json]]:
    """Running and waiting apps, each with what it does now. None if the list cannot be read."""
    try:
        data = get_json(base + "/ws/v1/cluster/apps?states=RUNNING,ACCEPTED")
        apps = ((data or {}).get("apps") or {}).get("app") or []
    except (RMError, AttributeError) as e:
        errors.append("%s: could not list the apps (%s). The Apps section is skipped." % (base, e))
        return None
    if isinstance(apps, dict):  # one app sent as an object
        apps = [apps]
    spark_running = [
        a for a in apps if a.get("state") == "RUNNING" and str(a.get("applicationType", "")).upper() == "SPARK"
    ]
    for a in spark_running[:SPARK_STAGE_LOOKUPS]:
        a["_stages"] = spark_stages(base, a.get("id", ""))
        if waiting_tasks(a) and not pending_request(a)[0]:
            a["_spark_conf"] = spark_limits(base, a.get("id", ""))
    return apps


def spark_limits(base: str, app_id: str) -> Optional[Dict[str, str]]:
    """The executor limits that a running Spark app uses, from its UI. None if the UI does not answer."""
    url = "%s/proxy/%s/api/v1/applications/%s/environment" % (base, app_id, app_id)
    try:
        env = get_json(url, timeout=5)
        props = dict((k, v) for k, v in env.get("sparkProperties") or [])
    except Exception:  # noqa: BLE001 - this extra detail must never stop the report
        return None
    keys = ("spark.dynamicAllocation.enabled", "spark.dynamicAllocation.maxExecutors", "spark.executor.instances")
    return {k: props[k] for k in keys if k in props}


def pending_request(app: Json) -> Tuple[int, Optional[int]]:
    """What a running app asks YARN for and does not have yet: (containers, MB each).
    (0, None) when it asks for nothing."""
    count, size = 0, None  # type: int, Optional[int]
    for r in app.get("resourceRequests") or []:
        if r.get("resourceName", "*") != "*":  # host and rack requests repeat the "*" one
            continue
        n = int(to_float(r.get("numContainers")) or 0)
        if n > 0:
            count += n
            size = max(size or 0, int(to_float((r.get("capability") or {}).get("memory")) or 0)) or None
    if not count:  # Hadoop 3 also gives the pending memory per partition
        for part in (app.get("resourceInfo") or {}).get("resourceUsagesByPartition") or []:
            pend = int(to_float((part.get("pending") or {}).get("memory")) or 0)
            if pend > 0:
                return -1, pend  # -1: the number of containers is unknown, pend is the total MB
    return count, size


def waiting_tasks(app: Json) -> int:
    """Tasks of the active Spark stages that wait for a free executor core."""
    return sum(
        max(0, int(st.get("numTasks", 0)) - int(st.get("numCompleteTasks", 0)) - int(st.get("numActiveTasks", 0)))
        for st in app.get("_stages") or []
    )


def spark_stages(base: str, app_id: str) -> Optional[List[Json]]:
    """The active stages of a running Spark app, from its UI through the ResourceManager proxy.
    None if the UI does not answer (for example a driver on a host that the proxy cannot reach)."""
    url = "%s/proxy/%s/api/v1/applications/%s/stages?status=active" % (base, app_id, app_id)
    try:
        stages = get_json(url, timeout=5)
    except (RMError, Exception):  # noqa: BLE001 - this extra detail must never stop the report
        return None
    return stages if isinstance(stages, list) else None


# ================================================================== the model


class Queue:
    def __init__(self, path: str) -> None:
        self.path = path
        self.abs_cap: Optional[float] = None  # guaranteed share of the cluster, 0..1
        self.abs_max: Optional[float] = None  # maximum share of the cluster, 0..1
        self.ulf = 1.0  # user-limit-factor
        self.min_user_pct = 100.0  # minimum-user-limit-percent
        self.am_pct: Optional[float] = None  # maximum-am-resource-percent, 0..1
        self.state = "RUNNING"
        self.max_alloc_mb: Optional[float] = None
        self.ordering: Optional[str] = None  # ordering-policy: fifo (default) or fair
        self.live: Optional[Json] = None  # the queue from the REST API

    @property
    def one_user_max(self) -> Optional[float]:
        if self.abs_cap is None or self.abs_max is None:
            return None
        if self.ulf <= 0:  # -1 means no user limit (Hadoop 3.3+)
            return self.abs_max
        return min(self.ulf * self.abs_cap, self.abs_max)

    def live_mb(self, key: str) -> Optional[float]:
        if not self.live:
            return None
        return (self.live.get(key) or {}).get("memory")


def build_queues(cs: ConfigFile, live: Optional[Live]) -> Dict[str, Queue]:
    """Leaf queues from capacity-scheduler.xml, completed (or replaced) by the live data."""
    queues: Dict[str, Queue] = {}
    global_am = to_float(cs.get("yarn.scheduler.capacity.maximum-am-resource-percent", "0.1"))

    def walk(path: str, abs_cap: Optional[float], abs_max: Optional[float]) -> None:
        children = cs.get("yarn.scheduler.capacity.%s.queues" % path)
        if children:
            names = [c.strip() for c in children.split(",") if c.strip()]
            raws = [(cs.get("yarn.scheduler.capacity.%s.%s.capacity" % (path, n)) or "").strip() for n in names]
            weights = [to_float(r[:-1]) if r.lower().endswith("w") else None for r in raws]
            caps: List[Optional[float]]
            known = [w for w in weights if w is not None]
            if weights and len(known) == len(weights):  # weights (2w, 1w): each child gets weight / sum
                caps = [w * 100.0 / (sum(known) or 1.0) for w in known]
            else:
                caps = [to_float(r) for r in raws]
            for child, cap in zip(names, caps):
                p = "%s.%s" % (path, child)
                maxcap = to_float(cs.get("yarn.scheduler.capacity.%s.maximum-capacity" % p, "100"))
                if maxcap is not None and maxcap < 0:  # -1 means 100
                    maxcap = 100.0
                walk(
                    p,
                    abs_cap * cap / 100.0 if (abs_cap is not None and cap is not None) else None,
                    abs_max * maxcap / 100.0 if (abs_max is not None and maxcap is not None) else None,
                )
            return
        q = Queue(path)
        q.abs_cap, q.abs_max = abs_cap, abs_max
        prefix = "yarn.scheduler.capacity.%s." % path
        q.ulf = to_float(cs.get(prefix + "user-limit-factor", "1")) or 1.0
        q.min_user_pct = to_float(cs.get(prefix + "minimum-user-limit-percent", "100")) or 100.0
        q.am_pct = to_float(cs.get(prefix + "maximum-am-resource-percent")) or global_am
        q.state = cs.get(prefix + "state", "RUNNING").upper()
        q.max_alloc_mb = to_float(cs.get(prefix + "maximum-allocation-mb"))
        q.ordering = cs.get(prefix + "ordering-policy")
        queues[path] = q

    if cs.props:
        walk("root", 1.0, 1.0)
    for path, lq in ((live or {}).get("queues") or {}).items():
        q = queues.get(path)
        if q is None:  # no capacity-scheduler.xml here (edge node): take it from the live data
            q = queues[path] = Queue(path)
            q.abs_cap = (lq.get("absoluteCapacity") or 0) / 100.0
            q.abs_max = (lq.get("absoluteMaxCapacity") or 0) / 100.0
            q.ulf = to_float(lq.get("userLimitFactor")) or 1.0
            q.min_user_pct = to_float(lq.get("userLimit")) or 100.0
            q.am_pct = to_float(lq.get("maxAMResourcePercentage")) if "maxAMResourcePercentage" in lq else None
            q.state = (lq.get("state") or "RUNNING").upper()
        q.live = lq
        if not q.ordering and lq.get("orderingPolicyInfo"):
            q.ordering = str(lq.get("orderingPolicyInfo"))
    return queues


def find_queue(queues: Dict[str, Queue], name: str) -> Optional[Queue]:
    """Find the leaf queue that an app or launcher names ("default" or "root.default")."""
    if name in queues:
        return queues[name]
    if "root." + name in queues:
        return queues["root." + name]
    matches = [q for p, q in queues.items() if p.endswith("." + name)]
    return matches[0] if len(matches) == 1 else None


class Container:
    def __init__(
        self,
        name: str,
        heap: int,
        heap_note: str,
        overhead: int,
        overhead_note: str,
        other: int,
        other_note: str,
        vcores: int,
        min_alloc: float,
    ) -> None:
        self.name, self.heap, self.heap_note = name, heap, heap_note
        self.overhead, self.overhead_note = overhead, overhead_note
        self.other, self.other_note, self.vcores = other, other_note, vcores
        self.total = heap + overhead + other
        # YARN rounds each request up to a multiple of the smallest container
        self.size = int(math.ceil(self.total / float(min_alloc)) * min_alloc)


def spark_containers(spark: ConfigFile, min_alloc: float) -> Tuple[Container, Container, str]:
    """What one Spark app asks YARN for: (executor, AM, deploy mode)."""
    deploy = spark.get("spark.submit.deployMode", "client")

    def overhead(key: str, factor_key: Optional[str], heap: int) -> Tuple[int, str]:
        if spark.get(key):
            return to_mb(spark.get(key)) or 0, "set (%s)" % key
        factor = (to_float(spark.get(factor_key, "0.10")) or 0.10) if factor_key else 0.10
        return max(384, int(heap * factor)), "%.0f%% of heap, at least 384 MB" % (factor * 100)

    # Executor. On EMR, spark.emr.default.executor.* apply when spark.executor.* are not set.
    if spark.get("spark.executor.memory"):
        heap, note = to_mb(spark.get("spark.executor.memory")) or 1024, "spark.executor.memory"
    elif spark.get("spark.emr.default.executor.memory"):
        heap, note = to_mb(spark.get("spark.emr.default.executor.memory")) or 1024, "EMR default for the instance type"
    else:
        heap, note = 1024, "Spark default"
    ovh, ovh_note = overhead("spark.executor.memoryOverhead", "spark.executor.memoryOverheadFactor", heap)
    other, other_notes = 0, []  # type: int, List[str]
    if spark.get("spark.executor.pyspark.memory"):
        other += to_mb(spark.get("spark.executor.pyspark.memory")) or 0
        other_notes.append("Python workers")
    if spark.get("spark.memory.offHeap.enabled", "false").lower() == "true":
        other += to_mb(spark.get("spark.memory.offHeap.size", "0")) or 0
        other_notes.append("off-heap")
    cores = int(to_float(spark.get("spark.executor.cores") or spark.get("spark.emr.default.executor.cores")) or 1)
    executor = Container("Executor", heap, note, ovh, ovh_note, other, ", ".join(other_notes), cores, min_alloc)

    # ApplicationMaster: holds the driver in cluster mode; a small container in client mode
    if deploy == "cluster":
        heap = to_mb(spark.get("spark.driver.memory", "1g")) or 1024
        ovh, ovh_note = overhead("spark.driver.memoryOverhead", "spark.driver.memoryOverheadFactor", heap)
        cores = int(to_float(spark.get("spark.driver.cores")) or 1)
        am = Container(
            "ApplicationMaster (holds the driver)", heap, "spark.driver.memory", ovh, ovh_note, 0, "", cores, min_alloc
        )
    else:
        heap = to_mb(spark.get("spark.yarn.am.memory", "512m")) or 512
        # In client mode Spark uses a fixed factor of 0.10 for the AM, not spark.driver.memoryOverheadFactor
        ovh, ovh_note = overhead("spark.yarn.am.memoryOverhead", None, heap)
        cores = int(to_float(spark.get("spark.yarn.am.cores")) or 1)
        am = Container("ApplicationMaster", heap, "spark.yarn.am.memory", ovh, ovh_note, 0, "", cores, min_alloc)
    return executor, am, deploy


# ================================================================== findings


class Finding:
    ORDER: Dict[str, int] = {"PROBLEM": 0, "WARNING": 1, "NOTE": 2, "OK": 3}

    def __init__(self, level: str, title: str, text: str, fix: Optional[str] = None) -> None:
        self.level, self.title, self.text, self.fix = level, title, text, fix


def find_problems(
    files: Dict[str, ConfigFile],
    queues: Dict[str, Queue],
    live: Optional[Live],
    executor: Container,
    am: Container,
    deploy: str,
) -> List[Finding]:
    yarn, cs, spark, oozie = files["yarn"], files["cs"], files["spark"], files["oozie"]
    out: List[Finding] = []

    def add(level: str, title: str, text: str, fix: Optional[str] = None) -> None:
        out.append(Finding(level, title, text, fix))

    max_alloc = to_float(yarn.get("yarn.scheduler.maximum-allocation-mb", "8192")) or 8192
    max_vcores = to_float(yarn.get("yarn.scheduler.maximum-allocation-vcores", "4")) or 4
    uses_vcores = "dominant" in cs.get("yarn.scheduler.capacity.resource-calculator", "").lower()
    queue_name = spark.get("spark.yarn.queue", "default")
    q = find_queue(queues, queue_name) if queues else None
    nodes = [n for n in (live or {}).get("nodes", []) if n.get("state") == "RUNNING"]
    who = (
        "client mode: a small AM; the driver runs where you start spark-submit"
        if deploy != "cluster"
        else "cluster mode: the driver runs inside the AM"
    )

    # ---- the queue that Spark uses
    if queues and q is None:
        add(
            "PROBLEM",
            'Spark\'s queue "%s" does not exist' % queue_name,
            'spark-defaults.conf sends apps to queue "%s" (spark.yarn.queue), but there is no such leaf '
            "queue. Apps are rejected at submit unless spark-submit gives another queue with --queue." % queue_name,
            "Set spark.yarn.queue to one of: %s." % ", ".join(sorted(queues)),
        )
    if q is not None and q.state != "RUNNING":
        add(
            "PROBLEM",
            "Queue %s is %s" % (q.path, q.state),
            "A stopped queue accepts no new apps. Spark apps sent to it are rejected.",
            "Set yarn.scheduler.capacity.%s.state to RUNNING and refresh the queues." % q.path,
        )

    # ---- one container must fit under YARN's per-container limit
    limit = q.max_alloc_mb if (q is not None and q.max_alloc_mb) else max_alloc
    limit_name = (
        "yarn.scheduler.capacity.%s.maximum-allocation-mb" % q.path
        if (q is not None and q.max_alloc_mb)
        else "yarn.scheduler.maximum-allocation-mb"
    )
    # Without spark-defaults.conf the sizes are only Spark's defaults; without yarn-site.xml the
    # limits are only YARN's defaults. Then the checks would say nothing about this cluster.
    sizes_known = spark.props is not None
    too_big = [c for c in (executor, am) if c.total > limit] if (sizes_known and yarn.props is not None) else []
    for c in too_big:
        add(
            "PROBLEM",
            "The %s container is bigger than YARN allows" % c.name.split(" (")[0].lower(),
            "It needs %s (heap %s + overhead %s%s), but the largest container YARN gives is %s (%s). "
            'spark-submit stops at once with "Required ... memory ... is above the max threshold".'
            % (
                mb(c.total),
                mb(c.heap),
                mb(c.overhead),
                " + %s" % mb(c.other) if c.other else "",
                mb(limit),
                limit_name,
            ),
            "Make the container smaller (%s or its overhead), or raise %s." % (c.heap_note, limit_name),
        )
    if sizes_known and yarn.props is not None and not too_big:
        add(
            "OK",
            "Each Spark container fits under YARN's per-container limit",
            "Executor %s and ApplicationMaster %s; the limit is %s (%s)."
            % (mb(executor.total), mb(am.total), mb(limit), limit_name),
        )
    if uses_vcores and sizes_known and yarn.props is not None:
        for c in (executor, am):
            if c.vcores > max_vcores:
                add(
                    "PROBLEM",
                    "The %s asks for more vcores than YARN allows" % c.name.lower(),
                    "It asks for %d vcores; the most for one container is %d "
                    "(yarn.scheduler.maximum-allocation-vcores). spark-submit fails." % (c.vcores, max_vcores),
                    "Lower the cores (spark.executor.cores / spark.driver.cores), or raise the limit.",
                )

    # ---- an executor must fit on a node
    if nodes:
        sizes = [(n.get("usedMemoryMB") or 0) + (n.get("availMemoryMB") or 0) for n in nodes]
        node_mb, node_src = max(sizes), "the largest node, from the ResourceManager"
    else:
        node_mb = to_float(yarn.get("yarn.nodemanager.resource.memory-mb"))
        node_src = "yarn.nodemanager.resource.memory-mb on this host; other nodes can differ"
    if node_mb and sizes_known:
        per_node = int(node_mb // executor.size)
        if per_node == 0:
            add(
                "PROBLEM",
                "No node is big enough for one executor",
                "An executor takes %s in YARN, but a node gives YARN only %s (%s). YARN accepts the "
                "app, but its executors never start." % (mb(executor.size), mb(node_mb), node_src),
                "Make executors smaller (spark.executor.memory), or use bigger instance types.",
            )
        else:
            text = "A node gives YARN %s (%s) and an executor takes %s, so a node holds %d executor%s." % (
                mb(node_mb),
                node_src,
                mb(executor.size),
                per_node,
                "" if per_node == 1 else "s",
            )
            if nodes:
                text += " The %d running nodes hold about %d executors in all, when nothing else runs." % (
                    len(nodes),
                    sum(int(s // executor.size) for s in sizes),
                )
            add("OK", "Executors fit on the nodes", text)
    if nodes and live:
        free = max(n.get("availMemoryMB") or 0 for n in nodes)
        apps = (live["metrics"].get("appsRunning") or 0) + (live["metrics"].get("appsPending") or 0)
        if free < executor.size and apps:
            add(
                "WARNING",
                "Right now no node has room for another executor",
                "The most free memory on one node is %s; an executor needs %s. New executors wait "
                "until running containers end." % (mb(free), mb(executor.size)),
                "Wait, add nodes (or turn on managed scaling), or make executors smaller.",
            )
    bad = [n for n in (live or {}).get("nodes", []) if n.get("state") not in ("RUNNING", "DECOMMISSIONED")]
    if bad:
        add(
            "WARNING",
            "%d node%s not usable" % (len(bad), " is" if len(bad) == 1 else "s are"),
            ", ".join("%s (%s)" % (n.get("nodeHostName"), n.get("state")) for n in bad)
            + ". YARN does not start containers on these nodes.",
            "Check the NodeManager log and disk space on those nodes.",
        )

    # ---- the AM pool of Spark's queue
    if q is not None:
        am_limit, am_used = q.live_mb("AMResourceLimit"), q.live_mb("usedAMResource")
        pending = (q.live or {}).get("numPendingApplications") or 0
        what = (
            "Only part of a queue may hold ApplicationMasters: %s of queue %s "
            "(maximum-am-resource-percent). Each Spark app has one AM of %s (%s)."
            % (pct(q.am_pct), q.path, mb(am.size), who)
        )
        if am_limit:
            fits = int(am_limit // am.size)
            if fits == 0:
                add(
                    "PROBLEM",
                    "A Spark ApplicationMaster does not fit in queue %s's AM pool" % q.path,
                    what + " The pool is only %s, so apps stay in ACCEPTED." % mb(am_limit),
                    "Raise yarn.scheduler.capacity.%s.maximum-am-resource-percent." % q.path,
                )
            elif pending and am_used is not None and am_used + am.size > am_limit:
                add(
                    "PROBLEM",
                    "Apps wait in queue %s because its AM pool is full" % q.path,
                    what
                    + " The pool is %s and %s is in use, so %d app%s wait%s in ACCEPTED."
                    % (mb(am_limit), mb(am_used), pending, "" if pending == 1 else "s", "s" if pending == 1 else ""),
                    "Raise yarn.scheduler.capacity.%s.maximum-am-resource-percent, or run fewer apps "
                    "at the same time." % q.path,
                )
            else:
                add(
                    "OK" if fits >= 3 else "WARNING",
                    "Queue %s can run about %d Spark app%s at the same time" % (q.path, fits, "" if fits == 1 else "s"),
                    what + " The pool is %s now, so about %d apps fit; more wait in ACCEPTED." % (mb(am_limit), fits),
                    None if fits >= 3 else "Raise yarn.scheduler.capacity.%s.maximum-am-resource-percent." % q.path,
                )
        elif q.am_pct is not None and q.abs_cap is not None and q.abs_max is not None:
            add(
                "NOTE",
                "The AM pool of queue %s is %s to %s of the cluster"
                % (q.path, pct(q.abs_cap * q.am_pct), pct(q.abs_max * q.am_pct)),
                what + " The pool grows from the queue's guaranteed share towards its maximum when "
                "other queues leave room. Run with --live to see it in MB and how many apps fit.",
            )

    # ---- Oozie launchers in the same queue as the Spark AMs
    if oozie.props is not None and q is not None:
        launcher_mb = to_float(oozie.get("oozie.launcher.default.memory.mb", "2048")) or 2048
        lq = find_queue(queues, oozie.get("oozie.launcher.default.queue", "default"))
        if lq is q:
            am_limit = q.live_mb("AMResourceLimit")
            per_wf = launcher_mb + am.size
            text = (
                "If you run Spark from Oozie workflows: each Oozie Spark action keeps a launcher of %s running "
                "in YARN until its Spark job ends, and the Spark job adds its own AM of %s. Both count against "
                "queue %s's AM pool." % (mb(launcher_mb), mb(am.size), q.path)
            )
            if am_limit:
                n = int(am_limit // per_wf)
                text += (
                    " The pool (%s) holds about %d such workflows at once. If more start together, "
                    "launchers can fill the pool, so their Spark jobs never start (a deadlock)." % (mb(am_limit), n)
                )
                level = "WARNING" if n <= 4 else "NOTE"
            else:
                text += (
                    " If many workflows start together, launchers can fill the pool, so their Spark "
                    "jobs never start (a deadlock)."
                )
                level = "NOTE"
            user_am = ((q.live or {}).get("userAMResourceLimit") or {}).get("memory")
            if user_am and per_wf > user_am:
                add(
                    "PROBLEM",
                    "A Spark action from Oozie can never start in queue %s" % q.path,
                    "When the launcher and the Spark job run as the same user (the usual case), the "
                    "launcher (%s) and the Spark AM (%s) both count against that user's AM limit in queue %s: "
                    "%s together, but the limit is %s. The launcher starts, and the Spark AM then waits in "
                    'ACCEPTED for good. See "Can a new Spark app start here now?" under Queue room in numbers.'
                    % (mb(launcher_mb), mb(am.size), q.path, mb(per_wf), mb(user_am)),
                    "Send the launchers to another queue (oozie.launcher.default.queue), or raise "
                    "yarn.scheduler.capacity.%s.maximum-am-resource-percent (or its user-limit-factor, which "
                    "also raises the user AM limit)." % q.path,
                )
            add(
                level,
                "Oozie launchers and Spark apps share queue %s" % q.path,
                text,
                "Send the launchers to another queue: oozie.launcher.default.queue in oozie-site.xml, or "
                "<launcher><queue> in the workflow action."
                if len(queues) > 1
                else "Create a small queue for the launchers and set oozie.launcher.default.queue to it.",
            )
        elif lq is not None:
            add(
                "OK",
                "Oozie launchers use their own queue",
                "Launchers run in %s and Spark apps in %s, so launchers cannot block the Spark "
                "ApplicationMasters." % (lq.path, q.path),
            )

    # ---- the user limit of each queue
    for path in sorted(queues):
        qq = queues[path]
        one = qq.one_user_max
        if one is None or qq.abs_max is None or one >= qq.abs_max - 1e-9:
            continue
        need = qq.abs_max / qq.abs_cap if qq.abs_cap else None
        add(
            "WARNING" if qq is q else "NOTE",
            "In queue %s one user can use only %s of the cluster" % (path, pct(one)),
            "The queue can grow to %s of the cluster when other queues are idle, but user-limit-factor "
            "%.3g stops one user at %.3g x the queue's guaranteed %s. YARN checks the limit before it "
            "starts each container, so a user can go over it by up to one container. If one user runs "
            "the apps, they wait while part of the cluster stays idle."
            % (pct(qq.abs_max), qq.ulf, qq.ulf, pct(qq.abs_cap)),
            "Set yarn.scheduler.capacity.%s.user-limit-factor to %.2f or more." % (path, need) if need else None,
        )
        if qq.min_user_pct < 100:
            add(
                "NOTE",
                "In queue %s each waiting user gets at least %.0f%% of the queue" % (path, qq.min_user_pct),
                "With several active users, YARN shares the queue between them (minimum-user-limit-percent).",
            )

    # ---- what Spark itself limits
    if spark.get("spark.dynamicAllocation.enabled", "false").lower() == "true":
        if spark.get("spark.dynamicAllocation.maxExecutors"):
            add(
                "NOTE",
                "Spark asks for at most %s executors" % spark.get("spark.dynamicAllocation.maxExecutors"),
                "spark.dynamicAllocation.maxExecutors caps each app. Few executors can be this limit, not YARN.",
            )
    elif spark.get("spark.executor.instances"):
        add(
            "NOTE",
            "Spark asks for exactly %s executors" % spark.get("spark.executor.instances"),
            "Dynamic allocation is off, so each app gets spark.executor.instances executors at most.",
        )

    # ---- apps waiting anywhere (live)
    for path, qq in sorted(queues.items()):
        pending = (qq.live or {}).get("numPendingApplications") or 0
        if not pending or qq is q:
            continue
        am_limit, am_used = qq.live_mb("AMResourceLimit"), qq.live_mb("usedAMResource")
        title = "%d app%s waiting in queue %s" % (pending, " is" if pending == 1 else "s are", path)
        if am_limit and am_used is not None and am_used + am.size > am_limit:
            add(
                "PROBLEM",
                title + " because its AM pool is full",
                "The queue's AM pool is %s and %s of it is in use, so a new ApplicationMaster (%s for a "
                "Spark app) does not fit. The apps stay in ACCEPTED until a running app ends."
                % (mb(am_limit), mb(am_used), mb(am.size)),
                "Raise yarn.scheduler.capacity.%s.maximum-am-resource-percent, or run fewer apps at the "
                "same time in this queue." % path,
            )
        else:
            add(
                "WARNING",
                title,
                "The apps stay in ACCEPTED. Their AM pool has room, so the queue is probably at its "
                'limit (see "One user max" and "Max" in the queue table) or no node has room.',
            )
    # ---- running apps that do not get what they ask for (live)
    starved = (
        [a for a in live.get("apps") or [] if a.get("state") == "RUNNING" and why_no_resources(a, live, queues)]
        if live
        else []
    )
    if starved:
        add(
            "WARNING",
            "%d running app%s do%s not get all the resources %s ask%s for"
            % (
                len(starved),
                "" if len(starved) == 1 else "s",
                "es" if len(starved) == 1 else "",
                "it" if len(starved) == 1 else "they",
                "s" if len(starved) == 1 else "",
            ),
            '%s. The Apps section below says why for each app, under "Why running apps do not get more '
            'resources".' % ", ".join("%s (%s)" % (a.get("id"), a.get("name")) for a in starved[:5]),
            None,
        )

    # ---- apps that wait because one user's AM limit is full (live)
    blocked: Dict[Tuple[str, str], List[Json]] = {}
    for a in (live or {}).get("apps") or []:
        if a.get("state") == "ACCEPTED" and "user's am resource limit exceeded" in (a.get("diagnostics") or "").lower():
            blocked.setdefault((str(a.get("user", "?")), str(a.get("queue", "?"))), []).append(a)
    for (user, qname), group in sorted(blocked.items()):
        add(
            "PROBLEM",
            "%d app%s of user %s wait%s: the user's AM limit is full"
            % (len(group), "" if len(group) == 1 else "s", user, "s" if len(group) == 1 else ""),
            "YARN gives each user only part of a queue's AM pool, and all apps of one user share it. "
            "The running apps of %s in queue %s hold all of it, so its new apps stay in ACCEPTED, even "
            "if the queue has room. A service account that submits many apps meets this limit first." % (user, qname),
            "Raise yarn.scheduler.capacity.<queue>.user-limit-factor for this queue (the user AM limit grows "
            "with it), submit as several users (--proxy-user), or run fewer apps at the same time.",
        )
    return sorted(out, key=lambda f: Finding.ORDER[f.level])


# ================================================================== the report

TERMS = [
    (
        "Container",
        "A share of one node's memory and vcores that YARN gives to a process. Each Spark "
        "executor runs in one container.",
    ),
    (
        "ApplicationMaster (AM)",
        "The first container of each YARN app; it asks YARN for the other "
        "containers. In Spark cluster mode the driver runs in it.",
    ),
    (
        "AM pool",
        "The part of a queue that ApplicationMasters may use (maximum-am-resource-percent). "
        "When it is full, new apps wait in ACCEPTED even if the cluster has free memory.",
    ),
    ("Overhead", "Memory that Spark adds to the heap for the JVM itself, threads and buffers."),
    (
        "Guaranteed / Max",
        "A queue always gets its guaranteed share; it can borrow up to its maximum when other queues are idle.",
    ),
    (
        "One user",
        "The most of the cluster that one user's apps can use in the queue (user-limit-factor x guaranteed share).",
    ),
    (
        "Oozie launcher",
        "A small YARN app that Oozie starts for each action; it stays until the action "
        "(for example the Spark job) ends.",
    ),
]


def print_findings(findings: List[Finding]) -> None:
    counts: Dict[str, int] = {}
    for f in findings:
        counts[f.level] = counts.get(f.level, 0) + 1
    parts = [
        "%d %s%s" % (counts[lv], "OK" if lv == "OK" else lv.lower(), "" if counts[lv] == 1 or lv == "OK" else "s")
        for lv in ("PROBLEM", "WARNING", "NOTE", "OK")
        if counts.get(lv)
    ]
    if not findings:
        section("Findings", "No findings: the files that the checks need could not be read (see above).")
        return
    section("Findings")
    levels = [lv for lv in ("PROBLEM", "WARNING", "NOTE", "OK") if counts.get(lv)]
    print("Summary: " + ", ".join(paint(p, lv) for p, lv in zip(parts, levels)) + ".")
    print()
    for f in findings:
        tag = "[%s]" % f.level
        lines = wrap(f.title, " " * 10, " " * 10).split("\n")
        role = "PROBLEM" if f.level in ("PROBLEM", "WARNING") else "title"  # bad news reads in red
        print(paint(tag, f.level) + " " * (10 - len(tag)) + paint(lines[0].strip(), role))
        for line in lines[1:]:
            print(" " * 10 + paint(line.strip(), role))
        print(highlight(wrap(f.text, " " * 10)))
        if f.fix:
            fix = highlight(wrap("What to do: " + f.fix, " " * 10))
            print(fix.replace("What to do:", paint("What to do:", "fix"), 1))
        print()


def print_containers(executor: Container, am: Container, deploy: str, spark: ConfigFile) -> None:
    section(
        "What one Spark app asks YARN for",
        "From spark-defaults.conf (deploy mode: %s). Options given to spark-submit (--conf, "
        "--executor-memory, ...) change these for one app." % deploy,
    )
    rows = []
    for c in (executor, am):
        rows.append([c.name, mb(c.heap), mb(c.overhead), mb(c.other) if c.other else "-", mb(c.size), str(c.vcores)])
    table(
        ["Container", "Heap", "Overhead", "Other", "Size in YARN", "vcores"],
        rows,
        roles=["label", None, None, None, "value", None],
    )
    print()
    print(
        highlight(
            wrap(
                "Heap of the executor: %s. Overhead: %s.%s Size in YARN is the total rounded up to a "
                "multiple of yarn.scheduler.minimum-allocation-mb."
                % (
                    executor.heap_note,
                    executor.overhead_note,
                    " Other: %s." % executor.other_note if executor.other_note else "",
                ),
                "  ",
            )
        )
    )
    if spark.get("spark.dynamicAllocation.enabled", "false").lower() == "true":
        count = "between %s and %s (dynamic allocation)" % (
            spark.get("spark.dynamicAllocation.minExecutors", "0"),
            spark.get("spark.dynamicAllocation.maxExecutors", "no limit"),
        )
    else:
        count = "%s (spark.executor.instances)" % spark.get("spark.executor.instances", "2")
    print(highlight(wrap("Number of executors per app: %s." % count, "  ")))


QUEUE_COLUMNS = [
    (
        "Queue",
        "The queue name. (Spark) marks the queue in spark.yarn.queue, where your Spark apps run. "
        "(Oozie) marks the queue of the Oozie launchers.",
    ),
    (
        "Of parent",
        "The capacity of the queue as written in capacity-scheduler.xml: its share of the parent "
        "queue (the frame title names the parent). The children of one parent must add up to 100%; "
        "the line under each frame shows the sum. A weight such as 2w gets weight / sum of weights.",
    ),
    (
        "Guaranteed",
        "The share of the whole cluster that the queue always gets when it has work "
        "(its capacity, times the capacity of each parent queue). If other queues borrowed it, "
        "YARN gives it back as their containers end.",
    ),
    (
        "Max",
        "The most of the cluster that the queue can use (maximum-capacity). Above Guaranteed, it can "
        "only borrow what other queues leave idle, and it gives it back when they need it.",
    ),
    (
        "One user",
        "The most of the cluster that the apps of one user can use together in this queue: "
        "user-limit-factor x Guaranteed, never more than Max. With a factor of 1, one user cannot "
        "borrow above the guaranteed share, even when the cluster is idle.",
    ),
    (
        "Smallest share per user",
        "Not a column: minimum-user-limit-percent in the Settings. When several "
        "users have apps in the queue at the same time, each one gets at least this "
        "part of the queue. 100% (the default) means no sharing rule.",
    ),
    (
        "AM pool used",
        "Each YARN app has one ApplicationMaster (AM): for Spark in cluster mode it holds the "
        "driver, and each Oozie launcher is one too. All AMs of a queue together may use only "
        "part of it (maximum-am-resource-percent). The column shows the AM memory in use, of the "
        "most allowed. When the pool is full, new apps wait in ACCEPTED even if memory is free. "
        "Without live data it shows the range as a share of the cluster.",
    ),
    ("Used", "Memory that the queue's containers use now (live data)."),
    ("Run / Wait", "Apps that run now / apps that wait in ACCEPTED to start (live data)."),
]


def queue_example(q: Queue, am: Container) -> str:
    """A worked example with the numbers of one queue, so the columns are easy to read."""
    if q.abs_cap is not None and q.abs_max is not None and q.abs_max > q.abs_cap + 1e-9:
        text = "Example, queue %s: it always gets %s of the cluster, and can grow to %s when other queues are idle." % (
            q.path,
            pct(q.abs_cap),
            pct(q.abs_max),
        )
    else:
        text = "Example, queue %s: it always gets %s of the cluster and cannot grow above it." % (
            q.path,
            pct(q.abs_cap),
        )
    if q.one_user_max is not None:
        if q.ulf <= 0:
            text += " One user has no limit inside it (user-limit-factor %g)." % q.ulf
        else:
            text += " One user's apps together can use up to %s (user-limit-factor %g x %s)." % (
                pct(q.one_user_max),
                q.ulf,
                pct(q.abs_cap),
            )
            if q.abs_max is not None and q.one_user_max < q.abs_max - 1e-9:
                text += (
                    " So one user alone cannot use the part between %s and %s; that needs a second "
                    "user, or a higher user-limit-factor." % (pct(q.one_user_max), pct(q.abs_max))
                )
    am_limit = q.live_mb("AMResourceLimit")
    if am_limit:
        used = q.live_mb("usedAMResource") or 0
        text += (
            " Its AM pool is %s, with %s in use; one Spark app needs an AM of %s, so about %d apps "
            "fit in all, and %d more can start now."
            % (mb(am_limit), mb(used), mb(am.size), int(am_limit // am.size), max(0, int((am_limit - used) // am.size)))
        )
    elif q.am_pct is not None and q.abs_cap is not None and q.abs_max is not None:
        text += " Its AM pool is %s of the queue, so %s to %s of the cluster; run with --live to see it in MB." % (
            pct(q.am_pct),
            pct(q.abs_cap * q.am_pct),
            pct(q.abs_max * q.am_pct),
        )
    return text


def queue_groups(cs: ConfigFile, live: Optional[Live]) -> List[Tuple[str, str, List[Json]]]:
    """Each parent queue with its children, in tree order: [(parent path, kind, [child, ...])].
    kind is "percent", "weight" or "other" (absolute resources). A child is a dict with path, raw (the
    capacity as written), share (of the parent, 0..1, or None), abs_cap, abs_max, has_children and live."""
    tree = (live or {}).get("tree") or {}
    groups: List[Tuple[str, str, List[Json]]] = []

    def kids(path: str) -> List[str]:
        if cs.props:
            names = cs.get("yarn.scheduler.capacity.%s.queues" % path) or ""
            return ["%s.%s" % (path, n.strip()) for n in names.split(",") if n.strip()]
        children = ((tree.get(path) or {}).get("queues") or {}).get("queue") or []
        if isinstance(children, dict):
            children = [children]
        return [c.get("queuePath") or "%s.%s" % (path, c.get("queueName", "?")) for c in children]

    def walk(path: str, abs_cap: Optional[float], abs_max: Optional[float]) -> None:
        paths = kids(path)
        if not paths:
            return
        children: List[Json] = []
        for p in paths:
            lq = tree.get(p)
            if cs.props:
                raw = (cs.get("yarn.scheduler.capacity.%s.capacity" % p) or "0").strip()
                maxcap = to_float(cs.get("yarn.scheduler.capacity.%s.maximum-capacity" % p, "100"))
            else:
                raw = str(lq.get("capacity", "?")) if lq else "?"
                maxcap = to_float(lq.get("maxCapacity")) if lq else None
            if maxcap is not None and maxcap < 0:  # -1 means 100
                maxcap = 100.0
            children.append({"path": p, "raw": raw, "maxcap": maxcap, "live": lq})
        if all(c["raw"].lower().endswith("w") and to_float(c["raw"][:-1]) is not None for c in children):
            kind = "weight"
            total = sum(to_float(c["raw"][:-1]) or 0.0 for c in children) or 1.0  # all are numbers: checked above
            for c in children:
                c["share"] = (to_float(c["raw"][:-1]) or 0.0) / total
        else:
            kind = "percent" if all(to_float(c["raw"]) is not None for c in children) else "other"
            for c in children:
                share = to_float(c["raw"])
                c["share"] = share / 100.0 if share is not None else None
        for c in children:
            lq = c["live"] or {}
            if not cs.props and lq:
                c["abs_cap"] = (to_float(lq.get("absoluteCapacity")) or 0) / 100.0
                c["abs_max"] = (to_float(lq.get("absoluteMaxCapacity")) or 0) / 100.0
            else:
                c["abs_cap"] = abs_cap * c["share"] if None not in (abs_cap, c["share"]) else None
                c["abs_max"] = abs_max * c["maxcap"] / 100.0 if None not in (abs_max, c["maxcap"]) else None
            c["has_children"] = bool(kids(c["path"]))
        groups.append((path, kind, children))
        for c in children:
            walk(c["path"], c["abs_cap"], c["abs_max"])

    if cs.props or tree:
        walk("root", 1.0, 1.0)
    return groups


def group_sum(kind: str, children: List[Json], parent: str) -> Tuple[str, Optional[str]]:
    """The line under a group of sibling queues: how their shares add up. Returns (text, role)."""
    if kind == "weight":
        return (
            "weights %s: each queue gets its weight / %s of %s"
            % (
                " + ".join(c["raw"] for c in children),
                "%g" % sum(to_float(c["raw"][:-1]) or 0.0 for c in children),
                parent,
            ),
            "good",
        )
    if kind == "other":
        return "capacities in absolute resources: the sum is not checked here", None
    total = sum(c["share"] for c in children) * 100
    text = "%s = %s of %s" % (
        " + ".join("%g%%" % (c["share"] * 100) for c in children),
        "%g%%" % round(total, 2),
        parent,
    )
    if abs(total - 100) < 0.01:
        return text + " " + MARK_OK, "good"
    return text + ", not 100%: check the capacities of these queues", "bad"


def print_terms(pairs: Sequence[Tuple[str, str]]) -> None:
    """Print term / meaning pairs: the meaning in a column next to the term, a blank line after each."""
    width = max(len(term) for term, _ in pairs) + 2
    for term, meaning in pairs:
        # wrap and cut the indent first, colour last: colour codes would shift the cut
        text = wrap(meaning, " " * (width + 2), " " * (width + 2))[width + 2 :]
        print("  " + paint(term.ljust(width), "label") + highlight(text))
        print()


def print_queues(
    queues: Dict[str, Queue],
    groups: List[Tuple[str, str, List[Json]]],
    spark_queue: Optional[Queue],
    launcher_queue: Optional[Queue],
    am_container: Container,
    explain: bool = True,
) -> None:
    section(
        "Queues",
        "How YARN shares the cluster between the queues. Each frame holds the children of one "
        "parent queue; their shares of the parent must add up to 100%."
        + (" The columns are explained below the table." if explain else ""),
    )

    def leaf_row(q: Queue, of_parent: str) -> List[str]:
        roles = []
        if q is spark_queue:
            roles.append("Spark")
        if q is launcher_queue:
            roles.append("Oozie")
        am_limit = q.live_mb("AMResourceLimit")
        if am_limit is not None:
            am = "{:,} of {:,} MB".format(int(q.live_mb("usedAMResource") or 0), int(am_limit))
        elif q.am_pct is not None and q.abs_cap is not None and q.abs_max is not None:
            am = "%s-%s" % (pct(q.abs_cap * q.am_pct), pct(q.abs_max * q.am_pct))
        else:
            am = "-"
        lv = q.live or {}
        name = q.path + (" (%s)" % ", ".join(roles) if roles else "")
        if q.state != "RUNNING":
            name += " " + q.state
        return [
            name,
            of_parent,
            pct(q.abs_cap),
            pct(q.abs_max),
            pct(q.one_user_max),
            am,
            mb(q.live_mb("resourcesUsed")) if q.live else "-",
            str(lv.get("numActiveApplications", "-")),
            str(lv.get("numPendingApplications", "-")),
        ]

    def of_parent(c: Json) -> str:
        if c["share"] is None:
            return c["raw"]
        if c["raw"].lower().endswith("w"):
            return "%s (%s)" % (c["raw"], pct(c["share"]))
        return "%g%%" % (c["share"] * 100)

    headers = ["Queue", "Of parent", "Guaranteed", "Max", "One user", "AM pool used", "Used", "Run", "Wait"]
    roles = ["queue", None, None, None, None, None, None, None, lambda c: "bad" if c not in ("0", "-") else None]
    blocks: List[Tuple[str, Tuple[str, Optional[str]], List[List[str]]]] = []
    shown: Set[str] = set()
    for parent, kind, children in groups:
        rows: List[List[str]] = []
        for c in children:
            q = queues.get(c["path"])
            if q is not None and not c["has_children"]:
                rows.append(leaf_row(q, of_parent(c)))
                shown.add(c["path"])
            else:
                lq = c["live"] or {}
                used = (lq.get("resourcesUsed") or {}).get("memory")
                rows.append(
                    [
                        c["path"] + " (parent)",
                        of_parent(c),
                        pct(c["abs_cap"]),
                        pct(c["abs_max"]),
                        "-",
                        "-",
                        mb(used) if c["live"] else "-",
                        "-",
                        "-",
                    ]
                )
        blocks.append((parent, group_sum(kind, children, parent), rows))
    rest = [leaf_row(queues[p], "-") for p in sorted(queues) if p not in shown]

    widths = column_widths(headers, [r for b in blocks for r in b[2]] + rest)
    inner = sum(widths) + 2 * (len(widths) - 1)
    print(("    " + table_row(headers, widths, ["label"] * len(headers))).rstrip())
    for i, (parent, (total, total_role), rows) in enumerate(blocks):
        frame = GROUP_ROLES[i % len(GROUP_ROLES)]
        title = "%s children of %s " % (BOX["line"], parent)
        print("  " + paint(BOX["top"] + title + BOX["line"] * max(0, inner + 1 - len(title)), frame))
        for r in rows:
            print(("  " + paint(BOX["side"], frame) + " " + table_row(r, widths, roles)).rstrip())
        print("  " + paint(BOX["bottom"] + BOX["line"] + " ", frame) + paint(total, total_role))
    if rest:  # leaf queues of the live data that the configuration here does not list
        print("  " + BOX["top"] + BOX["line"] + " other queues")
        for r in rest:
            print(("  " + BOX["side"] + " " + table_row(r, widths, roles)).rstrip())
        print("  " + BOX["bottom"] + BOX["line"])
    if not explain:
        return
    print()
    print("  " + paint("What the columns mean", "label"))
    print()
    print_terms(QUEUE_COLUMNS)
    example_queue = spark_queue or (queues[sorted(queues)[0]] if queues else None)
    if example_queue is not None:
        print()
        print(highlight(wrap(queue_example(example_queue, am_container), "  ")))


# ================================================================== queue balance sheets


def gb(n: Optional[float]) -> str:
    return "-" if n is None else "%.2f GB" % (n / 1024.0)


def res(d: Any, key: str = "memory") -> Optional[float]:
    """A number from a resource object of the REST API ({"memory": .., "vCores": ..}), or None."""
    return to_float((d or {}).get(key)) if isinstance(d, dict) else None


def effective(lq: Optional[Json], which: str) -> Optional[Json]:
    """effectiveMinResource / effectiveMaxResource of the default partition (Hadoop 3.1+), or None."""
    parts = ((lq or {}).get("capacities") or {}).get("queueCapacitiesByPartition") or []
    if isinstance(parts, dict):
        parts = [parts]
    for p in parts:
        if not p.get("partitionName"):
            r = p.get(which)
            if res(r) is not None:
                return r
    return None


def queue_users(lq: Optional[Json]) -> List[Json]:
    users = ((lq or {}).get("users") or {}).get("user") or []
    return [users] if isinstance(users, dict) else users


def queue_balance(q: Queue, live: Live, am_size: int, executor_size: int) -> Optional[Balance]:
    """All the numbers of one queue's balance sheet, from the live data. None without live data."""
    lq = q.live
    if not lq:
        return None
    m, tree = live["metrics"], live.get("tree") or {}
    total = to_float(m.get("totalMB")) or 0
    total_vc = to_float(m.get("totalVirtualCores")) or 0
    b: Balance = {"q": q, "total": total, "total_vc": total_vc}
    abs_cap = (to_float(lq.get("absoluteCapacity")) or 0) / 100.0
    abs_max = (to_float(lq.get("absoluteMaxCapacity")) or 0) / 100.0
    emin, emax = effective(lq, "effectiveMinResource"), effective(lq, "effectiveMaxResource")
    b["abs_cap"], b["abs_max"] = abs_cap, abs_max
    b["guar"] = (res(emin), res(emin, "vCores")) if emin else (abs_cap * total, abs_cap * total_vc)
    b["max"] = (res(emax), res(emax, "vCores")) if emax else (abs_max * total, abs_max * total_vc)
    if total:  # YARN's effective figures win over the percentages (they differ with absolute capacities)
        b["abs_cap"], b["abs_max"] = b["guar"][0] / total, b["max"][0] / total
    b["used"] = (res(lq.get("resourcesUsed")) or 0, res(lq.get("resourcesUsed"), "vCores") or 0)
    b["left_q"] = (b["max"][0] - b["used"][0], b["max"][1] - b["used"][1])

    # each parent queue caps its children together
    b["parents"] = []
    path = q.path
    while "." in path:
        path = path.rsplit(".", 1)[0]
        pq = tree.get(path)
        if path == "root" or not pq:
            continue
        pmax = (to_float(pq.get("absoluteMaxCapacity")) or 100) / 100.0
        pe = effective(pq, "effectiveMaxResource")
        pmax_mb = (res(pe) if pe else None) or pmax * total
        pmax_vc = (res(pe, "vCores") if pe else None) or pmax * total_vc
        pused = (res(pq.get("resourcesUsed")) or 0, res(pq.get("resourcesUsed"), "vCores") or 0)
        b["parents"].append((path, pmax, (pmax_mb, pmax_vc), pused, (pmax_mb - pused[0], pmax_vc - pused[1])))
    b["free"] = (to_float(m.get("availableMB")) or 0, to_float(m.get("availableVirtualCores")) or 0)
    nodes = [n for n in live.get("nodes") or [] if n.get("state") == "RUNNING"]
    b["nodes_free"] = sorted([to_float(n.get("availMemoryMB")) or 0 for n in nodes], reverse=True)
    b["node_free"] = (b["nodes_free"] or [0])[0]

    options = (
        [("the queue maximum", b["left_q"][0])]
        + [("the maximum of parent %s" % p[0], p[4][0]) for p in b["parents"]]
        + [("the free memory of the cluster", b["free"][0])]
    )
    b["room_parts"] = [
        (name, value, scope)
        for (name, value), scope in zip(options, [q.path] + [p[0] for p in b["parents"]] + ["cluster"])
    ]
    room = min(o[1] for o in options)
    b["room_by"] = " and ".join(name for name, value in options if value - room < 1)
    b["room"] = max(0.0, room)
    b["room_vc"] = max(0.0, min([b["left_q"][1], b["free"][1]] + [p[4][1] for p in b["parents"]]))

    b["am_limit"] = res(lq.get("AMResourceLimit"))
    b["am_used"] = res(lq.get("usedAMResource")) or 0
    b["user_am_limit"] = res(lq.get("userAMResourceLimit"))
    b["am_pct"] = to_float(lq.get("maxAMResourcePercentage"))
    b["am_pct"] = b["am_pct"] / 100.0 if b["am_pct"] is not None and b["am_pct"] > 1 else (b["am_pct"] or q.am_pct)
    b["am_size"], b["executor_size"] = am_size, executor_size

    # the users that have apps in the queue now; YARN counts the user limit per user, not per app
    b["users"] = []
    ulf = to_float(lq.get("userLimitFactor")) or q.ulf  # what the ResourceManager runs with
    b["ulf"] = ulf
    one_user = b["max"][0] if ulf <= 0 else min(ulf * b["guar"][0], b["max"][0])
    # from the user's apps in this queue, for Hadoop versions without per-user numbers
    by_app: Dict[str, float] = {}
    counts: Dict[str, List[int]] = {}
    for a in live.get("apps") or []:
        if find_queue({q.path: q}, str(a.get("queue", ""))) is q:
            user = str(a.get("user", "?"))
            by_app[user] = by_app.get(user, 0) + max(0, to_float(a.get("allocatedMB")) or 0)
            c = counts.setdefault(user, [0, 0])
            c[0 if a.get("state") == "RUNNING" else 1] += 1
    for u in queue_users(lq):
        name = str(u.get("username", "?"))
        used = res(u.get("resourcesUsed"))
        used = by_app.get(name, 0) if used is None else used
        # Not YARN's userResourceLimit: above the queue's guaranteed share YARN recomputes it on every
        # scheduling pass as about "used + one container", so limit - used is always near 0. The ceiling
        # is user-limit-factor x guaranteed, at most the queue maximum.
        limit = one_user
        am_u = res(u.get("AMResourceUsed")) or 0
        am_l = res(u.get("userAMResourceLimit")) or b["user_am_limit"]
        b["users"].append(
            {
                "name": name,
                "used": used,
                "limit": limit,
                "am_used": am_u,
                "am_limit": am_l,
                "apps": u.get("numActiveApplications", counts.get(name, [0, 0])[0]),
                "waiting": u.get("numPendingApplications", counts.get(name, [0, 0])[1]),
            }
        )
    b["one_user"] = one_user
    return b


NEW_USER = "a new user"


def who(user: User) -> str:
    return user["name"] if user["name"] == NEW_USER else "user " + user["name"]


def user_in(b: Balance, name: Optional[str]) -> User:
    """The numbers of one user in a queue; a user without apps there starts fresh."""
    for u in b["users"]:
        if u["name"] == name:
            return u
    return {
        "name": name or NEW_USER,
        "used": 0.0,
        "limit": b["one_user"],
        "am_used": 0.0,
        "am_limit": b["user_am_limit"],
        "apps": 0,
        "waiting": 0,
    }


def room_after(b: Balance, taken: List[Tuple[str, float]]) -> Tuple[float, str]:
    """Room for queue b after earlier steps took memory: taken is [(queue path, MB)]. Memory taken in a queue
    also counts against each of its parents and the cluster. Returns (room, the part that sets it)."""

    def shared(scope: str, path: str) -> bool:
        return scope == "cluster" or path == scope or path.startswith(scope + ".")

    parts = [
        (value - sum(m for path, m in taken if shared(scope, path)), name, scope)
        for name, value, scope in b["room_parts"]
    ]
    room, _, scope = min(parts, key=lambda p: p[0])
    return max(0.0, room), scope


def qprop(q: Queue, name: str) -> str:
    return "yarn.scheduler.capacity.%s.%s" % (q.path, name)


def why_room(b: Balance, scope: str, need: float, what: str) -> Why:
    """Plain explanation when the queue has no room: the queue max, a parent's max or a full cluster.
    An explanation is a dict: title, what (sentences), calc ([(mark, text)], mark "ok", "no" or None),
    props and fix (lists)."""
    q = b["q"]
    if scope == q.path:
        return {
            "title": "Queue %s is at its maximum size" % q.path,
            "what": [
                "A queue can grow above its guaranteed share, but only up to its maximum (maximum-capacity).",
                "Queue %s is at that maximum now." % q.path,
            ],
            "calc": [
                (None, "Queue max: %s of the cluster = %s" % (pct(b["abs_max"]), gb(b["max"][0]))),
                (None, "Used by the apps of the queue: %s" % gb(b["used"][0])),
                ("no", "Left: %s, but the %s needs %s" % (gb(max(0, b["left_q"][0])), what, gb(need))),
            ],
            "props": [qprop(q, "maximum-capacity")],
            "fix": ["Wait until apps in queue %s end." % q.path, "Or raise maximum-capacity of queue %s." % q.path],
        }
    for path, pmax, pm, pused, pleft in b["parents"]:
        if path == scope:
            return {
                "title": "Parent queue %s is at its maximum size" % path,
                "what": [
                    "All queues under %s share its maximum (maximum-capacity)." % path,
                    "Together they use it up now.",
                    "So queue %s cannot grow, even if it is below its own maximum." % q.path,
                ],
                "calc": [
                    (None, "%s max: %s of the cluster = %s" % (path, pct(pmax), gb(pm[0]))),
                    (None, "Used by all queues under %s: %s" % (path, gb(pused[0]))),
                    ("no", "Left: %s, but the %s needs %s" % (gb(max(0, pleft[0])), what, gb(need))),
                ],
                "props": ["yarn.scheduler.capacity.%s.maximum-capacity" % path],
                "fix": ["Wait until apps under %s end." % path, "Or raise maximum-capacity of queue %s." % path],
            }
    below = b["used"][0] < b["guar"][0]
    return {
        "title": "The cluster is full",
        "what": [
            "Queue %s is allowed more, but the memory is not there." % q.path,
            "Other queues use the rest of the cluster now.",
        ],
        "calc": [
            (None, "Cluster for YARN: %s" % gb(b["total"])),
            ("no", "Free now: %s, but the %s needs %s" % (gb(b["free"][0]), what, gb(need))),
            (None, "Queue %s uses %s; its guaranteed share is %s" % (q.path, gb(b["used"][0]), gb(b["guar"][0]))),
            (
                "ok" if below else None,
                "It is below its guaranteed share, so YARN gives it memory first as other containers end "
                "(or takes it back from other queues, if preemption is on)"
                if below
                else "It is above its guaranteed share, so it gets more only when other queues leave memory free",
            ),
        ],
        "props": [qprop(q, "capacity"), "yarn.resourcemanager.scheduler.monitor.enable (preemption)"],
        "fix": [
            "Wait until containers end.",
            "For more room for good: add nodes, or give the queue a bigger guaranteed share (capacity).",
        ],
    }


def why_user_limit(b: Balance, user: User, need: float, what: str, same_u: float) -> Why:
    q = b["q"]
    capped = b["ulf"] > 0 and b["ulf"] * b["guar"][0] > b["max"][0]
    name = "A new user" if user["name"] == NEW_USER else user["name"]
    if b["ulf"] > 0:
        limit_line = "User limit: user-limit-factor %g x guaranteed %s = %s%s" % (
            b["ulf"],
            gb(b["guar"][0]),
            gb(b["ulf"] * b["guar"][0]),
            ", cut to the queue max %s" % gb(b["max"][0]) if capped else "",
        )
    else:  # -1 (Hadoop 3.3+): no user limit, so one user may use the queue max
        limit_line = "User limit: user-limit-factor %g means no user limit, so it is the queue max: %s" % (
            b["ulf"],
            gb(b["max"][0]),
        )
    fix = ["Wait until one of this user's apps ends."]
    if b["guar"][0] and b["ulf"] > 0 and not capped:
        fix.append(
            "Or raise user-limit-factor of queue %s. With %.2f or more, one user may use the whole "
            "queue max (%s)." % (q.path, b["max"][0] / b["guar"][0], gb(b["max"][0]))
        )
    return {
        "title": "%s has reached its user limit in queue %s" % (who(user).capitalize(), q.path),
        "what": [
            "One user's apps together may use only part of a queue.",
            "YARN counts per user, not per app: all apps of one account (for example a service "
            "account) share this limit.",
        ],
        "calc": [
            (None, limit_line),
            (None, "%s uses now: %s (all its apps together)" % (name, gb(user["used"] + same_u))),
            (
                "no",
                "Left: %s, but the %s needs %s" % (gb(max(0, user["limit"] - user["used"] - same_u)), what, gb(need)),
            ),
        ],
        "props": [qprop(q, "user-limit-factor"), qprop(q, "capacity")],
        "fix": fix,
    }


def why_am_pool(b: Balance, need: float, what: str, same_q: float) -> Why:
    q = b["q"]
    n = (q.live or {}).get("numActiveApplications")
    calc: List[Tuple[Optional[str], str]] = [
        (
            None,
            "AM pool: maximum-am-resource-percent %s of the queue = %s"
            % (pct(b["am_pct"]) if b["am_pct"] else "-", gb(b["am_limit"])),
        ),
        (
            None,
            "Used by the AMs of %s: %s"
            % ("the %s running apps" % n if n is not None else "the running apps", gb(b["am_used"])),
        ),
    ]
    if same_q:
        calc.append((None, "Taken by the step above: %s" % gb(same_q)))
    calc.append(
        ("no", "Free: %s, but the %s needs %s" % (gb(max(0, b["am_limit"] - b["am_used"] - same_q)), what, gb(need)))
    )
    return {
        "title": "The AM pool of queue %s is full" % q.path,
        "what": [
            "Every YARN app starts with one ApplicationMaster (AM) container.",
            "The app waits in ACCEPTED until its AM can start.",
            "All AMs in a queue together may use only part of the queue: the AM pool.",
            "Each running app holds one AM while it runs (a Spark driver in cluster mode, or an Oozie launcher).",
        ],
        "calc": calc,
        "props": [
            qprop(q, "maximum-am-resource-percent"),
            "yarn.scheduler.capacity.maximum-am-resource-percent (the default for all queues)",
        ],
        "fix": [
            "Wait until an app in queue %s ends." % q.path,
            "Or raise maximum-am-resource-percent of queue %s." % q.path,
            "If Oozie launchers use this queue too, give them their own queue (oozie.launcher.default.queue).",
        ],
    }


def why_user_am(b: Balance, user: User, need: float, what: str, same_u: float) -> Why:
    q = b["q"]
    guar, pct_am, mulp = b["guar"][0], b["am_pct"], q.min_user_pct
    guess = guar * (pct_am or 0) * max(b["ulf"], 0) * (mulp / 100.0 if mulp < 100 else 1)
    if b["am_limit"]:
        guess = min(guess, b["am_limit"])
    if pct_am and b["ulf"] > 0 and guess and abs(guess - user["am_limit"]) <= 0.15 * user["am_limit"]:
        how = "Worked out as: guaranteed %s x maximum-am-resource-percent %s x user-limit-factor %g%s = about %s" % (
            gb(guar),
            pct(pct_am),
            b["ulf"],
            " x minimum-user-limit-percent %g%%" % mulp if mulp < 100 else "",
            gb(guess),
        )
    else:
        how = "Worked out by YARN from maximum-am-resource-percent, user-limit-factor and minimum-user-limit-percent"
    name = "A new user" if user["name"] == NEW_USER else user["name"]
    calc: List[Tuple[Optional[str], str]] = [
        (None, "User AM limit: %s (YARN's figure, userAMResourceLimit)" % gb(user["am_limit"])),
        (None, how),
        (
            None,
            "%s's AMs now: %s (%s running app%s)"
            % (name, gb(user["am_used"]), user["apps"], "" if user["apps"] == 1 else "s"),
        ),
    ]
    if same_u:
        calc.append((None, "Taken by the step above: %s" % gb(same_u)))
    calc.append(
        (
            "no",
            "Free: %s, but the %s needs %s" % (gb(max(0, user["am_limit"] - user["am_used"] - same_u)), what, gb(need)),
        )
    )
    return {
        "title": "%s has reached its AM limit in queue %s" % (who(user).capitalize(), q.path),
        "what": [
            "YARN limits the AMs of each user, not only of the whole queue.",
            "All apps of one account share this limit.",
            "So one service account can block itself, while the AM pool still has room for other users.",
        ],
        "calc": calc,
        "props": [
            qprop(q, "maximum-am-resource-percent"),
            qprop(q, "user-limit-factor"),
            qprop(q, "minimum-user-limit-percent"),
        ],
        "fix": [
            "Wait until one of this user's apps ends.",
            "Or raise user-limit-factor of queue %s." % q.path,
            "Or raise maximum-am-resource-percent of queue %s." % q.path,
            "Or run some apps under another account: each user has its own AM limit.",
        ],
    }


def why_node(b: Balance, need: float, what: str, biggest: float, free_total: float) -> Why:
    return {
        "title": "No single node has room for the %s" % what,
        "what": [
            "A container must fit on one node; YARN cannot split it.",
            "There is free memory, but it is spread over the nodes in pieces that are too small.",
        ],
        "calc": [
            (None, "Free in the whole cluster: %s" % gb(free_total)),
            ("no", "The most free on one node: %s, but the %s needs %s" % (gb(biggest), what, gb(need))),
        ],
        "props": [
            "yarn.nodemanager.resource.memory-mb (memory per node)",
            "spark.executor.memory, spark.driver.memory / spark.yarn.am.memory (container sizes)",
        ],
        "fix": [
            "Wait until containers end on one of the nodes.",
            "Or use smaller containers (less memory per executor or AM).",
        ],
    }


def am_step(
    b: Balance, user: Optional[User], size: float, what: str, taken: List[Tuple[str, float]], nodes: List[float]
) -> Tuple[bool, List[Check], float, Tuple[str, Any]]:
    """Checks for one AM container of `size` in queue b, after the earlier steps in `taken`.
    nodes: the free memory of each node, changed in place when the container fits.
    Returns (ok, [(ok, short text, explanation)], room left after it, what sets that room)."""
    q = b["q"]
    same_q = sum(m for path, m in taken if path == q.path)  # earlier AMs in this queue
    same_u = sum(m for path, m in taken if path == q.path and user is not None)  # ... of the same user
    pre = "%s: " % what
    checks = [
        (
            q.state == "RUNNING",
            pre
            + (
                "queue %s is RUNNING" % q.path
                if q.state == "RUNNING"
                else "queue %s is %s: it takes no new apps" % (q.path, q.state)
            ),
            {
                "title": "Queue %s is %s" % (q.path, q.state),
                "what": ["A queue that is not RUNNING takes no new apps.", "Apps that already run go on."],
                "calc": [("no", "State of queue %s: %s" % (q.path, q.state))],
                "props": [qprop(q, "state")],
                "fix": [
                    "Ask the admin to set it to RUNNING and run yarn rmadmin -refreshQueues.",
                    "Or submit to another queue (spark-submit --queue).",
                ],
            },
        )
    ]
    after = " after the %s above" % gb(same_q) if same_q else ""
    if b["am_limit"] is not None:
        left = b["am_limit"] - b["am_used"] - same_q
        checks.append(
            (
                left >= size,
                pre + "AM pool of %s: %s free%s, needs %s" % (q.path, gb(max(0, left)), after, gb(size)),
                why_am_pool(b, size, what, same_q),
            )
        )
    if user is not None and user["am_limit"]:
        left = user["am_limit"] - user["am_used"] - same_u
        checks.append(
            (
                left >= size,
                pre + "AM limit of %s: %s free%s, needs %s" % (who(user), gb(max(0, left)), after, gb(size)),
                why_user_am(b, user, size, what, same_u),
            )
        )
    room, scope = room_after(b, taken)
    checks.append(
        (
            room >= size,
            pre + "room for queue %s now: %s, needs %s" % (q.path, gb(room), gb(size)),
            why_room(b, scope, size, what),
        )
    )
    biggest = max(nodes or [0])
    checks.append(
        (
            biggest >= size,
            pre + "the most free on one node: %s (a container must fit on one node)" % gb(biggest),
            why_node(b, size, what, biggest, sum(nodes or [0])),
        )
    )
    room_by: Tuple[str, Any] = ("room", scope)
    if user is not None and user["limit"]:
        user_left = user["limit"] - user["used"] - same_u
        checks.append(
            (
                user_left >= size,
                pre
                + "user limit of %s in %s: %s left (limit %s, uses %s)"
                % (user["name"], q.path, gb(max(0, user_left)), gb(user["limit"]), gb(user["used"] + same_u)),
                why_user_limit(b, user, size, what, same_u),
            )
        )
        if user_left < room:
            room, room_by = user_left, ("user", user)
    ok = all(c for c, _, _ in checks)
    if ok and nodes:
        nodes[nodes.index(biggest)] -= size
    return ok, checks, room - size, room_by


def executors_fit(
    b: Balance, room_left: float, nodes: List[float], room_by: Tuple[str, Any], same_u: float = 0
) -> Tuple[int, Optional[Why]]:
    """How many executors fit now, and if none: the explanation of what stops them."""
    size = b["executor_size"]
    if not size:
        return 0, None
    on_nodes = sum(int(max(0, f) // size) for f in nodes)
    n = min(on_nodes, int(max(0, room_left) // size))
    if n:
        return n, None
    what = "executor (%s)" % mb(size)
    if room_left < size:
        if room_by[0] == "user":
            return 0, why_user_limit(b, room_by[1], size, "first executor", same_u)
        return 0, why_room(b, room_by[1], size, "first executor")
    return 0, why_node(b, size, what, max(nodes or [0]), sum(nodes or [0]))


def new_app_check(b: Balance, user: Optional[User] = None) -> Tuple[bool, List[Check], int, Optional[Why]]:
    """Can a new Spark app (spark-submit) start in the queue now?
    Returns (yes, [(ok, text, explanation)], executors, explanation when no executor fits)."""
    nodes = list(b["nodes_free"])
    yes, checks, left, room_by = am_step(b, user, b["am_size"], "Spark AM", [], nodes)
    if not yes:
        return False, checks, 0, None
    n, why = executors_fit(b, left, nodes, room_by, b["am_size"])
    return True, checks, n, why


def oozie_check(
    b_launch: Balance, b_spark: Balance, name: Optional[str], launcher_size: float
) -> Tuple[bool, Optional[str], List[Check], int, Optional[Why]]:
    """Can a Spark action of an Oozie workflow start now? The launcher AM starts first in the launcher queue
    and stays until the Spark job ends; then the Spark AM must fit as well.
    Returns (yes, failed step or None, [(ok, text, explanation)], executors, explanation when none fit)."""
    nodes = list(b_spark["nodes_free"])
    ok1, checks1, _, _ = am_step(b_launch, user_in(b_launch, name), launcher_size, "launcher", [], nodes)
    if not ok1:
        return False, "launcher", checks1, 0, None
    taken = [(b_launch["q"].path, launcher_size)]
    ok2, checks2, left, room_by = am_step(b_spark, user_in(b_spark, name), b_spark["am_size"], "Spark AM", taken, nodes)
    if not ok2:
        return False, "spark", checks1 + checks2, 0, None
    same_u = sum(m for path, m in taken if path == b_spark["q"].path) + b_spark["am_size"]
    n, why = executors_fit(b_spark, left, nodes, room_by, same_u)
    return True, None, checks1 + checks2, n, why


LEDGER_W = 54


def ledger(
    label: str, amount: str, vcores: Optional[str] = None, sign: str = " ", role: Optional[str] = None, note: str = ""
) -> None:
    """One line of a balance sheet: sign, label, memory and vcores right-aligned, then a note."""
    sign = {"-": BOX["minus"], "=": "="}.get(sign, sign)
    if len(label) > LEDGER_W:  # a long label gets its own line, so the numbers stay in their columns
        print(highlight("    %s %s" % (sign, label)))
        sign, label = " ", ""
    text = ("    %s %-*s %11s %9s" % (sign, LEDGER_W, label, amount, "" if vcores is None else vcores)).rstrip()
    line = paint(text, role) if role else highlight(text)
    print(line + ("  " + paint(note, "bad") if note else ""))


def rule(vcores: bool = True) -> None:
    print(
        (
            "      %-*s %s %s"
            % (LEDGER_W, "", paint(BOX["line"] * 11, "rule"), paint(BOX["line"] * 9, "rule") if vcores else "")
        ).rstrip()
    )


def vc(n: Optional[float]) -> str:
    return "-" if n is None else "%g" % round(n, 1)


def print_balance(
    b: Balance,
    spark_queue: Optional[Queue],
    launcher_queue: Optional[Queue],
    uses_vcores: bool,
    oozie: Optional[Tuple[Balance, float]] = None,
) -> None:
    q = b["q"]
    roles = [r for r, x in (("Spark", spark_queue), ("Oozie", launcher_queue)) if x is q]
    print("  " + paint("Queue %s" % q.path, "queue") + (" (%s)" % ", ".join(roles) if roles else ""))
    print("    " + paint("How to read it:", "label"))
    bullets(
        [
            "What the queue may use.",
            "Minus what is used now.",
            "Equals what is left.",
            "Can a new app start now? If not: which limit stops it, the numbers, the setting behind it, "
            "and what you can do.",
        ],
        "      ",
        numbered=True,
    )
    print()
    print("      %-*s %11s %9s" % (LEDGER_W, "", paint("   Memory", "label"), paint("   vcores", "label")))
    ledger("Cluster for YARN, all running nodes", gb(b["total"]), vc(b["total_vc"]))
    ledger("Guaranteed: %s of the cluster (capacity)" % pct(b["abs_cap"]), gb(b["guar"][0]), vc(b["guar"][1]))
    ledger("Max: %s of the cluster (maximum-capacity)" % pct(b["abs_max"]), gb(b["max"][0]), vc(b["max"][1]))

    print()
    print("    " + paint("Room left now", "label"))
    ledger("Queue max", gb(b["max"][0]), vc(b["max"][1]))
    ledger("Used by the apps of this queue", gb(b["used"][0]), vc(b["used"][1]), sign="-")
    rule()
    ledger("(a) Left under the queue max", gb(b["left_q"][0]), vc(b["left_q"][1]), sign="=")
    letters = "bcdefgh"
    for i, (path, pmax, pm, pused, pleft) in enumerate(b["parents"]):
        ledger("Parent %s max (%s of the cluster)" % (path, pct(pmax)), gb(pm[0]), vc(pm[1]))
        ledger("Used by all queues under %s" % path, gb(pused[0]), vc(pused[1]), sign="-")
        rule()
        ledger("(%s) Left under the parent max" % letters[i], gb(pleft[0]), vc(pleft[1]), sign="=")
    last = letters[len(b["parents"])]
    ledger("(%s) Free in the whole cluster now" % last, gb(b["free"][0]), vc(b["free"][1]))
    rule()
    ledger(
        "Room for this queue now: the smallest of (a)%s" % ("" if last == "b" else "..") + "(%s)" % last,
        gb(b["room"]),
        vc(b["room_vc"]),
        sign="=",
        role="value",
        note="%s set by %s" % (BOX["arrow"], b["room_by"]),
    )
    if not uses_vcores:
        print(
            paint(
                wrap(
                    "vcores are shown, but YARN does not limit by them here: the resource calculator "
                    "counts memory only (yarn.scheduler.capacity.resource-calculator).",
                    "      ",
                ),
                "default",
            )
        )

    if b["am_limit"] is not None:
        print()
        print("    " + paint("AM pool: each new app needs room for its ApplicationMaster here", "label"))
        ledger(
            "AM pool (maximum-am-resource-percent %s)" % (pct(b["am_pct"]) if b["am_pct"] else "-"), gb(b["am_limit"])
        )
        ledger("Used by the AMs of the running apps", gb(b["am_used"]), sign="-")
        rule(vcores=False)
        left = b["am_limit"] - b["am_used"]
        n = int(max(0, left) // b["am_size"]) if b["am_size"] else 0
        ledger(
            "Free in the AM pool",
            gb(left),
            sign="=",
            role="value" if n else "bad",
            note="" if n else "%s full: new apps wait in ACCEPTED" % BOX["arrow"],
        )
        print(
            highlight(
                "      That is room for %d more Spark AM%s of %s (one AM per app)."
                % (n, "" if n == 1 else "s", mb(b["am_size"]))
            )
        )
        if b["user_am_limit"]:
            print(
                highlight(
                    wrap(
                        "Each user may use at most %s of the AM pool (userAMResourceLimit)." % gb(b["user_am_limit"]),
                        "      ",
                    )
                )
            )

    if b["users"]:
        print()
        print("    " + paint("Users with apps in this queue (the user limit is per user, not per app)", "label"))
        rows = []
        for u in b["users"]:
            left = u["limit"] - u["used"] if u["limit"] else None
            am_left = u["am_limit"] - u["am_used"] if u["am_limit"] else None
            rows.append(
                [
                    u["name"],
                    "%s / %s" % (u["apps"], u["waiting"]),
                    gb(u["limit"]),
                    gb(u["used"]),
                    gb(left),
                    gb(u["am_limit"]),
                    gb(u["am_used"]),
                    gb(am_left),
                ]
            )
        table(
            ["User", "Run / wait", "User limit", "Used", "Left", "AM limit", "AM used", "AM left"],
            rows,
            indent="      ",
            roles=[
                "queue",
                None,
                None,
                None,
                lambda c: "bad" if c.startswith("0.00") or c.startswith("-") and c != "-" else None,
                None,
                None,
                lambda c: "bad" if c.startswith("0.00") or c.startswith("-") and c != "-" else None,
            ],
        )
        notes = [
            "User limit: the most one user may use in this queue.",
            "It is user-limit-factor %g x guaranteed %s, at most the queue max." % (b["ulf"], gb(b["guar"][0]))
            if b["ulf"] > 0
            else "user-limit-factor %g means no user limit, so it is the queue max." % b["ulf"],
        ]
        if len(b["users"]) > 1 and b["q"].min_user_pct < 100:
            notes.append(
                "When several users want more at the same time, YARN also splits the queue between "
                "them (minimum-user-limit-percent %g%%), so each may get less." % b["q"].min_user_pct
            )
        bullets(notes, "      ")

    print()
    print("    " + paint("Can a new Spark app start here now?", "label"))
    users = list(b["users"])
    if oozie:  # the users of the launcher queue may start workflows too
        users += [u for u in oozie[0]["users"] if all(u["name"] != w["name"] for w in users)]
    users = users or [None]
    seen: Set[str] = set()
    for user in users:
        name = user["name"] if user else None
        mine = user_in(b, name)
        yes, checks, executors, why_exec = new_app_check(b, mine)
        print()
        print("      " + paint("As %s, with spark-submit:" % who(mine), "label"))
        print_checks(checks)
        print_verdict(
            yes,
            executors,
            b["executor_size"],
            ["it waits in ACCEPTED.", "ACCEPTED means: YARN has the app, but does not start it yet."],
            checks,
            why_exec,
            seen,
        )
        if oozie:
            b_launch, launcher_size = oozie
            same = b_launch["q"] is b["q"]
            print()
            print(
                paint(
                    wrap(
                        "As %s, " % who(mine)
                        + "from an Oozie workflow: first the launcher (%s, queue %s), then the Spark AM%s:"
                        % (mb(launcher_size), b_launch["q"].path, " in the same queue" if same else ""),
                        "      ",
                        "      ",
                    ),
                    "label",
                )
            )
            yes, failed, checks, executors, why_exec = oozie_check(b_launch, b, name, launcher_size)
            print_checks(checks)
            both = launcher_size + b["am_size"]
            limits = [
                (lim, text)
                for lim, text in (
                    (user_in(b, name)["am_limit"], "the AM limit of one user"),
                    (b["am_limit"], "the AM pool of the queue"),
                )
                if lim
            ]
            never = [(lim, text) for lim, text in limits if same and both > lim]
            if never:
                why_not = [
                    "it can NEVER start.",
                    "The launcher and the Spark AM together need %s." % gb(both),
                    "That is more than %s (%s), even when nothing else runs." % (never[0][1], gb(never[0][0])),
                    "So the launcher starts, and then waits for good.",
                    "Best fix: give the launchers their own queue (oozie.launcher.default.queue).",
                ]
            elif failed == "launcher":
                why_not = [
                    "the launcher waits in ACCEPTED.",
                    "ACCEPTED means: YARN has the app, but does not start it yet.",
                ]
            else:
                why_not = [
                    "the launcher starts, but the Spark AM waits in ACCEPTED.",
                    "The launcher keeps its %s while it waits." % mb(launcher_size),
                    "So the workflow hangs until the limit below frees up.",
                ]
            print_verdict(yes, executors, b["executor_size"], why_not, checks, why_exec, seen)
    if not b["users"]:
        print(
            highlight(
                wrap(
                    "No user has apps in this queue now, so the check is for a user that starts "
                    "fresh (user limit %s)." % gb(b["one_user"]),
                    "      ",
                )
            )
        )
    print()


def print_checks(checks: List[Check]) -> None:
    for ok, text, _ in checks:
        print("        " + mark("ok" if ok else "no") + " " + highlight(text))


# All marks take the same width, so the text after them lines up. On an ASCII terminal "OK" is two
# characters while "X" and "*" are one.
MARK_W = max(len(MARK_OK), len(BOX["no"]), len(BOX["bullet"]))


def mark(kind: Optional[str]) -> str:
    """✓ for ok, ✗ for not enough, a bullet for a plain fact; padded to MARK_W."""
    if kind == "ok":
        text, role = MARK_OK, "good"
    elif kind == "no":
        text, role = BOX["no"], "bad"
    else:
        text, role = BOX["bullet"], ""
    return paint(text, role) + " " * (MARK_W - len(text))


def bullets(items: Sequence[Union[str, Tuple[Optional[str], str]]], ind: str, numbered: bool = False) -> None:
    """Print a list: each item on its own line(s), with a mark, a bullet or a number."""
    for i, item in enumerate(items, 1):
        kind, text = item if isinstance(item, tuple) else (None, item)
        head = "%d." % i if numbered else mark(kind)
        width = len("%d." % i) if numbered else MARK_W
        line = wrap(text, ind + "x" * width + " ", ind + " " * (width + 1))
        print(ind + head + highlight(line[len(ind) + width :]))


def print_why(why: Why, also: Sequence[str] = (), seen: Optional[Set[str]] = None) -> None:
    """The plain explanation of the limit that stops an app, as short lists.
    seen: the explanations already given in this sheet; their "what it is" part is not repeated."""
    ind, sub = "          ", "            "
    print(paint(wrap("Why: " + why["title"], "        ", ind), "label"))
    if seen is None or why["title"] not in seen:
        print(ind + paint("What this limit is:", "label"))
        bullets(why["what"], sub)
        if seen is not None:
            seen.add(why["title"])
    else:
        print(ind + paint("What this limit is:", "label") + " see above.")
    if why.get("calc"):
        print(ind + paint("The numbers:", "label"))
        bullets(why["calc"], sub)
    print(ind + paint("Set by:", "label"))
    bullets(why["props"], sub)
    print(ind + paint("What you can do:", "label"))
    bullets(why["fix"], sub, numbered=True)
    if also:
        print(ind + paint("Also not enough (it would stop the app next):", "label"))
        bullets([("no", t) for t in also], sub)


def print_verdict(
    yes: bool,
    executors: int,
    executor_size: int,
    why_not: List[str],
    checks: List[Check],
    why_exec: Optional[Why] = None,
    seen: Optional[Set[str]] = None,
) -> None:
    """why_not: the headline when the app cannot start, then more lines, shown as bullets."""
    ind = "        "
    if not yes:
        print(ind + paint(BOX["no"] + " NO: " + why_not[0], "PROBLEM"))
        bullets(why_not[1:], ind + "  ")
        failed = [why for ok, _, why in checks if not ok]
        print_why(failed[0], [w["title"] for w in failed[1:]], seen)
        return
    if executors:
        print(
            ind
            + paint(
                MARK_OK
                + " YES: it starts now, and about %d executor%s of %s fit%s right away."
                % (executors, "" if executors == 1 else "s", mb(executor_size), "s" if executors == 1 else ""),
                "good",
            )
        )
        return
    print(ind + paint(MARK_OK + " YES, it starts, but:", "good"))
    bullets([("no", "no executor of %s fits right away, so its tasks wait." % mb(executor_size))], ind + "  ")
    if why_exec:
        print_why(why_exec, seen=seen)


def short_reason(text: str) -> str:
    """'Spark AM: AM pool of root.default: 0.00 GB free' -> 'Spark AM: AM pool of root.default'"""
    return ": ".join(text.split(": ")[:2])


def launcher_size(files: Dict[str, ConfigFile], min_alloc: float) -> Optional[int]:
    """The memory of one Oozie launcher in YARN (rounded like any container), or None without oozie-site.xml."""
    if files["oozie"].props is None:
        return None
    raw = to_float(files["oozie"].get("oozie.launcher.default.memory.mb", "2048")) or 2048
    return int(math.ceil(raw / float(min_alloc)) * min_alloc)


def print_balances(
    queues: Dict[str, Queue],
    live: Live,
    spark_queue: Optional[Queue],
    launcher_queue: Optional[Queue],
    am: Container,
    executor: Container,
    uses_vcores: bool,
    only: Optional[List[str]] = None,
    full: bool = True,
    launcher_size: Optional[int] = None,
    details: bool = False,
) -> None:
    """The room of each queue in numbers: an overview table, then a balance sheet per queue.
    With --queue the cards at the top answer the question, so the sheets come only with --details."""
    balances = [b for b in (queue_balance(queues[p], live, am.size, executor.size) for p in sorted(queues)) if b]
    if not balances:
        return
    section(
        "Queue room in numbers",
        "What each queue may use, what it uses now and what is left, in GB (1 GB = 1,024 MB) and vcores. "
        "A snapshot: containers end every few seconds."
        + (" The balance sheets below show how each number is worked out." if full else ""),
    )
    b_launch = next((b for b in balances if b["q"] is launcher_queue), None) if launcher_queue else None
    oozie = (b_launch, launcher_size) if b_launch and launcher_size else None
    rows: List[List[str]] = []
    for b in balances:
        top = max(b["users"], key=lambda u: u["used"]) if b["users"] else user_in(b, None)
        yes, checks, _, _ = new_app_check(b, top)
        row = [
            b["q"].path,
            gb(b["guar"][0]),
            gb(b["max"][0]),
            gb(b["used"][0]),
            gb(b["room"]),
            gb(b["am_limit"] - b["am_used"]) if b["am_limit"] is not None else "-",
            "yes" if yes else "NO: " + short_reason(next(t for ok, t, _ in checks if not ok)),
        ]
        if oozie:
            yes, _, checks, _, _ = oozie_check(oozie[0], b, top["name"] if b["users"] else None, oozie[1])
            row.append("yes" if yes else "NO: " + short_reason(next(t for ok, t, _ in checks if not ok)))
        rows.append(row)
    headers = ["Queue", "Guaranteed", "Max", "Used", "Room now", "AM pool free", "spark-submit now?"]

    def verdict(c: str) -> str:
        return "good" if c == "yes" else "bad"

    table(
        headers + (["From Oozie now?"] if oozie else []),
        rows,
        roles=["queue", None, None, None, "value", None, verdict, verdict],
    )
    print(
        highlight(
            wrap(
                "Checked for the user that uses the most of each queue (or a fresh user)."
                + (
                    " From Oozie: the launcher (%s) starts first in %s and stays, then the Spark AM must "
                    "fit too." % (mb(oozie[1]), oozie[0]["q"].path)
                    if oozie
                    else ""
                ),
                "  ",
            )
        )
    )
    if not full:
        return
    if only and not details:
        print()
        print(highlight(wrap("The balance sheets (every number, step by step): run again with --details.", "  ")))
        return
    if only:
        chosen = [b for b in balances if any(b["q"] is find_queue(queues, n) for n in only)]
    else:
        chosen = [
            b
            for b in balances
            if b["q"] in (spark_queue, launcher_queue)
            or b["users"]
            or (b["q"].live or {}).get("numPendingApplications")
        ]
    print()
    if not chosen:
        print(
            highlight(
                wrap("No balance sheet: no queue is Spark's or has apps now. Use --queue NAME to show one.", "  ")
            )
        )
        return
    print(
        highlight(
            wrap(
                "Balance sheets for %s. Use --queue NAME (more than once) to pick others."
                % ", ".join(b["q"].path for b in chosen),
                "  ",
            )
        )
    )
    print()
    for b in chosen:
        print_balance(b, spark_queue, launcher_queue, uses_vcores, oozie)


# ================================================================== the queue card (--queue)
# One screen per queue that answers "can my job run, and if not, why", in jobs rather than GB.
# Two budgets decide it: room to START a job (its ApplicationMaster, limited by the AM limits) and
# room to RUN it (its executors, limited by the queue and user limits and by free memory on the nodes).
# Starting a job that then gets no executor does not help, so the card shows both side by side.


def gauge(used: float, limit: Optional[float]) -> str:
    """A 10-character bar and a percentage: how full a limit is."""
    if not limit or limit <= 0:
        return " " * 17
    frac = max(0.0, min(1.0, used / limit))
    cells = frac * 10
    full = int(cells)
    part = BAR["parts"][int((cells - full) * len(BAR["parts"]))] if full < 10 else ""
    bar = BAR["full"] * full + part
    return "[%s] %3d%%" % (bar + BAR["empty"] * (10 - len(bar)), round(frac * 100))


def simulate_jobs(
    b: Balance, user: User, am: float, ex: float, n_ex: int, launcher: float = 0, cap: int = 200
) -> Tuple[int, int, str, str, Dict[str, float], Dict[str, float]]:
    """Start jobs one by one, each with its AM (plus an Oozie launcher when given) and n_ex executors.
    Returns (jobs whose AM can start, jobs that also get their executors, what stops the start,
    what stops the executors, what was left when the start stopped, what was left when the
    executors stopped). "What was left" has the jobs before it, so a reason can say "after them"."""
    nodes = sorted(b["nodes_free"], reverse=True)
    user_am_left = (user["am_limit"] - user["am_used"]) if user["am_limit"] else float("inf")
    pool_left = (b["am_limit"] - b["am_used"]) if b["am_limit"] is not None else float("inf")
    room = b["room"]
    user_left = (user["limit"] - user["used"]) if user["limit"] else float("inf")
    starter = am + launcher
    started = running = 0
    stop_start = stop_run = ""
    at_start: Dict[str, float] = {}
    at_run: Dict[str, float] = {}

    def left(jobs: int) -> Dict[str, float]:
        return {
            "user_am": user_am_left,
            "pool": pool_left,
            "room": room,
            "user": user_left,
            "node": max(nodes or [0]),
            "jobs": jobs,
        }

    if b["q"].state != "RUNNING":
        return 0, 0, "state", "state", left(0), left(0)
    while started < cap:
        if user_am_left < starter:
            stop_start, at_start = "user_am", left(started)
            break
        if pool_left < starter:
            stop_start, at_start = "pool", left(started)
            break
        if min(room, user_left) < starter:
            stop_start, at_start = ("user" if user_left < room else "room"), left(started)
            break
        placed = []
        for size in ([launcher] if launcher else []) + [am]:
            big = max(range(len(nodes)), key=lambda i: nodes[i]) if nodes else -1
            if big < 0 or nodes[big] < size:
                break
            nodes[big] -= size
            placed.append((big, size))
        if len(placed) < (2 if launcher else 1):
            for i, size in placed:
                nodes[i] += size
            stop_start, at_start = "node", left(started)
            break
        started += 1
        user_am_left -= starter
        pool_left -= starter
        room -= starter
        user_left -= starter
        if stop_run:
            continue  # later jobs can start, but the ones before already found no executor room
        got = 0
        for _ in range(n_ex):
            if min(room, user_left) < ex:
                stop_run, at_run = ("user" if user_left < room else "room"), left(running)
                break
            big = max(range(len(nodes)), key=lambda i: nodes[i]) if nodes else -1
            if big < 0 or nodes[big] < ex:
                stop_run, at_run = "node", left(running)
                break
            nodes[big] -= ex
            room -= ex
            user_left -= ex
            got += 1
        if got == n_ex:
            running += 1
    return started, running, stop_start, stop_run, at_start, at_run


def limit_text(kind: str, at: Dict[str, float], name: str, need: float, what: str, own: str = "") -> str:
    """A short reason: which limit has too little left, after the jobs before it (when any) and, for an
    executor, after the job's own starter (own names it)."""
    texts = {
        "state": "the queue is not RUNNING",
        "user_am": "%s's job-starter limit has only %s left" % (name, gb(max(0, at.get("user_am", 0)))),
        "pool": "the queue's job starters (AM pool) have only %s left" % gb(max(0, at.get("pool", 0))),
        "room": "the queue has only %s of memory left" % gb(max(0, at.get("room", 0))),
        "user": "%s's share of the queue has only %s left" % (name, gb(max(0, at.get("user", 0)))),
        "node": "the most free on one node is %s" % gb(max(0, at.get("node", 0))),
    }
    text = texts.get(kind, "a limit is full")
    if kind != "state":
        text += ", and %s needs %s" % (what, gb(need))
    jobs = int(at.get("jobs", 0))
    before = ["the %d new job%s above" % (jobs, "" if jobs == 1 else "s")] if jobs else []
    before += [own] if own else []
    if before:
        text = "after %s, %s" % (" and ".join(before), text)
    return text


def free_executors(b: Balance, user: User, am: float, launcher: float = 0) -> Tuple[int, str, Dict[str, float]]:
    """After one new job starter (and an Oozie launcher when given): how many executors of the configured
    size fit right now, what stops more, and what is left then. Each job decides how many it asks for, so
    this is the room, not a number of jobs."""
    ex = float(b["executor_size"])
    nodes = sorted(b["nodes_free"], reverse=True)
    room, user_left = b["room"], (user["limit"] - user["used"]) if user["limit"] else float("inf")
    for size in ([launcher] if launcher else []) + [am]:
        if nodes:
            nodes[0] -= size
            nodes.sort(reverse=True)
        room -= size
        user_left -= size
    count, kind = 0, ""
    while count < 1000:
        if min(room, user_left) < ex:
            kind = "user" if user_left < room else "room"
            break
        if not nodes or nodes[0] < ex:
            kind = "node"
            break
        nodes[0] -= ex
        nodes.sort(reverse=True)
        room -= ex
        user_left -= ex
        count += 1
    return count, kind, {"room": room, "user": user_left, "node": max(nodes or [0]), "jobs": 0}


def print_queue_card(b: Balance, user: User, oozie: Optional[Tuple[Balance, float]], uses_vcores: bool) -> None:
    q = b["q"]
    am, ex = float(b["am_size"]), float(b["executor_size"])
    name = "a new user" if user["name"] == NEW_USER else user["name"]
    who_text = "no jobs now (checked for a new user)" if user["name"] == NEW_USER else "user " + name
    title = "QUEUE %s  %s  %s" % (q.path, DOT, who_text)
    print()
    print(paint(title, "section"))
    print(paint(BOX["line"] * len(title), "rule"))

    # how many job starters fit: their size is known, so this is a real count
    starters, _, stop_start, _, at_start, _ = simulate_jobs(b, user, am, ex, 0)
    user_am_left = (user["am_limit"] - user["am_used"]) if user["am_limit"] else None
    pool_left = (b["am_limit"] - b["am_used"]) if b["am_limit"] is not None else None
    user_left = (user["limit"] - user["used"]) if user["limit"] else None
    jobs_now = int(user["apps"] or 0)
    n_exec, exec_stop, at_exec = free_executors(b, user, am)
    exec_why = limit_text(exec_stop, at_exec, name, ex, "one executor", "its own job starter")

    # ---- the answer
    print()
    if starters == 0:
        start_why = limit_text(stop_start, at_start, name, am, "one job starter")
        if stop_start == "user_am":
            start_why = (
                "%s already runs %d job%s, and its job-starter limit has only %s left; one job starter needs %s"
                % (
                    name,
                    jobs_now,
                    "" if jobs_now == 1 else "s",
                    gb(max(0.0, at_start.get("user_am", 0.0))),
                    gb(am),
                )
            )
        print("  " + paint(BOX["no"] + " A new Spark job would WAIT now (state ACCEPTED).", "PROBLEM"))
        reasons: List[Union[str, Tuple[Optional[str], str]]] = ["Why: " + start_why + "."]
        if stop_start != "state" and n_exec == 0:
            now = {
                "room": b["room"],
                "user": user_left if user_left is not None else float("inf"),
                "node": b["node_free"],
                "jobs": 0,
            }
            kind = (
                "user" if (user_left or float("inf")) < min(b["room"], ex) else ("room" if b["room"] < ex else "node")
            )
            reasons.append(
                (
                    "no",
                    "And even if it could start, it would get no executor now: %s."
                    % limit_text(kind, now, name, ex, "one executor"),
                )
            )
        bullets(reasons, "    ")
    elif n_exec == 0:
        print("  " + paint("! A new Spark job would START, but get no executor now, so it would do nothing.", "bad"))
        bullets(["Why: " + exec_why + "."], "    ")
    else:
        print(
            "  "
            + paint(
                MARK_OK
                + " A new Spark job can START now and get up to %d executor%s (%s%s) right away."
                % (n_exec, "" if n_exec == 1 else "s", gb(ex), "" if n_exec == 1 else " each"),
                "good",
            )
        )
        bullets(["More than that: as memory frees up (%s)." % exec_why], "    ")

    # ---- the two budgets
    def row(label: str, used: float, limit: Optional[float], left_text: str, ok: Optional[bool]) -> None:
        line = "%-34s %s  %s" % (label, gauge(used, limit), left_text)
        print("    " + (mark("ok" if ok else "no") if ok is not None else mark(None)) + " " + highlight(line))

    def execs(gb_left: float) -> str:
        n = int(max(0.0, gb_left) // ex) if ex else 0
        return "%d executor%s" % (n, "" if n == 1 else "s")

    print()
    print("  " + paint("1. Job-starter (AM) limit: what is available", "label"))
    bullets(["Every job first starts one job starter (its ApplicationMaster) of %s." % gb(am)], "    ")
    if user["am_limit"]:
        n = int(max(0.0, user_am_left or 0.0) // am)
        row(
            "%s's job starters" % name,
            user["am_used"],
            user["am_limit"],
            "%s free of %s  room for %d more" % (gb(max(0.0, user_am_left or 0.0)), gb(user["am_limit"]), n),
            n > 0,
        )
    if b["am_limit"] is not None:
        n = int(max(0.0, pool_left or 0.0) // am)
        row(
            "job starters of the whole queue",
            b["am_used"],
            b["am_limit"],
            "%s free of %s  room for %d more" % (gb(max(0.0, pool_left or 0.0)), gb(b["am_limit"]), n),
            n > 0,
        )

    print()
    print("  " + paint("2. Memory: what is available", "label"))
    bullets(
        [
            "Then a job needs executors to do the work: %s each (spark-defaults.conf). A job that starts but "
            "gets no executor does nothing." % gb(ex)
        ],
        "    ",
    )
    left_q = max(0.0, b["max"][0] - b["used"][0])
    row(
        "memory of the queue (its max)",
        b["used"][0],
        b["max"][0],
        "%s free of %s  = %s" % (gb(left_q), gb(b["max"][0]), execs(left_q)),
        left_q >= ex,
    )
    if user["limit"]:
        left_u = max(0.0, user["limit"] - user["used"])
        row(
            "%s's share of the queue" % name,
            user["used"],
            user["limit"],
            "%s free of %s  = %s" % (gb(left_u), gb(user["limit"]), execs(left_u)),
            left_u >= ex,
        )
    used_c = b["total"] - b["free"][0]
    row(
        "memory of the whole cluster",
        used_c,
        b["total"],
        "%s free of %s  = %s" % (gb(b["free"][0]), gb(b["total"]), execs(b["free"][0])),
        b["free"][0] >= ex,
    )
    row(
        "largest free space on one node",
        0,
        None,
        "%s  (an executor needs %s)" % (gb(b["node_free"]), gb(ex)),
        b["node_free"] >= ex,
    )
    print(
        "    "
        + mark("ok" if n_exec else "no")
        + " "
        + highlight(
            "Executors a new job can get right now: %d (placed on the real nodes, after its job starter)." % n_exec
        )
    )

    # ---- in short
    print()
    print("  " + paint("3. In short", "label"))
    lines: List[Union[str, Tuple[Optional[str], str]]] = [
        "%s runs %d job%s here now."
        % (name[0].upper() + name[1:] if name == NEW_USER else name, jobs_now, "" if jobs_now == 1 else "s"),
        ("ok" if starters else "no", "Job starters: room for %d more." % starters),
        (
            "ok" if n_exec else "no",
            "Memory: %s free for %s in this queue, up to %d executor%s for a new job now."
            % (
                gb(max(0.0, min(b["room"], user_left if user_left is not None else b["room"]))),
                name,
                n_exec,
                "" if n_exec == 1 else "s",
            ),
        ),
    ]
    # the job-starter limit against the memory behind it: how much each job would have
    if user["am_limit"] and user["limit"]:
        most = int(user["am_limit"] // am)
        if most >= 2:
            per_job = (user["limit"] - most * am) / most
            lines.append(
                (
                    "no" if per_job < ex else None,
                    "If %s ran %d jobs (the most its job-starter limit allows), each would have about %s "
                    "for executors (its share of the queue, %s, split %d ways)%s."
                    % (
                        name,
                        most,
                        gb(max(0.0, per_job)),
                        gb(user["limit"]),
                        most,
                        ": less than one executor, so some would sit idle" if per_job < ex else "",
                    ),
                )
            )
    if oozie:
        b_l, l_size = oozie
        same = b_l["q"] is q
        o_starts, _, o_stop, _, o_at, _ = simulate_jobs(b, user, am, ex, 0, l_size if same else 0)
        if same and user["am_limit"] and l_size + am > user["am_limit"]:
            lines.append(
                (
                    "no",
                    "From Oozie: NEVER. Its launcher (%s) and the job starter (%s) together are more "
                    "than %s's job-starter limit (%s)." % (gb(l_size), gb(am), name, gb(user["am_limit"])),
                )
            )
        else:
            lines.append(
                (
                    "ok" if o_starts else "no",
                    "From Oozie (a launcher of %s first%s): room for %d more%s."
                    % (
                        gb(l_size),
                        ", in the same queue" if same else " in " + b_l["q"].path,
                        o_starts,
                        ""
                        if o_starts
                        else ": "
                        + limit_text(o_stop, o_at, name, am + (l_size if same else 0), "the launcher and job starter"),
                    ),
                )
            )
    bullets(lines, "    ")

    # ---- what to do
    fixes: List[str] = []
    prefix = "yarn.scheduler.capacity.%s." % q.path
    if starters == 0 and stop_start == "user_am":
        fixes.append("Wait: when one of %s's jobs ends, its job starter frees up." % name)
        if b["ulf"] > 0 and user["am_limit"]:
            need = user["am_used"] + 2 * am
            new_ulf = max(math.ceil(b["ulf"] * need / user["am_limit"] * 10) / 10.0, b["ulf"] + 0.1)
            fixes.append(
                "Let %s start more jobs: %suser-limit-factor %g -> %g gives a job-starter limit of about %s (room "
                "for about 2 more). It also raises %s's share of the queue."
                % (name, prefix, b["ulf"], new_ulf, gb(user["am_limit"] * new_ulf / b["ulf"]), name)
            )
    if starters == 0 and stop_start == "pool" and b["am_pct"]:
        need = b["am_used"] + am
        new_pct = max(math.ceil(b["am_pct"] * need / b["am_limit"] * 100) / 100.0, b["am_pct"] + 0.01)
        fixes.append(
            "Wait for a job in the queue to end, or raise %smaximum-am-resource-percent %g -> %g (room for "
            "about 1 more job starter)." % (prefix, b["am_pct"], new_pct)
        )
    if n_exec == 0 or (starters == 0 and stop_start in ("room", "node")):
        if exec_stop == "user" or stop_start == "user":
            fixes.append(
                "Raise %suser-limit-factor (now %g) so %s may use more of the queue." % (prefix, b["ulf"], name)
            )
        elif "queue maximum" in b["room_by"] and b["abs_max"] < 0.999:
            fixes.append(
                "Wait until other jobs in the queue end, or raise %smaximum-capacity (now %s of the cluster)."
                % (prefix, pct(b["abs_max"]))
            )
        elif "parent" in b["room_by"]:
            fixes.append(
                "Wait until jobs under the parent queue end, or raise its maximum-capacity (%s)." % b["room_by"]
            )
        else:
            fixes.append("Wait until other containers end. For more room for good, add nodes to the cluster.")
        if exec_stop == "node":
            fixes.append("Or use smaller executors (spark.executor.memory), so they fit in the free space on a node.")
    if oozie and oozie[0]["q"] is q and user["am_limit"] and oozie[1] + am > user["am_limit"]:
        fixes.append("For Oozie: give the launchers their own queue (oozie.launcher.default.queue).")
    if fixes:
        print()
        print("  " + paint("4. What you can do", "label"))
        bullets(fixes, "    ", numbered=True)
    print()
    print(highlight(wrap("All numbers in detail (the balance sheet): run again with --details.", "  ")))
    if not uses_vcores:
        print(highlight(wrap("Only memory counts here: the resource calculator ignores vcores.", "  ")))


def print_queue_cards(
    names: List[str],
    queues: Dict[str, Queue],
    live: Optional[Live],
    am: Container,
    executor: Container,
    files: Dict[str, ConfigFile],
    l_size: Optional[int],
) -> None:
    """One card per queue named with --queue, and per user with apps in it (or a new user)."""
    print()
    if not live:
        print(paint(wrap("The queue cards need live data from the ResourceManager (see above).", "  "), "bad"))
        return
    launcher_q = (
        find_queue(queues, files["oozie"].get("oozie.launcher.default.queue", "default"))
        if files["oozie"].props is not None
        else None
    )
    b_launch = queue_balance(launcher_q, live, am.size, executor.size) if launcher_q else None
    oozie = (b_launch, float(l_size)) if b_launch and l_size else None
    uses_vcores = "dominant" in files["cs"].get("yarn.scheduler.capacity.resource-calculator", "").lower()
    for name in names:
        q = find_queue(queues, name)
        b = queue_balance(q, live, am.size, executor.size) if q else None
        if q is None or b is None:
            print(
                paint(
                    wrap(
                        "Queue %s: no such leaf queue in the live data. Leaf queues: %s."
                        % (name, ", ".join(sorted(queues))),
                        "  ",
                    ),
                    "bad",
                )
            )
            continue
        for user in sorted(b["users"], key=lambda u: -u["used"]) or [user_in(b, None)]:
            print_queue_card(b, user, oozie, uses_vcores)


# What YARN says when an app waits in ACCEPTED, in plain words
WAIT_REASONS = [
    ("user's am resource limit exceeded", "user's AM limit is full"),
    ("queue's am resource limit exceeded", "queue's AM pool is full"),
    ("am resource limit exceeded", "an AM limit is full"),
    ("waiting for am container to be allocated", "no node has room for its AM"),
    ("waiting for resources to be assigned for am", "no node has room for its AM"),
    ("maximum parallel apps", "max-parallel-apps reached"),
    ("not yet activated", "accepted, not started yet"),
]


def wait_reason(app: Json) -> Tuple[str, str]:
    """Return (short reason, cleaned YARN diagnostics) for an app in ACCEPTED."""
    text = re.sub(r"\[[^\]]*\d{4}\]\s*", "", app.get("diagnostics") or "")  # drop the timestamps
    text = " ".join(text.split())
    low = text.lower()
    for key, short in WAIT_REASONS:
        if key in low:
            return short, text
    return (text[:40] + "..." if len(text) > 40 else text) or "no reason given by YARN", text


def elapsed(ms: Any) -> str:
    try:
        sec = int(ms) // 1000
    except (TypeError, ValueError):
        return "-"
    if sec >= 3600:
        return "%dh%02dm" % (sec // 3600, sec % 3600 // 60)
    return "%dm%02ds" % (sec // 60, sec % 60)


def doing_now(app: Json) -> str:
    """What one app does now, in a few words."""
    if app.get("state") == "ACCEPTED":
        return "waiting: " + wait_reason(app)[0]
    name = str(app.get("name", ""))
    stages = app.get("_stages")
    if name.startswith("oozie:launcher"):
        text = "Oozie launcher: waits for its child job"
    elif stages:
        st = sorted(stages, key=lambda x: x.get("stageId", 0))[0]
        text = "stage %s %s: %s/%s tasks" % (
            st.get("stageId", "?"),
            " ".join(str(st.get("name", "")).split()[:3]),
            st.get("numCompleteTasks", "?"),
            st.get("numTasks", "?"),
        )
        if len(stages) > 1:
            text += " (+%d more stage%s)" % (len(stages) - 1, "" if len(stages) == 2 else "s")
    elif stages == []:
        text = "no stage runs now (driver code, or between jobs)"
    else:
        progress = to_float(app.get("progress"))
        text = "%.0f%% done (YARN progress)" % progress if progress is not None else "-"
    count = pending_request(app)[0]
    if count:  # it asks YARN for more and does not have it yet; the reason is listed below the table
        text += " %s wants %s" % (
            DOT,
            "%d more container%s" % (count, "" if count == 1 else "s") if count > 0 else "more memory",
        )
    return text


def why_no_resources(app: Json, live: Live, queues: Dict[str, Queue]) -> Optional[str]:
    """Explain in plain words why a running app does not get what it asks for. None if it gets it."""
    apps = live.get("apps") or []
    total = to_float(live["metrics"].get("totalMB")) or 0
    free_total = to_float(live["metrics"].get("availableMB")) or 0
    nodes = [n for n in live.get("nodes") or [] if n.get("state") == "RUNNING"]
    node_free = max([to_float(n.get("availMemoryMB")) or 0 for n in nodes] or [0])
    node_size = max(
        [(to_float(n.get("usedMemoryMB")) or 0) + (to_float(n.get("availMemoryMB")) or 0) for n in nodes] or [0]
    )
    count, size = pending_request(app)
    q = find_queue(queues, str(app.get("queue", ""))) if queues else None

    if not count:
        # YARN gives it all it asks for. If Spark has tasks that wait, Spark itself does not ask.
        conf, left = app.get("_spark_conf"), waiting_tasks(app)
        if not left or conf is None:
            return None
        executors = max(0, int(to_float(app.get("runningContainers")) or 1) - 1)
        if conf.get("spark.dynamicAllocation.enabled", "false").lower() != "true":
            return (
                "%d tasks wait, but the app does not ask YARN for more: dynamic allocation is off, so it "
                "keeps its %s executors (spark.executor.instances). Give more with --num-executors, or "
                "turn on dynamic allocation." % (left, conf.get("spark.executor.instances", "2 (default)"))
            )
        cap = conf.get("spark.dynamicAllocation.maxExecutors")
        if cap and executors >= int(to_float(cap) or 0):
            return (
                "%d tasks wait, but the app does not ask YARN for more: it already has %d executor%s, its own "
                "limit (spark.dynamicAllocation.maxExecutors = %s). YARN is not the cause. Raise the limit "
                "for this app if the cluster has room." % (left, executors, "" if executors == 1 else "s", cap)
            )
        return None  # dynamic allocation asks again after its backlog timeout (1 s by default)

    want = (
        "%d more container%s of %s" % (count, "" if count == 1 else "s", mb(size))
        if count > 0 and size
        else "%s more memory" % mb(size)
    )
    need = size if count > 0 and size else min(size or 0, 1024)  # one container, as far as we know
    head = "It asks YARN for %s and does not get %s yet: " % (want, "them" if count != 1 else "it")

    if node_size and need > node_size:
        return head + (
            "one container (%s) is bigger than any node gives YARN (%s), so it can never start. "
            "Make the executors smaller." % (mb(need), mb(node_size))
        )
    if q is not None and total:
        user = str(app.get("user", ""))
        user_used = sum(
            to_float(a.get("allocatedMB")) or 0
            for a in apps
            if str(a.get("user")) == user
            and (to_float(a.get("allocatedMB")) or 0) > 0
            and find_queue(queues, str(a.get("queue", ""))) is q
        )
        q_used = to_float(((q.live or {}).get("resourcesUsed") or {}).get("memory")) or 0
        if (
            q.one_user_max is not None
            and q.abs_max is not None
            and q.one_user_max < q.abs_max - 1e-9
            and user_used + need > q.one_user_max * total
        ):
            return head + (
                "user %s already uses %s of the cluster with all its apps, and one user may use only "
                "%s in queue %s (user-limit-factor %g x guaranteed %s). Every app of this user shares "
                "that limit. Raise user-limit-factor for the queue, or submit as several users."
                % (user, pct(user_used / total), pct(q.one_user_max), q.path, q.ulf, pct(q.abs_cap))
            )
        # A maximum of the whole cluster is no queue limit: then the cluster itself is full (below)
        if q.abs_max is not None and q.abs_max < 0.999 and q_used + need > q.abs_max * total:
            return head + (
                "queue %s is at its maximum (%s of the cluster, maximum-capacity). It gets more only "
                "when its own apps free containers." % (q.path, pct(q.abs_max))
            )
    if node_free >= need:
        older = [
            a
            for a in apps
            if a is not app
            and a.get("state") == "RUNNING"
            and pending_request(a)[0]
            and str(a.get("queue")) == str(app.get("queue"))
            and (a.get("startedTime") or 0, a.get("id", "")) < (app.get("startedTime") or 0, app.get("id", ""))
        ]
        fair = q is not None and "fair" in (q.ordering or "").lower()
        if older and not fair:
            return head + (
                "older apps in the same queue ask for containers too (%s), and YARN serves the apps of a "
                "queue in start order (FIFO, the default ordering-policy). This app gets containers "
                "after them. Setting the queue's ordering-policy to fair shares the queue between "
                "its apps." % ", ".join(str(a.get("id")) for a in older[:3])
            )
        if older and fair:
            return head + (
                "other apps in the same queue ask for containers too (%s). The queue shares between its "
                "apps (ordering-policy fair), so this app gets its turn as containers end."
                % ", ".join(str(a.get("id")) for a in older[:3])
            )
        return head + (
            "a node has %s free, so YARN should give them within seconds. If this stays, check "
            "node labels and data-locality waits (yarn.scheduler.capacity.node-locality-delay)." % mb(node_free)
        )
    if free_total >= need:
        return head + (
            "the cluster has %s free in total, but no single node has %s free (the most is %s), "
            "and one container must fit on one node." % (mb(free_total), mb(need), mb(node_free))
        )
    above = ""
    if q is not None and q.abs_cap is not None and total:
        q_used = to_float(((q.live or {}).get("resourcesUsed") or {}).get("memory")) or 0
        above = (
            " Queue %s already uses %s, more than its guaranteed %s, so it only gets what other queues "
            "free." % (q.path, pct(q_used / total), pct(q.abs_cap))
            if q_used > q.abs_cap * total
            else " Queue %s uses %s, less than its guaranteed %s, so YARN gives it memory first as other "
            "containers end (or by preemption, if it is on)." % (q.path, pct(q_used / total), pct(q.abs_cap))
        )
    return head + (
        "the cluster is full: only %s is free and the most on one node is %s.%s"
        % (mb(free_total), mb(node_free), above)
    )


def app_users(live: Optional[Live], queues: Dict[str, Queue]) -> List[Tuple[Tuple[str, str], List[Json]]]:
    """Group the apps by queue and user: [(queue path, user, apps)] with the busiest group first."""
    groups: Dict[Tuple[str, str], List[Json]] = {}
    for a in (live or {}).get("apps") or []:
        qname = str(a.get("queue", "?"))
        q = find_queue(queues, qname) if queues else None
        groups.setdefault((q.path if q else qname, str(a.get("user", "?"))), []).append(a)
    return sorted(groups.items(), key=lambda kv: -sum(to_float(a.get("allocatedMB")) or 0 for a in kv[1]))


def print_apps(live: Live, queues: Dict[str, Queue]) -> None:
    apps = live.get("apps")
    if apps is None:
        return
    total = to_float(live["metrics"].get("totalMB")) or 0
    section(
        "Apps",
        "The apps that run or wait in YARN now, grouped by queue and user. YARN counts the "
        "user limit per user, not per app: all apps of one account (for example a service "
        "account) share one user limit.",
    )
    listed = len(apps)
    reported = (live["metrics"].get("appsRunning") or 0) + (live["metrics"].get("appsPending") or 0)
    if reported > listed:
        print(
            paint(
                wrap(
                    "The ResourceManager reports %d running or waiting apps but shows only %d to this "
                    "user. YARN ACLs (yarn.acl.enable) hide the others. Run the script as a YARN admin "
                    "(yarn.admin.acl) to see all apps." % (reported, listed),
                    "  ",
                ),
                "bad",
            )
        )
        print()
    if not apps:
        print("  No apps run or wait now.")
        return
    waiting_notes: List[Tuple[str, str]] = []
    for (qpath, user), group in app_users(live, queues):
        q = queues.get(qpath) if queues else None
        running = [a for a in group if a.get("state") == "RUNNING"]
        waiting = [a for a in group if a.get("state") == "ACCEPTED"]
        used = sum(to_float(a.get("allocatedMB")) or 0 for a in group if (to_float(a.get("allocatedMB")) or 0) > 0)
        line = "Queue %s %s user %s: %d running, %d waiting" % (qpath, DOT, user, len(running), len(waiting))
        if total:
            line += " %s uses %s of the cluster" % (DOT, pct(used / total))
            if q is not None and q.one_user_max is not None:
                line += " (one-user limit %s)" % pct(q.one_user_max)
        lq = q.live if q is not None else None
        if lq:
            users = (lq.get("users") or {}).get("user") or []
            if isinstance(users, dict):
                users = [users]
            mine = next((u for u in users if u.get("username") == user), None)
            am_used = ((mine or {}).get("AMResourceUsed") or {}).get("memory")
            am_lim = (lq.get("userAMResourceLimit") or {}).get("memory")
            if am_used is not None and am_lim:
                line += " %s AM %s of %s" % (DOT, mb(am_used), mb(am_lim))
        at_limit = q is not None and q.one_user_max and total and used / total >= q.one_user_max * 0.95
        print(
            highlight(wrap(line, "  ", "    "))
            if not at_limit
            else highlight(wrap(line + " %s AT ITS USER LIMIT" % DOT, "  ", "    ")).replace(
                "AT ITS USER LIMIT", paint("AT ITS USER LIMIT", "PROBLEM")
            )
        )

        def count(value: Any) -> str:  # YARN sends -1 when an app has no containers yet
            n = to_float(value)
            return str(int(n)) if n is not None and n >= 0 else "-"

        rows = []
        for a in sorted(group, key=lambda x: (x.get("state") != "RUNNING", -(to_float(x.get("allocatedMB")) or 0))):
            name = str(a.get("name", "?"))
            alloc = to_float(a.get("allocatedMB"))
            rows.append(
                [
                    a.get("id", "?"),
                    name if len(name) <= 28 else name[:27] + "~",
                    a.get("state", "?"),
                    elapsed(a.get("elapsedTime")),
                    count(a.get("runningContainers")),
                    mb(alloc) if alloc is not None and alloc >= 0 else "-",
                    count(a.get("allocatedVCores")),
                    doing_now(a),
                ]
            )
            if a.get("state") == "ACCEPTED":
                waiting_notes.append((a.get("id", "?"), wait_reason(a)[1]))
        table(
            ["App", "Name", "State", "Time", "Cont", "Memory", "vcores", "Doing now"],
            rows,
            indent="    ",
            roles=[
                "label",
                None,
                lambda c: "good" if c == "RUNNING" else "bad",
                None,
                None,
                "value",
                None,
                lambda c: "bad" if (c.startswith("waiting") or " %s wants " % DOT in c) else None,
            ],
        )
        print()
    starved = [(a, why_no_resources(a, live, queues)) for a in apps if a.get("state") == "RUNNING"]
    starved = [(a, why) for a, why in starved if why]
    if starved:
        print("  " + paint("Why running apps do not get more resources", "label"))
        print()
        for a, why in starved:
            print(highlight(wrap("%s (%s): %s" % (a.get("id", "?"), a.get("name", "?"), why), "  - ", "    ")))
            print()
    if waiting_notes:
        print("  " + paint("Why apps wait (YARN's own words)", "label"))
        print()
        for app_id, text in waiting_notes:
            print(highlight(wrap("%s: %s" % (app_id, text or "no reason given by YARN"), "  - ", "    ")))
            print()
    print(
        highlight(
            wrap(
                "Doing now: for Spark apps, the active stage and its finished/total tasks, read from "
                "the Spark UI through the ResourceManager proxy. When the UI does not answer, YARN's "
                "progress is shown instead.",
                "  ",
            )
        )
    )


def watch(seconds: int, files: Dict[str, ConfigFile], rm: Optional[str], executor: Container, am: Container) -> None:
    """Redraw the queues and apps every few seconds, like yarn top. Ctrl-C stops it."""
    clear = sys.stdout.isatty()
    while True:
        live, errors = fetch_live(rm_urls(files["yarn"], rm))
        queues = build_queues(files["cs"], live)
        if clear:
            print("\033[H\033[2J", end="")
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(paint("YARN queues and apps", "section") + "   %s   every %d s, Ctrl-C stops" % (now, seconds))
        if live:
            m = live["metrics"]
            print(
                highlight(
                    "ResourceManager %s %s cluster memory %s, free %s %s %s running, %s waiting"
                    % (
                        live["base"],
                        DOT,
                        mb(m.get("totalMB")),
                        mb(m.get("availableMB")),
                        DOT,
                        m.get("appsRunning"),
                        m.get("appsPending"),
                    )
                )
            )
        for e in errors:
            print(paint(wrap(e, "  - ", "    "), "bad"))
        if live:
            if queues:
                print_queues(queues, queue_groups(files["cs"], live), None, None, am, explain=False)
                min_alloc = to_float(files["yarn"].get("yarn.scheduler.minimum-allocation-mb", "1024")) or 1024
                launcher_q = (
                    find_queue(queues, files["oozie"].get("oozie.launcher.default.queue", "default"))
                    if files["oozie"].props is not None
                    else None
                )
                print_balances(
                    queues,
                    live,
                    None,
                    launcher_q,
                    am,
                    executor,
                    False,
                    full=False,
                    launcher_size=launcher_size(files, min_alloc),
                )
            print_apps(live, queues)
        sys.stdout.flush()
        time.sleep(seconds)


def print_nodes(live: Live) -> None:
    nodes = live.get("nodes") or []
    m = live["metrics"]
    section(
        "Nodes",
        "Cluster memory for YARN: %s, free %s. Apps: %s running, %s waiting."
        % (mb(m.get("totalMB")), mb(m.get("availableMB")), m.get("appsRunning"), m.get("appsPending")),
    )
    rows = []
    for n in sorted(nodes, key=lambda n: n.get("availMemoryMB") or 0, reverse=True):
        used, free = n.get("usedMemoryMB") or 0, n.get("availMemoryMB") or 0
        # ip-10-0-0-1.ec2.internal -> ip-10-0-0-1: the short name is unique and keeps the table narrow
        rows.append(
            [
                n.get("nodeHostName", "?").split(".")[0],
                mb(used + free),
                mb(used),
                mb(free),
                "%s of %s"
                % (
                    n.get("usedVirtualCores", 0),
                    (n.get("usedVirtualCores") or 0) + (n.get("availableVirtualCores") or 0),
                ),
                str(n.get("numContainers", 0)),
                n.get("state", "?"),
            ]
        )
    table(
        ["Node", "YARN memory", "Used", "Free", "vcores used", "Containers", "State"],
        rows,
        roles=["label", None, None, "value", None, None, lambda c: "good" if c == "RUNNING" else "bad"],
    )


# (label, file, property, default, unit) - the settings behind the findings
SETTINGS = [
    ("yarn", "Largest container: memory", "yarn.scheduler.maximum-allocation-mb", "8192", " MB"),
    ("yarn", "Largest container: vcores", "yarn.scheduler.maximum-allocation-vcores", "4", ""),
    ("yarn", "Memory is rounded up to a multiple of", "yarn.scheduler.minimum-allocation-mb", "1024", " MB"),
    ("yarn", "Memory per node for YARN (this host)", "yarn.nodemanager.resource.memory-mb", "8192", " MB"),
    ("yarn", "vcores per node for YARN (this host)", "yarn.nodemanager.resource.cpu-vcores", "8", ""),
    ("yarn", "Node labels on", "yarn.node-labels.enabled", "false", ""),
    ("yarn", "Node label for ApplicationMasters", "yarn.node-labels.am.default-node-label-expression", "", ""),
    ("cs", "Containers are sized by", "yarn.scheduler.capacity.resource-calculator", "memory only", ""),
    ("cs", "AM pool of each queue (default)", "yarn.scheduler.capacity.maximum-am-resource-percent", "0.1", ""),
    ("cs", "Most apps (running + waiting)", "yarn.scheduler.capacity.maximum-applications", "10000", ""),
    ("cs", "Most running apps", "yarn.scheduler.capacity.max-parallel-apps", "", ""),
    ("cs", "Most running apps per user", "yarn.scheduler.capacity.user.max-parallel-apps", "", ""),
    ("spark", "Deploy mode", "spark.submit.deployMode", "client", ""),
    ("spark", "Queue", "spark.yarn.queue", "default", ""),
    ("spark", "Executor heap", "spark.executor.memory", "1g", ""),
    ("spark", "Executor heap (EMR default)", "spark.emr.default.executor.memory", "", ""),
    ("spark", "Executor overhead", "spark.executor.memoryOverhead", "10% of heap, min 384 MB", ""),
    ("spark", "Executor vcores", "spark.executor.cores", "1", ""),
    ("spark", "Executor vcores (EMR default)", "spark.emr.default.executor.cores", "", ""),
    ("spark", "Python worker memory per executor", "spark.executor.pyspark.memory", "", ""),
    ("spark", "Off-heap memory on", "spark.memory.offHeap.enabled", "false", ""),
    ("spark", "Dynamic allocation", "spark.dynamicAllocation.enabled", "false", ""),
    ("spark", "Fewest executors", "spark.dynamicAllocation.minExecutors", "0", ""),
    ("spark", "Most executors", "spark.dynamicAllocation.maxExecutors", "no limit", ""),
    ("spark", "Executors (without dynamic allocation)", "spark.executor.instances", "2", ""),
    ("spark", "Driver heap", "spark.driver.memory", "1g", ""),
    ("spark", "AM heap in client mode", "spark.yarn.am.memory", "512m", ""),
    ("oozie", "Launcher memory", "oozie.launcher.default.memory.mb", "2048", " MB"),
    ("oozie", "Launcher vcores", "oozie.launcher.default.vcores", "1", ""),
    ("oozie", "Launcher queue", "oozie.launcher.default.queue", "default", ""),
]


def setting_value(cf: ConfigFile, name: str, default: Optional[str], unit: str) -> str:
    """The value of one setting as the report shows it."""
    if cf.has(name):
        value = cf.get(name)
        if not value:
            return "(empty)"
        if name == "yarn.scheduler.capacity.resource-calculator":
            return "memory and vcores" if "dominant" in value.lower() else "memory only"
        if name == "yarn.scheduler.capacity.maximum-am-resource-percent" and to_float(value) is not None:
            return "%s (%s)" % (value, pct(to_float(value)))
        return value + unit
    if name == "spark.executor.memory" and cf.has("spark.emr.default.executor.memory"):
        return "EMR default (below)"
    if name == "spark.executor.cores" and cf.has("spark.emr.default.executor.cores"):
        return "EMR default (below)"
    return "%s%s (default)" % (default, unit) if default else "not set"


# Settings of each queue in capacity-scheduler.xml: (label, suffix, default, unit, leaf queues only)
QUEUE_SETTINGS = [
    ("Guaranteed share of the parent", "capacity", None, "%", False),
    ("Most share of the parent", "maximum-capacity", "100", "%", False),
    ("State", "state", "RUNNING", "", False),
    ("One user: x guaranteed share", "user-limit-factor", "1", "", True),
    ("Smallest share per active user", "minimum-user-limit-percent", "100", "%", True),
    ("AM pool share", "maximum-am-resource-percent", None, "", True),
    ("Largest container: memory", "maximum-allocation-mb", None, " MB", True),
    ("Most running apps", "max-parallel-apps", None, "", True),
]


def queue_paths(cs: ConfigFile) -> List[Tuple[str, bool]]:
    """Every queue in capacity-scheduler.xml, parents first: [(path, is_leaf)]."""
    out: List[Tuple[str, bool]] = []

    def walk(path: str) -> None:
        children = [c.strip() for c in cs.get("yarn.scheduler.capacity.%s.queues" % path, "").split(",") if c.strip()]
        out.append((path, not children))
        for c in children:
            walk("%s.%s" % (path, c))

    walk("root")
    return out[1:]  # root itself always has 100%


def queue_setting_rows(cs: ConfigFile) -> List[Tuple[str, bool, List[Tuple[str, str, str]]]]:
    """Rows (label, property, value) for the settings of each queue, with the default when not set."""
    global_am = cs.get("yarn.scheduler.capacity.maximum-am-resource-percent")
    global_apps = cs.get("yarn.scheduler.capacity.max-parallel-apps")
    blocks: List[Tuple[str, bool, List[Tuple[str, str, str]]]] = []
    for path, leaf in queue_paths(cs):
        rows: List[Tuple[str, str, str]] = []
        for label, suffix, default, unit, leaf_only in QUEUE_SETTINGS:
            if leaf_only and not leaf:
                continue
            name = "yarn.scheduler.capacity.%s.%s" % (path, suffix)
            if cs.has(name):
                raw = cs.get(name)
                value = raw + unit if raw and to_float(raw) is not None else (raw or "(empty)")
            elif suffix == "maximum-am-resource-percent":
                value = ("%s (cluster-wide value)" % global_am) if global_am else "0.1 (default)"
            elif suffix == "maximum-allocation-mb":
                value = "cluster limit (default)"
            elif suffix == "max-parallel-apps":
                value = ("%s (cluster-wide value)" % global_apps) if global_apps else "no limit (default)"
            elif default is None:
                value = "not set"
            else:
                value = "%s%s (default)" % (default, unit)
            rows.append((label, name, value))
        blocks.append((path, leaf, rows))
    return blocks


def print_settings(files: Dict[str, ConfigFile]) -> None:
    section(
        "Settings",
        'The settings behind the findings. "(default)" means that the file does not '
        'have the setting, so YARN, Spark or Oozie uses its default. "(cluster-wide value)" '
        "means that the queue has no own value and uses the one for all queues.",
    )
    rows = {
        key: [
            (label, name, setting_value(files[key], name, default, unit))
            for f, label, name, default, unit in SETTINGS
            if f == key
        ]
        for key in files
    }
    queue_blocks = queue_setting_rows(files["cs"]) if files["cs"].props is not None else []
    all_rows = [r for rs in rows.values() for r in rs] + [r for _, _, rs in queue_blocks for r in rs]
    w_label = max(len(r[0]) for r in all_rows)
    w_name = max(len(r[1]) for r in all_rows)

    def show(label: str, name: str, value: str) -> None:
        role = "default" if (value.endswith("(default)") or value == "not set") else "value"
        print(
            "  %s  %s  %s"
            % ("%-*s" % (w_label, label), paint(name, "prop") + " " * (w_name - len(name)), paint(value, role))
        )

    print(
        "  %s  %s  %s"
        % (
            paint("%-*s" % (w_label, "Description"), "label"),
            paint("%-*s" % (w_name, "Setting"), "label"),
            paint("Value", "label"),
        )
    )
    for key in ("yarn", "cs", "spark", "oozie"):
        cf = files[key]
        print()
        print("  " + paint(cf.path, "file"))
        if cf.props is None:
            print("    " + paint('not read, see "Could not read" above', "bad"))
            continue
        for label, name, value in rows[key]:
            show(label, name, value)
        if key == "cs":
            for path, leaf, qrows in queue_blocks:
                print()
                print(
                    "  "
                    + paint("Queue %s" % path, "queue")
                    + ("" if leaf else "  (parent queue: its children share it)")
                )
                for label, name, value in qrows:
                    show(label, name, value)


def print_header(
    emr: Optional[Json], files: Dict[str, ConfigFile], live: Optional[Live], live_errors: List[str], live_wanted: bool
) -> None:
    title = "YARN and Spark resource report"
    print(paint(title, "section"))
    print(paint("=" * len(title), "rule"))
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rows = [("Created", now)]
    if emr:
        rows.append(
            (
                "Cluster",
                "%s, %s (%s)"
                % (emr["id"], emr["release"] or "release unknown", emr["groups"] or "instance groups unknown"),
            )
        )
        if emr["is_primary"] is None:
            rows.append(("This host", "unknown (EMR node)"))
        else:
            rows.append(
                (
                    "This host",
                    "primary node"
                    if emr["is_primary"]
                    else "not the primary node: run the script on the primary node for the right config files",
                )
            )
    if live:
        rows.append(("Live data", "from the ResourceManager at %s" % live["base"]))
    elif live_wanted:
        rows.append(("Live data", "none: could not read the ResourceManager (see below)"))
    else:
        rows.append(("Live data", "not used. Add --live to see queue usage, AM pools in MB and nodes."))
    for label, value in rows:
        text = highlight(wrap(value, " " * 11, " " * 11))
        print(paint(label + ":", "label") + " " * (11 - len(label) - 1) + text[11:])

    errors = [cf for cf in files.values() if cf.error]
    emr_error = emr.get("error") if emr else None
    if errors or live_errors or emr_error:
        print()
        print(paint("COULD NOT READ", "error"))
        print(paint("-" * len("Could not read"), "rule"))
        print(wrap("The report below is missing the parts that come from these sources."))
        print()
        bullet = paint("  - ", "error")
        items = []
        for cf in errors:
            items.append(
                bullet
                + highlight(wrap(cf.error or "", "    ", "    "))[4:]
                + "\n"
                + highlight(wrap("Set the directory with: %s" % cf.option, "    "))
            )
        if emr_error:
            items.append(bullet + highlight(wrap(emr_error, "    ", "    "))[4:])
        for e in live_errors:
            items.append(bullet + highlight(wrap(e, "    ", "    "))[4:])
        print("\n\n".join(items))
        if live_errors and not live:
            print(
                highlight(
                    wrap(
                        "Give another address with --rm http://<host>:8088, or use --no-live to skip the live data.",
                        "    ",
                    )
                )
            )


# ================================================================== main


def main() -> None:
    if sys.version_info < (3, 6):
        sys.exit("This script needs Python 3.6 or later (found %s)." % sys.version.split()[0])
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--hadoop-conf",
        default=first_dir(os.environ.get("HADOOP_CONF_DIR"), os.environ.get("YARN_CONF_DIR"), "/etc/hadoop/conf"),
        help="directory with yarn-site.xml and capacity-scheduler.xml",
    )
    ap.add_argument(
        "--spark-conf",
        default=first_dir(os.environ.get("SPARK_CONF_DIR"), "/etc/spark/conf"),
        help="directory with spark-defaults.conf",
    )
    ap.add_argument(
        "--oozie-conf",
        default=first_dir(os.environ.get("OOZIE_CONF_DIR"), "/etc/oozie/conf"),
        help="directory with oozie-site.xml",
    )
    ap.add_argument(
        "--live",
        dest="live",
        action="store_true",
        default=None,
        help="read live usage from the ResourceManager REST API (default on EMR)",
    )
    ap.add_argument("--no-live", dest="live", action="store_false", help="do not contact the ResourceManager")
    ap.add_argument("--rm", help="ResourceManager web address, for example http://ip-10-0-0-1:8088")
    ap.add_argument("--all", action="store_true", help="also list every other property in the files")
    ap.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default="auto",
        help="colour the report: auto (only on a terminal, the default), always or never. NO_COLOR turns it off",
    )
    ap.add_argument(
        "--queue",
        action="append",
        metavar="NAME",
        help="start the report with a one-screen card for this queue: can a new job run now, and if not, "
        "why (more than once for more queues). Needs live data",
    )
    ap.add_argument(
        "--details",
        action="store_true",
        help="also print the balance sheets: every number behind the queue cards, worked out step by step",
    )
    ap.add_argument(
        "--watch",
        type=int,
        metavar="SECONDS",
        help="show only the queues and apps, redrawn every SECONDS (like yarn top). Needs live data",
    )
    args = ap.parse_args()
    use_color(args.color)
    if args.watch is not None and args.watch < 2:
        ap.error("--watch needs 2 seconds or more")
    if args.watch is not None and args.live is False:
        ap.error("--watch needs live data; remove --no-live")

    files = {
        "yarn": ConfigFile("yarn", os.path.join(args.hadoop_conf, "yarn-site.xml"), "yarn-site.xml"),
        "cs": ConfigFile("cs", os.path.join(args.hadoop_conf, "capacity-scheduler.xml"), "capacity-scheduler.xml"),
        "spark": ConfigFile("spark", os.path.join(args.spark_conf, "spark-defaults.conf"), "spark-defaults.conf"),
        "oozie": ConfigFile("oozie", os.path.join(args.oozie_conf, "oozie-site.xml"), "oozie-site.xml"),
    }
    files["yarn"].option = files["cs"].option = "--hadoop-conf DIR or HADOOP_CONF_DIR"
    files["spark"].option = "--spark-conf DIR or SPARK_CONF_DIR"
    files["oozie"].option = "--oozie-conf DIR or OOZIE_CONF_DIR (ignore this if you do not use Oozie)"
    for key, cf in files.items():
        (read_spark_defaults if key == "spark" else read_hadoop_xml)(cf)

    if args.watch:
        min_alloc = to_float(files["yarn"].get("yarn.scheduler.minimum-allocation-mb", "1024")) or 1024
        executor, am = spark_containers(files["spark"], min_alloc)[:2]
        watch(args.watch, files, args.rm, executor, am)
        return

    emr = read_emr_info()
    live_wanted = (emr is not None) if args.live is None else args.live
    live, live_errors = fetch_live(rm_urls(files["yarn"], args.rm)) if live_wanted else (None, [])

    queues = build_queues(files["cs"], live)
    min_alloc = to_float(files["yarn"].get("yarn.scheduler.minimum-allocation-mb", "1024")) or 1024
    executor, am, deploy = spark_containers(files["spark"], min_alloc)
    findings = find_problems(files, queues, live, executor, am, deploy)

    print_header(emr, files, live, live_errors, live_wanted)
    if args.queue:
        print_queue_cards(
            args.queue,
            queues,
            live,
            am,
            executor,
            files,
            launcher_size(files, min_alloc),
        )
    print_findings(findings)
    print_containers(executor, am, deploy, files["spark"])
    if queues:
        print_queues(
            queues,
            queue_groups(files["cs"], live),
            find_queue(queues, files["spark"].get("spark.yarn.queue", "default")),
            find_queue(queues, files["oozie"].get("oozie.launcher.default.queue", "default"))
            if files["oozie"].props is not None
            else None,
            am,
        )
    if live and queues:
        print_balances(
            queues,
            live,
            find_queue(queues, files["spark"].get("spark.yarn.queue", "default")),
            find_queue(queues, files["oozie"].get("oozie.launcher.default.queue", "default"))
            if files["oozie"].props is not None
            else None,
            am,
            executor,
            "dominant" in files["cs"].get("yarn.scheduler.capacity.resource-calculator", "").lower(),
            only=args.queue,
            launcher_size=launcher_size(files, min_alloc),
            details=args.details,
        )
    if live:
        print_apps(live, queues)
        print_nodes(live)
    print_settings(files)

    section("Terms")
    print_terms(TERMS)

    if args.all:
        known = {s[2] for s in SETTINGS}
        if files["cs"].props is not None:
            known |= {name for _, _, qrows in queue_setting_rows(files["cs"]) for _, name, _ in qrows}
        for cf in files.values():
            if not cf.props:
                continue
            section("All other properties in %s" % cf.path)
            for name in sorted(cf.props):
                if name in known:  # already shown in the Settings table
                    continue
                value = cf.props[name]
                if value and SECRET_NAME.search(name):
                    value = "****** (hidden)"
                else:  # a password inside a URL, for example jdbc:mysql://user:pass@host/db
                    value = re.sub(r"(://[^/:@\s]+:)[^@/\s]+@", r"\1******@", value)
                print("%s = %s" % (paint(name, "prop"), paint(value, "value")))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:  # for example: ... | head
        sys.exit(0)
