"""A fake ResourceManager (and Spark UI proxy) for the tests: a two-node cluster of 12 GB nodes with one
service account, svc_spark, and one Oozie launcher. Every name, host and ID in it is made up.

Modes change the cluster state:
  ok      apps running, svc_spark's AM limit full, one app waiting in ACCEPTED (the default)
  room    the same, with room on the nodes
  acl     only the apps of user hadoop are visible (YARN ACLs)
  single  YARN sends one app, one queue and one node as objects instead of lists
  nested  a parent queue with children, one child sent as an object without queuePath
  eff     Hadoop 3 effective resources (effectiveMinResource / effectiveMaxResource)
  dead    two queues; an Oozie job (launcher + Spark AM) needs more than the user's AM limit
  clear   two queues with room everywhere
  two     like clear (the launchers run in another queue when the config says so)
  etlstopped  like two, but the ResourceManager runs queue root.etl STOPPED (the file says RUNNING)
  over    one user above the queue's guaranteed share
  fair    the Fair Scheduler, which the report does not cover
  401 403 the ResourceManager refuses every request
  403scheduler  only the scheduler request is refused
  standby the address belongs to a standby ResourceManager
"""

import copy
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, Dict, List, Optional, Tuple

MODES = (
    "ok",
    "room",
    "acl",
    "single",
    "nested",
    "eff",
    "dead",
    "clear",
    "two",
    "etlstopped",
    "over",
    "fair",
    "401",
    "403",
    "403scheduler",
    "standby",
)

AM_FULL = (
    "[Tue Oct 14 01:02:03 +0000 2026] Application is added to the scheduler and is not yet activated. User's AM "
    "resource limit exceeded.  Details : AM Partition = <DEFAULT_PARTITION>; AM Resource Request = <memory:896, "
    "vCores:1>; User AM Resource Limit of the queue = <memory:2688, vCores:3>; User AM Resource Used = "
    "<memory:2688, vCores:3>; "
)


def _requests(n: int, mb: int) -> List[Dict[str, Any]]:
    return [
        {"resourceName": "*", "numContainers": n, "capability": {"memory": mb, "vCores": 4}},
        {"resourceName": "ip-10-0-0-2", "numContainers": n, "capability": {"memory": mb, "vCores": 4}},
    ]


def _app(
    n: int, name: str, user: str, state: str, started: int, elapsed: int, cont: int, mb: int, vc: int
) -> Dict[str, Any]:
    return {
        "id": "application_1760400000000_%04d" % n,
        "name": name,
        "user": user,
        "queue": "default",
        "state": state,
        "applicationType": "MAPREDUCE" if name.startswith("oozie:") else "SPARK",
        "startedTime": started,
        "elapsedTime": elapsed,
        "runningContainers": cont,
        "allocatedMB": mb,
        "allocatedVCores": vc,
        "progress": 0.0 if state == "ACCEPTED" else 35.0,
    }


def build(mode: str) -> Dict[str, Any]:
    """The answers of the fake ResourceManager in this mode: REST documents, apps, Spark stages and settings."""
    free = (6000, 6000) if mode == "room" else (1288, 1912)
    nodes = [
        {
            "nodeHostName": host,
            "state": "RUNNING",
            "usedMemoryMB": 12288 - f,
            "availMemoryMB": f,
            "usedVirtualCores": 3,
            "availableVirtualCores": 1,
            "numContainers": cont,
        }
        for host, f, cont in (("ip-10-0-0-2", free[0], 4), ("ip-10-0-0-3", free[1], 3))
    ]
    launcher = "oozie:launcher:T=spark:W=nightly:A=load:ID=0000003-251014-oozie-W"
    apps = [
        _app(11, launcher, "hadoop", "RUNNING", 500, 900000, 1, 2048, 1),
        _app(12, "daily_payments", "svc_spark", "RUNNING", 1000, 842000, 3, 12288, 9),
        _app(13, "merchant_sync", "svc_spark", "RUNNING", 2000, 400000, 2, 6144, 5),
        _app(14, "recon_report", "svc_spark", "RUNNING", 3000, 72000, 1, 896, 1),
        _app(15, "audit_export", "svc_spark", "ACCEPTED", 0, 270000, -1, -1, -1),
    ]
    apps[1]["resourceRequests"] = _requests(3, 5248)
    apps[2]["resourceRequests"] = _requests(2, 5248) if mode == "room" else []
    apps[4]["diagnostics"] = AM_FULL
    used = sum(a["allocatedMB"] for a in apps if a["allocatedMB"] > 0)
    default = {
        "queuePath": "root.default",
        "queueName": "default",
        "capacity": 100,
        "absoluteCapacity": 100.0,
        "absoluteMaxCapacity": 100.0,
        "maxCapacity": 100,
        "resourcesUsed": {"memory": used, "vCores": 6},
        "AMResourceLimit": {"memory": 12288, "vCores": 4},
        "usedAMResource": {"memory": 4736, "vCores": 4},
        "userAMResourceLimit": {"memory": 2688, "vCores": 3},
        "users": {
            "user": [
                {"username": "svc_spark", "AMResourceUsed": {"memory": 2688}},
                {"username": "hadoop", "AMResourceUsed": {"memory": 2048}},
            ]
        },
        "numActiveApplications": 4,
        "numPendingApplications": 1,
        "userLimitFactor": 1.0,
        "state": "RUNNING",
        "type": "capacitySchedulerLeafQueueInfo",
    }
    scheduler = {"type": "capacityScheduler", "queueName": "root", "queues": {"queue": [default]}}
    metrics = {
        "totalMB": 24576,
        "availableMB": sum(free),
        "appsRunning": 4,
        "appsPending": 1,
        "totalVirtualCores": 8,
        "availableVirtualCores": 2,
        "activeNodes": 2,
    }

    def one_app(alloc: int) -> None:
        apps[:] = [apps[1]]
        apps[0].update({"allocatedMB": alloc, "resourceRequests": []})

    def cluster_free(per_node: int) -> None:
        metrics["availableMB"] = 2 * per_node
        for n in nodes:
            n["availMemoryMB"], n["usedMemoryMB"] = per_node, 12288 - per_node

    if mode == "nested":
        default.update({"capacity": 60, "absoluteCapacity": 60.0})

        def leaf(name: str, cap: int, abs_cap: float) -> Dict[str, Any]:
            return {
                "queueName": name,
                "capacity": cap,
                "absoluteCapacity": abs_cap,
                "absoluteMaxCapacity": 100.0,
                "maxCapacity": 100,
                "resourcesUsed": {"memory": 1024},
                "numActiveApplications": 0,
                "numPendingApplications": 0,
                "state": "RUNNING",
            }

        team = {
            "queueName": "team",
            "capacity": 30,
            "absoluteCapacity": 12.0,
            "absoluteMaxCapacity": 100.0,
            "maxCapacity": 100,
            "resourcesUsed": {"memory": 2048},
            "queues": {"queue": leaf("only", 100, 12.0)},  # one child sent as an object, no queuePath
        }
        scheduler["queues"]["queue"].append(
            {
                "queuePath": "root.analytics",
                "queueName": "analytics",
                "capacity": 40,
                "absoluteCapacity": 40.0,
                "absoluteMaxCapacity": 100.0,
                "maxCapacity": 100,
                "resourcesUsed": {"memory": 3072},
                "queues": {"queue": [leaf("adhoc", 70, 28.0), team]},
            }
        )
    elif mode == "eff":
        default["capacities"] = {
            "queueCapacitiesByPartition": [
                {
                    "partitionName": "",
                    "effectiveMinResource": {"memory": 20480, "vCores": 6},
                    "effectiveMaxResource": {"memory": 22528, "vCores": 7},
                }
            ]
        }
        default.update({"resourcesUsed": {"memory": 6144, "vCores": 2}, "usedAMResource": {"memory": 896}})
        default["users"] = {
            "user": {
                "username": "svc_spark",
                "resourcesUsed": {"memory": 6144, "vCores": 2},
                "AMResourceUsed": {"memory": 896},
                "userResourceLimit": {"memory": 20480, "vCores": 6},
                "numActiveApplications": 1,
                "numPendingApplications": 0,
            }
        }
        cluster_free(9216)
        one_app(6144)
    elif mode in ("dead", "clear", "two", "etlstopped"):
        dead = mode == "dead"
        default.update(
            {
                "capacity": 70,
                "absoluteCapacity": 70.0,
                "resourcesUsed": {"memory": 6144, "vCores": 2},
                "usedAMResource": {"memory": 0 if dead else 896},
                "userAMResourceLimit": {"memory": 2688 if dead else 6144},
            }
        )
        default["users"] = {
            "user": {
                "username": "svc_spark",
                "resourcesUsed": {"memory": 6144, "vCores": 2},
                "AMResourceUsed": {"memory": 0 if dead else 896},
                "numActiveApplications": 1,
                "numPendingApplications": 0,
            }
        }
        scheduler["queues"]["queue"].append(
            {
                "queuePath": "root.etl",
                "queueName": "etl",
                "capacity": 30,
                "absoluteCapacity": 30.0,
                "absoluteMaxCapacity": 100.0,
                "maxCapacity": 100,
                "resourcesUsed": {"memory": 0, "vCores": 0},
                "AMResourceLimit": {"memory": 3686},
                "usedAMResource": {"memory": 0},
                "userAMResourceLimit": {"memory": 3686},
                "userLimitFactor": 1.0,
                "numActiveApplications": 0,
                "numPendingApplications": 0,
                "state": "STOPPED" if mode == "etlstopped" else "RUNNING",
            }
        )
        cluster_free(9216)
        one_app(6144)
    elif mode == "over":  # one user above the queue's guarantee; YARN's userResourceLimit tracks its use
        default.update(
            {
                "capacity": 40,
                "absoluteCapacity": 40.0,
                "userLimitFactor": 2.5,
                "resourcesUsed": {"memory": 14336, "vCores": 4},
                "usedAMResource": {"memory": 896},
                "userAMResourceLimit": {"memory": 6144},
            }
        )
        default["users"] = {
            "user": {
                "username": "svc_spark",
                "resourcesUsed": {"memory": 14336, "vCores": 4},
                "AMResourceUsed": {"memory": 896},
                "userResourceLimit": {"memory": 14368, "vCores": 5},
                "numActiveApplications": 1,
                "numPendingApplications": 0,
            }
        }
        cluster_free(5120)
        one_app(14336)
    elif mode == "fair":
        scheduler["type"] = "fairScheduler"

    visible = [a for a in apps if a["user"] == "hadoop"] if mode == "acl" else apps
    single = mode == "single"  # YARN sends a list of one as the object itself
    return {
        "/ws/v1/cluster/metrics": {"clusterMetrics": metrics},
        "/ws/v1/cluster/scheduler": {
            "scheduler": {"schedulerInfo": dict(scheduler, queues={"queue": default}) if single else scheduler}
        },
        "/ws/v1/cluster/nodes": {"nodes": {"node": nodes[0] if single else nodes}},
        "/ws/v1/cluster/apps": {"apps": {"app": visible[1] if mode == "single" else visible}},
        "stages": {
            "application_1760400000000_0012": [
                {
                    "stageId": 3,
                    "name": "groupBy at report.py:42",
                    "numTasks": 800,
                    "numCompleteTasks": 412,
                    "numActiveTasks": 12,
                }
            ],
            "application_1760400000000_0013": [
                {"stageId": 2, "name": "count at sync.py:20", "numTasks": 8, "numCompleteTasks": 0, "numActiveTasks": 0}
            ],
        },
        "environment": {
            "application_1760400000000_0013": {
                "sparkProperties": [
                    ["spark.dynamicAllocation.enabled", "true"],
                    ["spark.dynamicAllocation.maxExecutors", "1"],
                    ["spark.app.name", "merchant_sync"],
                ]
            }
        },
    }


class _Server(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def _handler(mode: str, docs: Dict[str, Any]) -> type:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def reply(
            self, code: int, body: bytes, kind: str = "application/json", extra: Optional[Tuple[str, str]] = None
        ) -> None:
            self.send_response(code)
            self.send_header("Content-Type", kind)
            if extra:
                self.send_header(*extra)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - the name http.server calls
            path = self.path.split("?")[0]
            if mode in ("401", "403"):
                auth = ("WWW-Authenticate", "Negotiate") if mode == "401" else None
                return self.reply(int(mode), b"<html>denied</html>", "text/html", auth)
            if mode == "403scheduler" and path.endswith("scheduler"):
                return self.reply(403, b"forbidden", "text/plain")
            if mode == "standby":
                return self.reply(200, b"This is standby RM. The redirect url is: http://x", "text/plain")
            if path.startswith("/proxy/"):
                app_id, what = path.split("/")[2], path.split("/")[-1]
                found = docs.get(what, {}).get(app_id) if what in ("stages", "environment") else None
                if found is not None:
                    return self.reply(200, json.dumps(found).encode())
                return self.reply(200, b"<html>redirecting</html>", "text/html")
            if path in docs:
                return self.reply(200, json.dumps(docs[path]).encode())
            return self.reply(404, b"{}")

    return Handler


def serve(mode: str = "ok") -> Tuple[HTTPServer, str]:
    """Start a fake ResourceManager on a free local port, in a background thread. Returns (server, its URL)."""
    server = _Server(("127.0.0.1", 0), _handler(mode, copy.deepcopy(build(mode))))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, "http://127.0.0.1:%d" % server.server_address[1]


if __name__ == "__main__":  # python tests/fake_rm.py [MODE] [PORT]: run one by hand
    import sys

    m = sys.argv[1] if len(sys.argv) > 1 else "ok"
    srv = _Server(("127.0.0.1", int(sys.argv[2]) if len(sys.argv) > 2 else 18088), _handler(m, build(m)))
    print("fake ResourceManager (%s) on http://127.0.0.1:%d" % (m, srv.server_address[1]))
    srv.serve_forever()
