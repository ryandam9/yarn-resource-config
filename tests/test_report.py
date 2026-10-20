"""Run the report against the fake ResourceManager (tests/fake_rm.py) and the test configs (tests/configs).

    python -m unittest discover -s tests -v

Only the standard library, so it runs on every Python the script supports (3.6 and later).
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from typing import Dict, List, Optional, Tuple

import fake_rm

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "yarn_resource_config.py")
CONFIGS = sorted(os.listdir(os.path.join(HERE, "configs")))
FLAGS = [[], ["--details"], ["--queue", "default"], ["--all", "--color", "always"]]

# Advice the report must never give, and text it must not show (see the README).
NEVER = [
    "give the launchers their own queue",
    "Best fix",
    "client mode:",
    "Traceback",
]


def run(
    config: str,
    url: Optional[str],
    args: List[str],
    env: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    configs: str = os.path.join(HERE, "configs"),
) -> Tuple[int, str]:
    """Run the report once on configs/<config>. url None means --no-live. Returns (exit code, stdout + stderr)."""
    base = os.path.join(configs, config)
    cmd = [
        sys.executable,
        SCRIPT,
        "--hadoop-conf",
        os.path.join(base, "hadoop"),
        "--spark-conf",
        os.path.join(base, "spark"),
        "--oozie-conf",
        os.path.join(base, "oozie"),
    ]
    cmd += ["--live", "--rm", url] if url else ["--no-live"]
    full_env = dict(os.environ, EMR_INFO_DIR=os.path.join(base, "info"))
    full_env.update(env or {})
    p = subprocess.run(cmd + args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=full_env, timeout=timeout)
    return p.returncode, p.stdout.decode("utf-8", "replace")


def reads_anything() -> bool:
    """True when file permissions do not stop this process (root, or CAP_DAC_OVERRIDE)."""
    fd, path = tempfile.mkstemp()
    os.close(fd)
    os.chmod(path, 0)
    try:
        with open(path):
            return True
    except OSError:
        return False
    finally:
        os.remove(path)


class Report(unittest.TestCase):
    servers: Dict[str, str] = {}

    @classmethod
    def setUpClass(cls) -> None:
        cls._running = []
        for mode in fake_rm.MODES:
            server, url = fake_rm.serve(mode)
            cls._running.append(server)
            cls.servers[mode] = url

    @classmethod
    def tearDownClass(cls) -> None:
        for server in cls._running:
            server.shutdown()
            server.server_close()

    def check(self, rc: int, out: str) -> None:
        self.assertEqual(rc, 0, out[-2000:])
        for text in NEVER:
            self.assertNotIn(text, out)

    def test_every_config_mode_and_flag(self) -> None:
        for config in CONFIGS:
            for mode in (None,) + fake_rm.MODES:
                for flags in FLAGS:
                    with self.subTest(config=config, mode=mode, flags=" ".join(flags)):
                        self.check(*run(config, self.servers[mode] if mode else None, flags))

    def test_answer_first(self) -> None:
        rc, out = run("default", self.servers["ok"], ["--color", "never"])
        self.check(rc, out)
        order = ["FINDINGS", "CAN A NEW JOB START?", "APPS", "QUEUES NOW", "SETTINGS"]
        places = [out.index("\n%s\n" % title) for title in order]
        self.assertEqual(places, sorted(places))
        self.assertIn("[PROBLEM] An Oozie job can never start in queue root.default", out)
        self.assertIn("A new Spark job from Oozie can NEVER start here now.", out)
        self.assertIn("oozie.launcher.default.memory.mb 2048 -> 1792", out)
        self.assertNotIn("Room left now", out)  # the balance sheets come only with --details

    def test_details(self) -> None:
        rc, out = run("default", self.servers["eff"], ["--details", "--color", "never"])
        self.check(rc, out)
        self.assertIn("Room left now", out)
        self.assertIn("Max: 92% of the cluster (maximum-capacity)", out)
        self.assertIn("NO: it can NEVER start.", out)
        # waiting or another account does not help a job that never fits
        never = out[out.index("NO: it can NEVER start.") :]
        self.assertNotIn("Wait until one of this user's apps ends.", never[: never.index("\n\n\n")])

    def test_service_accounts(self) -> None:
        # Spark jobs run as service accounts, mapped to queues with queue-mappings: the report names them
        # and never says "user", apart from YARN's own property names and messages.
        rc, out = run("default", self.servers["ok"], ["--details", "--color", "never"])
        self.check(rc, out)
        self.assertIn("QUEUE root.default  \u00b7  service account svc_spark\n", out)
        self.assertIn("QUEUE root.default  \u00b7  service account svc_reports (no jobs now)", out)  # mapped only
        self.assertIn("Service accounts mapped here", out)
        self.assertIn("svc_spark, svc_reports", out)  # u:%user:%user names no account
        allowed = re.compile(
            r"user-limit-factor|minimum-user-limit-percent|userAMResourceLimit|user\.max-parallel-apps|"
            r"%user|YARN calls (it|a service account) a user|User's AM resource limit|User AM Resource"
        )
        text = allowed.sub("", re.sub(r"\s+", " ", out.replace(os.path.dirname(HERE), "")))  # wrapped lines joined
        found = re.search(r".{0,60}\b[Uu]ser(s|'s)?\b.{0,60}", text)
        self.assertIsNone(found, found and found.group(0))

    def test_oozie_launchers(self) -> None:
        # Most jobs come from Oozie: the launcher is part of every job and counts against the AM limits.
        rc, out = run("default", self.servers["ok"], ["--color", "never"])
        self.check(rc, out)
        self.assertRegex(out, r"\n  Oozie launcher +- +- +- +2\.00 GB +1\n")  # in "What one Spark job asks"
        self.assertIn("One Spark job from Oozie, before any executor: launcher 2.00 GB + Spark AM 0.88 GB", out)
        self.assertIn("Oozie launchers running now: 1, holding 2.00 GB of AM limit", out)  # the finding
        joined = re.sub(r"\s+", " ", out)  # wrapped lines joined
        self.assertIn("1 Oozie launcher holds 2.00 GB (an AM that waits for its Spark job)", joined)  # Apps

    def test_account_and_group(self) -> None:
        # The service account or group comes from the command line, as given: no lookup of group members.
        rc, out = run("default", self.servers["ok"], ["--queue", "default", "--account", "svc_reports", "--details"])
        self.check(rc, out)
        cards = re.findall(r"\nQUEUE .*", out)
        self.assertEqual(cards, ["\nQUEUE root.default  \u00b7  service account svc_reports (no jobs now)"])
        self.assertIn("As service account svc_reports, with spark-submit:", out)
        self.assertNotIn("As service account svc_spark", out)
        rc, out = run("default", self.servers["ok"], ["--queue", "default", "--group", "analysts", "--color", "never"])
        self.check(rc, out)
        self.assertIn("QUEUE root.default  \u00b7  group analysts: one of its accounts, with no jobs now", out)
        self.assertIn("AM limit of one analysts account", out)
        rc, out = run("default", self.servers["ok"], ["--account", "svc_reports"])
        self.assertEqual(rc, 2)  # needs --queue
        self.assertIn("--account and --group need --queue", out)
        rc, out = run("queues", None, ["--color", "never"])  # g:analysts:analytics in the settings
        self.check(rc, out)
        self.assertRegex(
            out, r"Service accounts mapped here +yarn\.scheduler\.capacity\.queue-mappings +group analysts"
        )

    def test_help_has_examples(self) -> None:
        p = subprocess.run([sys.executable, SCRIPT, "--help"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = p.stdout.decode("utf-8", "replace")
        self.assertEqual(p.returncode, 0, out)
        for text in (
            "examples",
            "--queue etl --account svc_etl",
            "--queue etl --group analysts",
            "--watch 30",
            "--no-live",
            "--details",
            "releases/latest/download/yarn_resource_config.py",
        ):
            self.assertIn(text, out)

    def test_resourcemanager_wins_over_the_file(self) -> None:
        # The file says root.etl is RUNNING, the ResourceManager runs it STOPPED: YARN's own state counts.
        rc, out = run("launcher-in-etl", self.servers["etlstopped"], ["--details", "--color", "never"])
        self.check(rc, out)
        self.assertIn("[PROBLEM] Queue root.etl is STOPPED", out)  # the Oozie launcher queue here
        self.assertNotIn("queue root.etl is RUNNING", out)
        self.assertIn("queue root.etl is STOPPED: it takes no new apps", out)
        # a config that has root.etl: the difference to the file is a finding of its own
        rc, out = run("queues", self.servers["etlstopped"], ["--color", "never"])
        self.check(rc, out)
        self.assertIn("[WARNING] Queue root.etl runs with other settings than capacity-scheduler.xml says", out)
        self.assertIn("state: the file says RUNNING, the ResourceManager runs with STOPPED", out)

    def test_fixes_make_room_for_one_more_job(self) -> None:
        # 6.00 GB AM limit per account, 2.88 GB per Oozie job: 2 fit; the fix must make room for a third.
        rc, out = run("default", self.servers["clear"], ["--color", "never"])
        self.check(rc, out)
        joined = re.sub(r"\s+", " ", out)
        self.assertIn("allow about 2 Oozie jobs at once per service account", joined)
        m = re.search(r"each service account's AM limit becomes about ([\d.]+) GB", joined)
        assert m is not None, joined
        self.assertGreaterEqual(float(m.group(1)), 3 * 2.88)

    def test_details_hint_only_without_details(self) -> None:
        hint = "run again with --details"
        rc, out = run("default", self.servers["ok"], ["--queue", "default", "--color", "never"])
        self.assertIn(hint, out)
        rc, out = run("default", self.servers["ok"], ["--queue", "default", "--details", "--color", "never"])
        self.check(rc, out)
        self.assertNotIn(hint, out)

    def test_settings_in_gb(self) -> None:
        # With live data, each queue setting also says what it means in GB at the current cluster size (24 GB).
        rc, out = run("queues", self.servers["ok"], ["--color", "never"])
        self.check(rc, out)
        settings = out[out.index("\nSETTINGS\n") :]
        for text in (
            "40% (9.60 GB)",  # capacity of root.etl
            "80% (19.20 GB)",  # maximum-capacity
            "2 (19.20 GB = 2 \u00d7 guaranteed 9.60 GB)",  # user-limit-factor
            "0.3 (30% of the queue: 2.88 GB to 5.76 GB)",  # maximum-am-resource-percent
            "50% (2.40 GB; 10% of the cluster)",  # a child of root.analytics
            "100% (default; 9.60 GB of the guaranteed 9.60 GB)",  # minimum-user-limit-percent
            "0.5 (50% of each queue)",
            "12288 MB = 12.00 GB",
            "4743M = 4.63 GB",
        ):
            self.assertIn(text, settings)
        rc, out = run("queues", None, ["--color", "never"])  # no cluster size without live data
        self.check(rc, out)
        self.assertIn("40%\n", out)
        self.assertNotIn("40% (9.60 GB)", out)

    def test_memory_in_gb(self) -> None:
        rc, out = run("default", self.servers["ok"], ["--color", "never"])
        self.check(rc, out)
        report = out[: out.index("\nSETTINGS\n")]  # the settings keep their configured values
        for line in report.splitlines():
            if " MB" in line:  # only a property value as configured, or Spark's own rule
                quoted = ("spark.yarn.am.memory", "oozie.launcher.default.memory.mb", "at least 384 MB")
                self.assertTrue(any(q in line for q in quoted), line)

    def test_ascii_terminal(self) -> None:
        env = {"LC_ALL": "C", "LANG": "C", "PYTHONIOENCODING": "ascii"}
        for mode in ("ok", "nested"):
            with self.subTest(mode=mode):
                rc, out = run("default", self.servers[mode], ["--details"], env)
                self.check(rc, out)
                self.assertEqual([c for c in out if ord(c) > 127], [])

    def test_watch(self) -> None:
        base = os.path.join(HERE, "configs", "default")
        cmd = [sys.executable, SCRIPT, "--hadoop-conf", os.path.join(base, "hadoop"), "--spark-conf"]
        cmd += [os.path.join(base, "spark"), "--oozie-conf", os.path.join(base, "oozie")]
        cmd += ["--rm", self.servers["ok"], "--watch", "2", "--color", "never"]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(5)
        p.send_signal(signal.SIGINT)  # Ctrl-C, the way to stop --watch
        out = p.communicate(timeout=30)[0].decode("utf-8", "replace")
        self.assertEqual(p.returncode, 130, out[-2000:])
        self.assertGreaterEqual(out.count("YARN queues and apps"), 2)
        self.assertIn("QUEUES NOW", out)
        self.assertNotIn("Traceback", out)

    @unittest.skipIf(reads_anything(), "this user reads files without permission (root)")
    def test_files_that_cannot_be_read(self) -> None:
        cases = {
            "oozie-site.xml": ["oozie/oozie-site.xml"],
            "spark-defaults.conf": ["spark/spark-defaults.conf"],
            "capacity-scheduler.xml": ["hadoop/capacity-scheduler.xml"],
            "yarn-site.xml": ["hadoop/yarn-site.xml"],
            "all": ["oozie", "spark/spark-defaults.conf", "hadoop/capacity-scheduler.xml", "info"],
        }
        for name, paths in cases.items():
            with self.subTest(case=name):
                tmp = tempfile.mkdtemp()
                try:
                    copy = os.path.join(tmp, "denied")
                    shutil.copytree(os.path.join(HERE, "configs", "default"), copy)
                    for path in paths:
                        os.chmod(os.path.join(copy, path), 0)
                    rc, out = run("denied", self.servers["ok"], ["--color", "never"], configs=tmp)
                    self.check(rc, out)
                    self.assertIn("COULD NOT READ", out)
                finally:
                    for root, dirs, _ in os.walk(tmp):
                        for d in dirs:
                            os.chmod(os.path.join(root, d), 0o700)
                    for root, _, files in os.walk(tmp):
                        for f in files:
                            os.chmod(os.path.join(root, f), 0o600)
                    shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()
