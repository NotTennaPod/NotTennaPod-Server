"""Runner integration tests; the subprocess is a synthetic Go event producer."""

import contextlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import check_ops


FAKE_GO = r'''
import json, pathlib, re, signal, sys

root = pathlib.Path.cwd()
config = json.loads((root / ".work/fake-config.json").read_text())
args = sys.argv[1:]
package = "github.com/NotTennaPod/NotTennaPod-Server/internal/ops"
failure = config.get("failure")

def emit(action, test=None, output=None):
    event = {"Action": action, "Package": package}
    if test is not None:
        event["Test"] = test
    if output is not None:
        event["Output"] = output
    print(json.dumps(event), flush=True)

def body(name, skip=None):
    if failure == "omit" and "/" in name:
        return
    emit("run", name)
    if failure == "missing-terminal":
        return
    if failure == "unexpected-skip":
        skip = "unclassified skip"
    if skip:
        emit("output", name, "    fake_test.go:1: " + skip + "\n")
    emit("skip" if skip else "pass", name)
    if failure == "duplicate":
        emit("pass", name)

if args[0] == "list":
    print(package)
    print(package.rsplit("/", 1)[0] + "/storage")
    print(package.rsplit("/", 1)[0] + "/version")
    if "-race" in args:
        print(package.rsplit("/", 1)[0] + "/raceonly")
    sys.exit(0)
if "-list" in args:
    tops = list(config["tops"])
    if failure == "different-build-tags" and "-race" not in args:
        tops.append("TestNonraceOnly")
    for top in tops:
        emit("output", output=top + "\n")
    emit("pass")
    sys.exit(0)
if "./internal/ops" not in args:
    assert package not in args, "ops must be excluded from other-package race invocation"
    base = package.rsplit("/", 1)[0]
    package = base + "/storage"
    body("TestPlatform", "existing platform skip")
    if failure != "missing-package-receipt":
        emit("pass")
    package = base + "/version"
    emit("output", output="?\t" + package + "\t[no test files]\n")
    emit("skip")
    if base + "/raceonly" in args:
        package = base + "/raceonly"
        body("TestRaceOnly")
        emit("pass")
    sys.exit(0)
if failure == "deadline":
    signal.pause()
if failure == "invalid-json":
    print("not JSON")
    sys.exit(0)
pattern = args[args.index("-run") + 1].split("/")
if failure != "no-tests":
    for top in config["tops"]:
        if not re.fullmatch(pattern[0], top):
            continue
        if top == "TestRuleVectors":
            emit("run", top)
            for case in config["cases"]:
                if len(pattern) == 1 or re.fullmatch(pattern[1], case):
                    body(top + "/" + case)
            if failure == "extra":
                body(top + "/rules_uninventoried_0")
            emit("pass", top)
        else:
            skip = config["race_skips"].get(top) if "-race" in args else None
            if top == "TestQueueReplayMeasure":
                skip = "set NTP_QUEUE_MEASURE to the number of queue commands"
            body(top, skip)
    if failure == "extra" and len(pattern) == 1:
        body("TestUninventoried")
emit("fail" if failure == "failure-receipt" else "pass")
sys.exit(3 if failure == "nonzero" else 0)
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".work").mkdir()
        (self.root / "internal/ops").mkdir(parents=True)
        source = Path(__file__).resolve().parents[1] / "internal/ops/rules_test.go"
        (self.root / "internal/ops/rules_test.go").write_text(source.read_text())
        fake = self.root / ".work/fake-go.py"
        fake.write_text(FAKE_GO)
        self.go = shlex.quote(sys.executable) + " " + shlex.quote(str(fake))
        self.config = {
            "tops": ["TestRuleVectors", "TestPlain", "TestQueueReplayMeasure",
                     "TestUnsubscribeManyEpisodeScopes", "TestHistoryClearAndResetLoad",
                     "TestLongOfflineSessionUpload", "TestRebuildLoad", "TestRotationPerformance",
                     "TestSortedAddWithLongQueueHistory", "TestConcentratedHistoryIsStaged"],
            "cases": [],
            "race_skips": {
                "TestUnsubscribeManyEpisodeScopes": "load test (make check runs it without the race detector)",
                "TestHistoryClearAndResetLoad": "load test (make check runs it without the race detector)",
                "TestLongOfflineSessionUpload": "load test (make check runs it without the race detector)",
                "TestRebuildLoad": "load test (make check runs it without the race detector)",
                "TestRotationPerformance": "load test",
                "TestSortedAddWithLongQueueHistory": "load test (make check runs it without the race detector)",
                "TestConcentratedHistoryIsStaged": "load test",
            },
        }
        self.add_input("contract/vectors/rules_shared.json", 1)
        self.add_input("internal/ops/testdata/server/rules_server.json", 1)

    def add_input(self, relative, count):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"suite": "rules", "cases": [{"arrivals": []} for _ in range(count)]}))
        self.config["cases"].extend(f"{path.stem}_{i}" for i in range(count))

    def runner(self, mode="race", env=None, prefix=None):
        (self.root / ".work/fake-config.json").write_text(json.dumps(self.config))
        return check_ops.Runner(self.root, mode, self.go, env={} if env is None else env,
                                case_prefix=prefix)

    def run_quietly(self, runner):
        with contextlib.redirect_stdout(io.StringIO()):
            runner.run()
        return json.loads((runner.output / "coverage.json").read_text())

    def test_new_compiled_methods_and_all_input_roots_execute_once(self):
        self.config["tops"].extend(["TestFreshMethod", "ExampleFresh", "FuzzFresh"])
        self.add_input("contract/vectors/rules_literal.[+](a)$.json", 53)
        self.add_input(".work/generated/arbitrary.json", 2)
        runner = self.runner(env={"NTP_RULE_CASES": "../../.work/generated"})
        manifest = self.run_quietly(runner)
        self.assertEqual(manifest["status"], "pass")
        self.assertEqual(set(manifest["unique_rule_body_receipts"]), set(self.config["cases"]))
        self.assertEqual(set(manifest["unique_non_rule_top_receipts"]), set(self.config["tops"]) - {"TestRuleVectors"})
        self.assertGreater(manifest["rule_parent_setup_receipts"], 1)
        skips = [s for r in manifest["invocations"] for s in r.get("coverage", {}).get("skips", [])]
        self.assertEqual(len(skips), 8)
        for invocation in manifest["invocations"]:
            command = invocation["command"]
            self.assertIn("-count=1", command)
            self.assertEqual(command[command.index("-timeout") + 1], "30m")
            if invocation["label"].startswith(("top-", "rules-")):
                self.assertIn("-race", command)
                self.assertTrue((runner.output / invocation["stdout"]).is_file())

    def test_receipt_and_invocation_failures_fail_command_and_retain_manifest(self):
        # Independent synthetic event corruption verifies the checks, not a second
        # implementation of the reconciler. Omissions happen after real selection.
        for failure in ("omit", "duplicate", "extra", "missing-terminal", "unexpected-skip",
                        "no-tests", "nonzero", "failure-receipt", "invalid-json"):
            with self.subTest(failure=failure):
                self.config["failure"] = failure
                runner = self.runner("rules")
                with self.assertRaises(check_ops.CheckFailure), contextlib.redirect_stdout(io.StringIO()):
                    runner.run()
                manifest = json.loads((runner.output / "coverage.json").read_text())
                self.assertEqual(manifest["status"], "fail")
                self.assertTrue(manifest["error"])
                for invocation in manifest["invocations"]:
                    self.assertTrue((runner.output / invocation["stdout"]).is_file())
                    self.assertTrue((runner.output / invocation["stderr"]).is_file())

    def test_invocation_deadline_fails_and_stops_process(self):
        self.config["failure"] = "deadline"
        runner = self.runner("rules")
        # Only the synthetic blocked invocation uses this short deadline.
        with patch.object(check_ops, "DEADLINE_SECONDS", 2):
            with self.assertRaisesRegex(check_ops.CheckFailure, "deadline exceeded"), contextlib.redirect_stdout(io.StringIO()):
                runner.run()
        manifest = json.loads((runner.output / "coverage.json").read_text())
        self.assertTrue(manifest["invocations"][-1]["timed_out"])
        self.assertIsNotNone(manifest["invocations"][-1]["exit_code"])

    def test_loads_are_all_default_nonrace_and_reject_overrides(self):
        manifest = self.run_quietly(self.runner("loads"))
        loads = [r for r in manifest["invocations"] if r["label"].startswith("load-")]
        self.assertEqual(len(loads), 7)
        self.assertEqual({r["expected_tops"][0] for r in loads}, set(self.config["race_skips"]))
        self.assertTrue(all("-race" not in r["command"] and "-short" not in r["command"] for r in loads))
        self.assertTrue(all(r["command"][r["command"].index("-timeout") + 1] == "10m" for r in loads))
        for variable in ("NTP_LISTEN_LOAD", "NTP_REBUILD_LOAD", "NTP_REBUILD_EPISODES", "NTP_REBUILD_STAGING", "NTP_IDENTITY_PERF"):
            with self.subTest(variable=variable):
                with self.assertRaisesRegex(check_ops.CheckFailure, "reject environment overrides"):
                    self.run_quietly(self.runner("loads", {variable: "1"}))

    def test_recovery_prefix_records_only_selected_cases(self):
        self.add_input(".work/recovery/rules_random_0000.json", 2)
        runner = self.runner("rules", {"NTP_RULE_CASES": str(self.root / ".work/recovery")}, "rules_random_")
        manifest = self.run_quietly(runner)
        self.assertEqual(manifest["unique_rule_body_receipts"], ["rules_random_0000_0", "rules_random_0000_1"])
        self.assertEqual(manifest["excluded_by_case_prefix"], ["rules_server_0", "rules_shared_0"])
        with self.assertRaisesRegex(check_ops.CheckFailure, "selection contains no cases"):
            self.run_quietly(self.runner("rules", prefix="rules_absent_"))

    def test_duplicate_case_names_or_changed_discovery_fail_loudly(self):
        self.add_input("internal/ops/testdata/collision/rules_shared.json", 1)
        with self.assertRaisesRegex(check_ops.CheckFailure, "duplicate rule case identifier"):
            self.run_quietly(self.runner("rules"))
        (self.root / "internal/ops/testdata/collision/rules_shared.json").unlink()
        source = self.root / "internal/ops/rules_test.go"
        source.write_text(source.read_text().replace('"testdata/*/rules_*.json"', '"testdata/newroot/*.json"'))
        with self.assertRaisesRegex(check_ops.CheckFailure, "input discovery changed"):
            self.run_quietly(self.runner("rules"))

    def test_opt_in_measurement_and_build_tag_inventory_need_explicit_classification(self):
        with self.assertRaisesRegex(check_ops.CheckFailure, "opt-in"):
            self.run_quietly(self.runner(env={"NTP_QUEUE_MEASURE": "10"}))
        self.config["failure"] = "different-build-tags"
        with self.assertRaisesRegex(check_ops.CheckFailure, "inventories differ"):
            self.run_quietly(self.runner())

    def test_other_packages_exclude_ops_and_preserve_platform_skips(self):
        manifest = self.run_quietly(self.runner("other-race"))
        self.assertNotIn(check_ops.OPS, manifest["packages"])
        self.assertIn(check_ops.OPS.rsplit("/", 1)[0] + "/raceonly", manifest["packages"])
        self.assertEqual(manifest["status"], "pass")
        command = manifest["invocations"][-1]["command"]
        self.assertEqual(command[command.index("-run") + 1], ".")
        self.config["failure"] = "missing-package-receipt"
        with self.assertRaisesRegex(check_ops.CheckFailure, "terminal package coverage mismatch"):
            self.run_quietly(self.runner("other-race"))

    def test_cli_returns_nonzero_for_missing_case_receipt(self):
        self.config["failure"] = "omit"
        self.runner("rules")  # Write the synthetic Go configuration.
        scripts = self.root / "scripts"
        scripts.mkdir()
        entry = scripts / "check_ops.py"
        entry.write_text(Path(check_ops.__file__).read_text())
        result = subprocess.run([sys.executable, str(entry), "rules", "--go", self.go],
                                cwd=self.root, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 1)
        self.assertIn("rule coverage mismatch", result.stderr)
        manifests = list((self.root / ".work/check-ops").glob("*/coverage.json"))
        self.assertEqual(len(manifests), 1)
        self.assertEqual(json.loads(manifests[0].read_text())["status"], "fail")


if __name__ == "__main__":
    unittest.main()
