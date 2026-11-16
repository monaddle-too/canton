#!/usr/bin/env python3
import argparse
import glob
import json
import os
import sys
import tempfile
import xml.etree.ElementTree as ET


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit-glob", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--zero-weight-suites-without-tests",
        action="store_true",
        help="Record suites whose report has no test cases with weight 0 instead of leaving them out "
        "(they are then weighted at split_tests.py's fallback).",
    )
    return parser.parse_args(argv)


def testcase_key(tc):
    # Use only classname so keys match the class-level entries in split_tests.py
    classname = (tc.attrib.get("classname") or "").strip()
    return classname


def parse_junit_file(path, timings, zero_weight_suites_without_tests=False):
    try:
        root = ET.parse(path).getroot()
    except (
        ET.ParseError
    ):  # silent-ok: an unreadable report adds no timings, merge_timings fails if none are usable
        return

    for tc in root.iter("testcase"):
        key = testcase_key(tc)
        if not key:
            continue
        try:
            duration = float(tc.attrib.get("time") or 0.0)
        except ValueError:  # silent-ok: a malformed time attribute only skews one test's weight
            duration = 0.0
        # sbt's JUnit listener writes -1ms (-0.001s) for test events whose
        # duration the framework did not report. Left as-is those sentinels
        # accumulate into a small negative per-class total, so clamp them to
        # zero instead of summing the noise.
        duration = max(duration, 0.0)
        timings[key] = timings.get(key, 0.0) + duration

    if not zero_weight_suites_without_tests:
        return
    # A suite whose report has no test cases (all its tests excluded by tag in this lane, or a
    # class with no tests at all) still runs in the shard but costs nothing. Without an entry it
    # is weighted at split_tests.py's fallback (300 s in the lanes) on every run; record it as 0.
    # setdefault keeps the sum when another report for the class has test cases.
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    for suite in suites:
        name = (suite.attrib.get("name") or "").strip()
        if name and suite.find("testcase") is None:
            timings.setdefault(name, 0.0)


def collect_junit_paths(junit_glob):
    return sorted(glob.glob(junit_glob, recursive=True))


def build_timings(paths, zero_weight_suites_without_tests=False):
    timings = {}
    for path in paths:
        if os.path.isfile(path):
            parse_junit_file(path, timings, zero_weight_suites_without_tests)
    return timings


def ensure_output_dir(output_path):
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


def write_timings(output_path, timings):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(timings, f, indent=2, sort_keys=True)


def test_testcase_key_uses_classname():
    tc = ET.Element("testcase", attrib={"classname": "  suite.Foo  ", "name": "x"})
    assert testcase_key(tc) == "suite.Foo"


def test_parse_junit_file_aggregates_durations():
    xml = """<testsuite>
      <testcase classname="suite.A" name="a1" time="1.5"/>
      <testcase classname="suite.A" name="a2" time="2.0"/>
      <testcase classname="suite.B" name="b" time="3"/>
    </testsuite>"""
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write(xml)
        path = f.name
    try:
        timings = {}
        parse_junit_file(path, timings)
        assert timings["suite.A"] == 3.5
        assert timings["suite.B"] == 3.0
    finally:
        os.remove(path)


def test_parse_junit_file_drops_negative_durations():
    xml = """<testsuite>
      <testcase classname="suite.A" name="a1" time="1.5"/>
      <testcase classname="suite.A" name="a2" time="-0.001"/>
      <testcase classname="suite.A" name="a3" time="-0.001"/>
    </testsuite>"""
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write(xml)
        path = f.name
    try:
        timings = {}
        parse_junit_file(path, timings)
        assert timings["suite.A"] == 1.5
    finally:
        os.remove(path)


def test_parse_junit_file_ignores_invalid_xml():
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write("<testsuite><testcase")
        path = f.name
    try:
        timings = {"suite.A": 1.0}
        parse_junit_file(path, timings)
        assert timings == {"suite.A": 1.0}
    finally:
        os.remove(path)


def test_parse_junit_file_records_suites_without_test_cases_as_zero():
    reports = {
        "empty.xml": '<testsuite name="suite.Kms" tests="0" time="0.0"></testsuite>',
        "wrapped.xml": '<testsuites><testsuite name="suite.Base" tests="0"/></testsuites>',
        "with-tests.xml": '<testsuite name="suite.A" tests="1"><testcase classname="suite.A" name="a" time="2.0"/></testsuite>',
        "a-empty.xml": '<testsuite name="suite.A" tests="0"></testsuite>',
    }
    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        for filename in ("with-tests.xml", "a-empty.xml", "empty.xml", "wrapped.xml"):
            path = os.path.join(tmp, filename)
            with open(path, "w", encoding="utf-8") as f:
                f.write(reports[filename])
            paths.append(path)
        # Off by default: suites without test cases are left out, as before.
        assert build_timings(paths) == {"suite.A": 2.0}
        assert build_timings(paths, zero_weight_suites_without_tests=True) == {
            "suite.A": 2.0,
            "suite.Kms": 0.0,
            "suite.Base": 0.0,
        }


def test_build_timings_skips_missing_paths():
    timings = build_timings(["/tmp/does-not-exist-report.xml"])
    assert timings == {}


def test_write_timings_and_ensure_output_dir():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "nested", "timings.json")
        ensure_output_dir(output_path)
        write_timings(output_path, {"suite.A": 2.0})
        with open(output_path, encoding="utf-8") as f:
            payload = json.load(f)
        assert payload == {"suite.A": 2.0}


def self_test():
    test_testcase_key_uses_classname()
    test_parse_junit_file_aggregates_durations()
    test_parse_junit_file_drops_negative_durations()
    test_parse_junit_file_ignores_invalid_xml()
    test_parse_junit_file_records_suites_without_test_cases_as_zero()
    test_build_timings_skips_missing_paths()
    test_write_timings_and_ensure_output_dir()
    print("All self-checks passed")


def main():
    args = parse_args()
    paths = collect_junit_paths(args.junit_glob)
    timings = build_timings(paths, args.zero_weight_suites_without_tests)
    ensure_output_dir(args.output)
    write_timings(args.output, timings)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
    else:
        main()
