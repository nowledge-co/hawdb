# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""A refused baseline must preserve other outcomes without qualifying a speedup."""

import array
import contextlib
import io
import json
import sys
import types
import unittest
from unittest import mock

import retained_boundary as driver


EMPTY = {"retained_bytes": 0, "view_handles": 0, "buffer_owners": 0}


def result(rows=3):
    return {**driver.fixture_reference(rows), "query_boundary_ns": 10,
            "elapsed_ns": 20, "retained_snapshot": EMPTY.copy()}


class Buffer(array.array):
    def close(self):
        # Resizing refuses while any memoryview is still exported.
        self.append(0)
        self.pop()
        self.closed = True


class Batch:
    def __init__(self, fail_column=None):
        self.columns = [Buffer(fmt, range(3)) for fmt in ("q", "Q", "q")]
        self.indices = Buffer("I", range(3))
        self.closed = False
        self.fail_column = fail_column

    def column(self, index):
        if index == self.fail_column:
            raise RuntimeError("column failed")
        return self.columns[index]

    def selection(self):
        return self.indices

    def close(self):
        self.closed = True


class RetainedDriverTests(unittest.TestCase):
    def db(self):
        return mock.Mock(retained_snapshot_copy=mock.Mock(return_value=EMPTY.copy()))

    def test_success_preserves_reference_warmups_and_rotating_samples(self):
        with mock.patch.object(driver, "consume", return_value=result()) as consume:
            report = driver.benchmark(self.db(), 3, ("owned", "retained", "arrow"), 3, {})
        self.assertEqual(report["status"], "ok")
        self.assertTrue(report["all_paths_succeeded"])
        self.assertEqual(report["fixture_reference"]["checksum"], "876e812967824bc2")
        self.assertEqual([call.args[1] for call in consume.call_args_list], [
            "owned", "owned", "retained", "arrow",
            "owned", "retained", "arrow", "retained", "arrow", "owned",
            "arrow", "owned", "retained",
        ])
        self.assertEqual(len(report["records"]), 9)
        self.assertTrue(all(s["qualified"] for s in report["summary"].values()))

    def test_owned_refusal_does_not_suppress_retained_or_arrow(self):
        def consume(db, mode):
            if mode == "owned":
                raise RuntimeError("default result cap")
            return result()

        with mock.patch.object(driver, "consume", side_effect=consume) as calls:
            report = driver.benchmark(self.db(), 3, ("owned", "retained", "arrow"), 3, {})
        self.assertEqual(calls.call_count, 13)
        self.assertEqual(report["status"], "error")
        self.assertFalse(report["all_paths_succeeded"])
        self.assertEqual(report["reference"]["error"], "default result cap")
        self.assertEqual(len(report["records"]), 9)
        for mode, count in (("owned", 0), ("retained", 3), ("arrow", 3)):
            summary = report["summary"][mode]
            self.assertEqual(summary["successful_samples"], count)
            self.assertFalse(summary["qualified"])
            self.assertNotIn("median_elapsed_ns", summary)
            self.assertNotIn("median_query_boundary_ns", summary)

    def test_shared_wrong_results_cannot_validate_each_other(self):
        for change in ({"rows": 2}, {"checksum": "0000000000000000"}):
            with self.subTest(change=change), \
                    mock.patch.object(driver, "consume", return_value={**result(), **change}):
                report = driver.benchmark(self.db(), 3, ("owned", "retained", "arrow"), 1, {})
            self.assertEqual(report["status"], "error")
            self.assertFalse(report["reference"]["parity"])
            self.assertTrue(all(not s["parity"] for s in report["records"]))

    def test_bad_warmup_or_middle_sample_is_not_hidden_by_successes(self):
        for failing_call in (2, 4):
            calls = 0

            def consume(db, mode):
                nonlocal calls
                calls += 1
                return {**result(), "rows": 2} if calls == failing_call else result()

            with self.subTest(failing_call=failing_call), \
                    mock.patch.object(driver, "consume", side_effect=consume):
                report = driver.benchmark(self.db(), 3, ("owned",), 3, {})
            self.assertEqual(calls, 5)
            self.assertEqual(len(report["records"]), 3)
            self.assertFalse(report["summary"]["owned"]["qualified"])
            self.assertNotIn("median_elapsed_ns", report["summary"]["owned"])
            self.assertEqual(report["status"], "error")

    def test_owned_consumer_checks_types_and_order_against_fixture(self):
        rows = [{"score": i, "identity": i, "again": i} for i in range(3)]
        db = self.db()
        db.execute.return_value = rows
        self.assertEqual(driver.attempt(db, "owned", driver.fixture_reference(3), {})["status"], "ok")
        db.execute.return_value = rows[::-1]
        self.assertFalse(driver.attempt(db, "owned", driver.fixture_reference(3), {})["parity"])
        db.execute.return_value = [{"score": False, "identity": 0, "again": 0}]
        self.assertEqual(driver.attempt(db, "owned", driver.fixture_reference(1), {})["error_type"], "TypeError")

    def test_retained_consumer_releases_after_late_checksum_failure(self):
        db = self.db()
        batch = Batch()
        cursor = db.execute_retained.return_value
        cursor.next_batch.return_value = batch
        with mock.patch.object(driver, "fold", side_effect=RuntimeError("consumer failed")):
            with self.assertRaisesRegex(RuntimeError, "consumer failed"):
                driver.consume(db, "retained")
        self.assertTrue(all(column.closed for column in batch.columns))
        self.assertTrue(batch.indices.closed)
        self.assertTrue(batch.closed)
        cursor.close.assert_called_once()

    def test_retained_success_consumes_all_batches_and_releases(self):
        db = self.db()
        batch = Batch()
        cursor = db.execute_retained.return_value
        cursor.next_batch.side_effect = [batch, None]
        observed = driver.consume(db, "retained")
        self.assertEqual((observed["rows"], observed["checksum"]), (3, "876e812967824bc2"))
        self.assertTrue(all(column.closed for column in batch.columns))
        self.assertTrue(batch.indices.closed)
        self.assertTrue(batch.closed)
        cursor.close.assert_called_once()

    def test_partial_descriptor_failure_releases_prior_owners(self):
        db = self.db()
        batch = Batch(fail_column=1)
        cursor = db.execute_retained.return_value
        cursor.next_batch.return_value = batch
        with self.assertRaisesRegex(RuntimeError, "column failed"):
            driver.consume(db, "retained")
        self.assertTrue(batch.columns[0].closed)
        self.assertTrue(batch.closed)
        cursor.close.assert_called_once()

    def test_arrow_import_failure_closes_cursor(self):
        db = self.db()
        from_stream = mock.Mock(side_effect=RuntimeError("stream failed"))
        arrow = types.SimpleNamespace(RecordBatchReader=types.SimpleNamespace(from_stream=from_stream))
        with mock.patch.dict(sys.modules, {"pyarrow": arrow}):
            with self.assertRaisesRegex(RuntimeError, "stream failed"):
                driver.consume(db, "arrow")
        db.execute_retained.return_value.close.assert_called_once()

    def test_arrow_reader_close_failure_still_closes_cursor(self):
        db = self.db()
        reader = mock.MagicMock()
        reader.__next__.side_effect = StopIteration
        reader.close.side_effect = RuntimeError("close failed")
        arrow = types.SimpleNamespace(RecordBatchReader=types.SimpleNamespace(from_stream=lambda _: reader))
        with mock.patch.dict(sys.modules, {"pyarrow": arrow}):
            with self.assertRaisesRegex(RuntimeError, "close failed"):
                driver.consume(db, "arrow")
        db.execute_retained.return_value.close.assert_called_once()

    def test_arrow_batch_views_release_on_success_and_consumer_failure(self):
        for fail in (False, True):
            db = self.db()
            buffers = [Buffer("B", array.array(fmt, range(3)).tobytes()) for fmt in ("q", "Q", "q")]
            columns = [types.SimpleNamespace(offset=0, buffers=lambda b=buffer: (None, b))
                       for buffer in buffers]
            batch = types.SimpleNamespace(column=lambda i: columns[i], num_rows=3)
            reader = mock.MagicMock()
            reader.__next__.side_effect = [batch, StopIteration]
            reader.close.side_effect = lambda: [buffer.close() for buffer in buffers]
            arrow = types.SimpleNamespace(RecordBatchReader=types.SimpleNamespace(from_stream=lambda _: reader))
            with self.subTest(fail=fail), mock.patch.dict(sys.modules, {"pyarrow": arrow}):
                if fail:
                    with mock.patch.object(driver, "fold", side_effect=RuntimeError("consumer failed")):
                        with self.assertRaisesRegex(RuntimeError, "consumer failed"):
                            driver.consume(db, "arrow")
                else:
                    observed = driver.consume(db, "arrow")
                    self.assertEqual((observed["rows"], observed["checksum"]), (3, "876e812967824bc2"))
            self.assertTrue(all(buffer.closed for buffer in buffers))
            reader.close.assert_called_once()
            db.execute_retained.return_value.close.assert_called_once()

    def test_missing_arrow_preserves_other_cli_results_and_nonzero_exit(self):
        db = self.db()
        hawdb = types.SimpleNamespace(open=lambda: db)
        output = io.StringIO()
        with mock.patch.dict(sys.modules, {"hawdb": hawdb, "pyarrow": None}), \
                mock.patch.object(sys, "argv", ["retained_boundary.py", "--rows", "3", "--backend", "memory", "--mode", "all", "--samples", "2"]), \
                mock.patch.object(driver, "consume", return_value=result()) as consume, \
                contextlib.redirect_stdout(output):
            status = driver.main()
        report = json.loads(output.getvalue())
        self.assertEqual(status, 1)
        self.assertEqual(report["status"], "error")
        self.assertEqual(len(report["records"]), 6)
        self.assertTrue(all(call.args[1] != "arrow" for call in consume.call_args_list))
        self.assertEqual(report["summary"]["retained"]["successful_samples"], 2)
        self.assertFalse(report["summary"]["arrow"]["qualified"])
        self.assertEqual(report["warmups"]["arrow"]["phase"], "prerequisite")
        db.close.assert_called_once()

    def test_leaked_owners_are_not_successful_samples(self):
        db = self.db()
        db.execute.return_value = [{"score": 0, "identity": 0, "again": 0}]
        db.retained_snapshot_copy.return_value = {**EMPTY, "buffer_owners": 1}
        report = driver.benchmark(db, 1, ("owned",), 2, {})
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["summary"]["owned"]["successful_samples"], 0)
        self.assertEqual(report["reference"]["retained_snapshot"]["buffer_owners"], 1)

    def test_cli_close_failure_cannot_publish_qualified_medians(self):
        for close_fails in (False, True):
            db = self.db()
            if close_fails:
                db.close.side_effect = RuntimeError("database close failed")
            output = io.StringIO()
            with self.subTest(close_fails=close_fails), \
                    mock.patch.dict(sys.modules, {"hawdb": types.SimpleNamespace(open=lambda: db)}), \
                    mock.patch.object(sys, "argv", ["retained_boundary.py", "--rows", "3", "--backend", "memory", "--mode", "owned", "--samples", "2"]), \
                    mock.patch.object(driver, "consume", return_value=result()), \
                    contextlib.redirect_stdout(output):
                status = driver.main()
            report = json.loads(output.getvalue())
            self.assertEqual(status, int(close_fails))
            self.assertEqual(report["all_paths_succeeded"], not close_fails)
            self.assertEqual(report["summary"]["owned"]["qualified"], not close_fails)
            self.assertEqual("median_elapsed_ns" in report["summary"]["owned"], not close_fails)
            self.assertEqual(len(report["records"]), 2)
            db.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
