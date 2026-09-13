"""The combination that silently truncates, and the names that silently merge.

`replace` with an incremental cursor is measurable in dlt alone: run one loads
ten rows, run two extracts six, and the table ends with six. Nothing warns. It
stays invisible while the source is unchanged, because dlt deduplicates the
re-read boundary to zero rows and a zero-row replace is a no-op -- so the first
time it bites is the first time there is real data to load.
"""

import shutil

import dlt
import pytest

from erpl_dlt.query import ConfigurationError, check_unique_names, resolve_write_disposition


class TestDispositionResolution:
    def test_a_primary_key_means_merge(self):
        assert resolve_write_disposition(primary_key=["A"], cursor_column="F", explicit=None, resource="T") == "merge"

    def test_no_cursor_and_no_key_means_replace(self):
        assert resolve_write_disposition(primary_key=None, cursor_column=None, explicit=None, resource="T") == "replace"

    def test_a_cursor_without_a_key_is_refused(self):
        with pytest.raises(ConfigurationError) as caught:
            resolve_write_disposition(primary_key=None, cursor_column="FLDATE", explicit=None, resource="SFLIGHT")
        assert "SFLIGHT" in str(caught.value)
        assert "primary key" in str(caught.value)

    def test_an_explicit_choice_is_honoured(self):
        # Someone who wants append-with-duplicates may have it, knowingly.
        assert (
            resolve_write_disposition(primary_key=None, cursor_column="F", explicit="append", resource="T") == "append"
        )


class TestTheTruncationIsReal:
    """Not a claim about dlt: a measurement of it."""

    @staticmethod
    def _run(tmp_path, disposition, rows, primary_key=None):
        def gen(incremental=dlt.sources.incremental("ts")):  # noqa: B008
            for row in rows:
                if incremental.last_value is None or row["ts"] >= incremental.last_value:
                    yield row

        resource = dlt.resource(gen, name="t", write_disposition=disposition)()
        if primary_key:
            resource.apply_hints(primary_key=primary_key)
        # The destination file goes inside tmp_path too: dlt's duckdb
        # destination otherwise writes <pipeline>.duckdb into the working
        # directory, and a leftover from an earlier run makes this test pass or
        # fail on history rather than on behaviour.
        pipeline = dlt.pipeline(
            pipeline_name=f"trunc_{disposition}",
            destination=dlt.destinations.duckdb(str(tmp_path / "w.duckdb")),
            dataset_name="s",
            pipelines_dir=str(tmp_path),
        )
        pipeline.run(resource)
        with pipeline.sql_client() as client:
            return client.execute_sql("SELECT count(*) FROM t")[0][0]

    FIRST = [{"id": i, "ts": i} for i in range(1, 11)]
    SECOND = [{"id": i, "ts": i} for i in range(8, 14)]

    def test_replace_loses_rows(self, tmp_path):
        shutil.rmtree(tmp_path, ignore_errors=True)
        first = self._run(tmp_path, "replace", self.FIRST)
        second = self._run(tmp_path, "replace", self.SECOND)
        assert first == 10
        assert second < first, "if dlt ever stops truncating here, the guard can be relaxed"

    def test_append_does_not(self, tmp_path):
        shutil.rmtree(tmp_path, ignore_errors=True)
        first = self._run(tmp_path, "append", self.FIRST)
        second = self._run(tmp_path, "append", self.SECOND)
        assert second > first

    def test_merge_does_not(self, tmp_path):
        shutil.rmtree(tmp_path, ignore_errors=True)
        first = self._run(tmp_path, "merge", self.FIRST, primary_key="id")
        second = self._run(tmp_path, "merge", self.SECOND, primary_key="id")
        assert second > first


class TestNameCollisions:
    def test_two_resources_with_one_name_are_refused(self):
        with pytest.raises(ConfigurationError) as caught:
            check_unique_names(["salesorders", "salesorders"], what="entity set")
        assert "salesorders" in str(caught.value)

    def test_distinct_names_pass(self):
        check_unique_names(["a", "b", "c"])

    def test_collisions_after_normalisation_are_caught(self):
        # ODP folds $ and / to _, so two different providers can land on one name.
        folded = [n.lower().replace("$", "_").replace("/", "_") for n in ("A$B", "A/B")]
        with pytest.raises(ConfigurationError):
            check_unique_names(folded, what="provider")


class TestNativePreload:
    """A total preload failure must raise, not wait to core-dump."""

    def test_nothing_loading_is_fatal(self, tmp_path, monkeypatch):
        import ctypes

        from erpl_dlt import connection

        plat = tmp_path / "v1.5.5" / "linux_amd64"
        plat.mkdir(parents=True)
        for name in ("libsapnwrfc.so", "libicuuc.so.50"):
            (plat / name).write_bytes(b"")
        monkeypatch.setattr(ctypes, "CDLL", lambda *a, **k: (_ for _ in ()).throw(OSError("invalid ELF header")))

        with pytest.raises(RuntimeError) as caught:
            connection.preload_native_libraries(str(tmp_path))
        assert "abort the process" in str(caught.value)

    def test_a_partial_failure_is_tolerated(self, tmp_path, monkeypatch):
        # Measured against a real system: ICU i18n reports "could not open" and
        # the RFC read succeeds regardless. Failing here would break that.
        import ctypes

        from erpl_dlt import connection

        plat = tmp_path / "v1.5.5" / "linux_amd64"
        plat.mkdir(parents=True)
        for name in ("libsapnwrfc.so", "libicui18n.so.50"):
            (plat / name).write_bytes(b"")

        def loader(path, *args, **kwargs):
            if "icui18n" in str(path):
                raise OSError("cannot open shared object file")
            return object()

        monkeypatch.setattr(ctypes, "CDLL", loader)
        connection.preload_native_libraries(str(tmp_path))

    def test_an_empty_directory_is_not_an_error(self, tmp_path):
        from erpl_dlt import connection

        connection.preload_native_libraries(str(tmp_path))
