"""Defaults, and the measurements behind them.

The numbers here are not guesses. They come from a benchmark of the ERPL
extensions against an SAP ABAP Platform Trial, recorded in the erpl-airbyte
repository's docs/performance.md.
"""

from __future__ import annotations

#: The ERPL release this package is tested against.
ERPL_VERSION = "v2026.09.04"

#: Rows per Arrow ``RecordBatch`` handed to dlt.
DEFAULT_BATCH_SIZE = 50_000

#: DuckDB threads, which for an RFC extract means "SAP round trips in flight".
#:
#: An RFC read is latency-bound, not CPU-bound: every thread here is blocked on
#: SAP. Measured on a 55-column, 164,673-row table, throughput rises almost
#: linearly with this number -- 1,479 rows/s at 1, 8,740 at 8, 11,720 at 16,
#: 12,919 at 32. The last doubling buys 10% for twice the SAP-side load, and
#: ERPL itself caches at most 16 RFC connections, so 16 is where this stops.
DEFAULT_DUCKDB_THREADS = 16

#: Extensions to load. ``erpl`` itself is a trampoline: its payload unpacks into
#: the three below, and ``erpl_web`` is a separate extension.
RFC_EXTENSIONS = ("erpl_rfc",)
BICS_EXTENSIONS = ("erpl_rfc", "erpl_bics")
ODP_RFC_EXTENSIONS = ("erpl_rfc", "erpl_odp")
WEB_EXTENSIONS = ("erpl_web",)

#: Partitioning is deliberately not exposed as a tuning knob.
#:
#: ``PARTITIONS := n`` replaces ERPL's column-parallel read -- one concurrent RFC
#: call per projected column -- with a row-window scheduler that opened ~3 RFC
#: connections where the serial path opened 8, and measured ~5.5x *slower* on a
#: wide table and 1.5x slower on a narrow one. Pass it through ``extra_params``
#: if you have a system that behaves differently, with a measurement in hand.
