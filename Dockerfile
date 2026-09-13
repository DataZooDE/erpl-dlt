FROM python:3.12-slim

WORKDIR /app

COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl

# Container smoke test: verify package, extensions, and DuckDB loading
RUN python -c "import erpl_dlt, duckdb, dlt; from erpl_extensions import is_populated; assert is_populated(); from erpl_dlt.connection import ErplConnection; con1 = ErplConnection(extensions=('erpl_rfc',)); con2 = ErplConnection(extensions=('erpl_web',)); print('erpl-dlt container build smoke test passed')"

ENTRYPOINT ["python"]
CMD ["-c", "import erpl_dlt; print(f'erpl-dlt {erpl_dlt.__version__} ready')"]
