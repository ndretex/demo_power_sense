import hashlib
import json
import math
import time
from datetime import timezone
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

from .config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER, HISTORY_DATA_URL
from .transform import normalize_record, remap_metric_name


def _is_nullish(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    return False


def _format_ukey(ts, perimetre: str, nature, metric: str) -> str:
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.astimezone(timezone.utc)
    date_str = ts.strftime("%Y%m%d")
    time_str = ts.strftime("%H:%M:%S")
    payload = {
        "perimetre": perimetre,
        "nature": nature,
        "metric": metric,
        "date": date_str,
        "time": time_str,
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def get_client():
    return clickhouse_connect.get_client(
        host=DB_HOST,
        port=DB_PORT,
        username=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )


def check_db_emptiness() -> bool:
    """
    Check if the measurements table is empty.
    Returns True if empty, False otherwise.
    """
    client = get_client()
    result = client.query("SELECT count() FROM measurements")
    count = result.result_rows[0][0] if result.result_rows else 0
    return count == 0


def bootstrap_history() -> bool:
    """
    Download initial history and insert into DB if not already present.
    """
    import io
    import urllib.request
    import zipfile

    import pandas as pd

    # check if DB is empty
    if not check_db_emptiness():
        return False  # already has data

    # download temporary zip file, extract XLS, parse rows, insert into DB
    with urllib.request.urlopen(HISTORY_DATA_URL) as resp:
        zip_bytes = resp.read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # find the first xls/xlsx file
        xls_filename = None
        for name in z.namelist():
            if name.lower().endswith((".xls", ".xlsx")):
                xls_filename = name
                break
        if not xls_filename:
            raise ValueError("No XLS/XLSX file found in history zip")

        # read file bytes and pass to pandas
        content_bytes = z.read(xls_filename)
        # choose engine based on extension (.xls uses xlrd, .xlsx uses openpyxl)
        ext = xls_filename.lower().rsplit(".", 1)[-1]
        if ext == "xls":
            # pandas requires the 'xlrd' package to read .xls files.
            # Ensure 'xlrd' is installed in the environment (requirements.txt may need updating).
            engine = "xlrd"
        else:
            engine = "openpyxl"
        try:
            df = pd.read_excel(
                io.BytesIO(content_bytes), engine=engine, index_col=False, header=0
            )
        except Exception:
            # Some providers label a text/TSV file with .xls extension.
            # Fall back to parsing as tab-separated text using latin-1 encoding.
            text = content_bytes.decode("latin-1")
            df = pd.read_csv(
                io.StringIO(text), sep="\t", index_col=False, header=0, low_memory=False
            )
            print(f"Bootstrap history - number of rows fetched: {len(df)}")

        # drop rows where "Nature" is null
        df = df[df["Nature"].notnull()]

        # normalize/rename columns using remap_metric_name
        new_columns = [remap_metric_name(col) for col in df.columns]
        df.columns = new_columns

        # remove rows where
        # create date_heure column from date and heure columns if present
        if "date" in df.columns and "heure" in df.columns:
            df["date_heure"] = df.apply(
                lambda row: f"{row['date']}T{row['heure']}:00+00:00", axis=1
            )

    # for testing purposes, filter dataframe to include rows for date_heure <= today()-1day
    from datetime import datetime, timedelta

    today = datetime.now().date()
    df = df[pd.to_datetime(df["date_heure"]).dt.date <= (today - timedelta(days=1))]

    # iterate rows and normalize/insert
    print("Bootstrap history - normalizing rows...")
    all_rows: List[Tuple] = []
    for _, rec in df.iterrows():
        # drop null values to match previous behavior
        clean = {k: v for k, v in rec.items() if v is not None}
        rows = normalize_record(clean)
        all_rows.extend(rows)

    print(f"Bootstrap history - total normalized rows to insert: {len(all_rows)}")

    print("Bootstrap history - inserting rows into DB...")
    n = insert_measurements(all_rows, is_bootstrap=True)
    print(f"Bootstrap history - inserted {n} rows into DB.")
    return True


def _compute_version(value, ukey_json: str) -> int:
    payload = json.dumps(
        {"value": value, "ukey": ukey_json},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


def insert_measurements(
    rows: List[Tuple],
    is_bootstrap: bool = False,
    retry_seconds: int = 1,
    max_retries: int = 5,
) -> int:
    """
    Batch insert rows into measurements.

    rows: list of (ts, source, metric, value, perimetre, nature)
    Returns number of rows inserted/upserted.
    """
    _ = is_bootstrap

    if not rows:
        return 0

    attempts = 0
    while True:
        try:
            client = get_client()

            def prepare(r):
                ts, source, metric, value, perimetre, nature = r
                clean_value = None if _is_nullish(value) else value
                ukey_json = _format_ukey(ts, perimetre, nature, metric)
                version = _compute_version(clean_value, ukey_json)
                return (
                    ts,
                    source,
                    metric,
                    clean_value,
                    ukey_json,
                    version,
                )

            vals = [prepare(r) for r in rows]
            client.insert(
                "measurements",
                vals,
                column_names=["ts", "source", "metric", "value", "ukey", "version"],
            )
            return len(rows)
        except ClickHouseError:
            attempts += 1
            if attempts >= max_retries:
                raise
            time.sleep(retry_seconds * attempts)
