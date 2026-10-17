"""Local operational dashboard for the file-based monitoring v1 outputs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.dashboard_reader import SnapshotUnavailable, load_dashboard_snapshot  # noqa: E402


st.set_page_config(page_title="E-commerce Pipeline Monitoring", layout="wide")
st.title("E-commerce Pipeline Monitoring")
st.caption("Local observability v1 · dữ liệu từ collector 5 phút, không quét raw JSONL")

root = Path(os.getenv("MONITORING_HOST_DIR", str(PROJECT_ROOT / "monitoring")))
with st.sidebar:
    st.write(f"Nguồn aggregate: `{root}`")
    st.button("Tải lại dữ liệu")

try:
    snapshot = load_dashboard_snapshot(root)
except SnapshotUnavailable as error:
    st.error(f"Chưa có snapshot nhất quán: {error}")
    st.stop()

summary = snapshot["summary"]
history = snapshot["history"]
alerts = snapshot["alerts"]
status = summary.get("alerts_status", "UNKNOWN")
if status == "OPEN":
    st.error(f"Có {len(summary.get('open_alerts', []))} alert đang mở")
elif status == "OK":
    st.success("Không có alert đang mở")
else:
    st.warning(f"Trạng thái alert chưa xác định: {status}")

st.caption(
    f"Snapshot tạo lúc {summary.get('generated_at', '—')} · ID {summary['snapshot_id'][:12]} · "
    "thời điểm snapshot không phải heartbeat của DAG khi không có event mới"
)

cards = st.columns(4)
cards[0].metric("Event hợp lệ", summary.get("accepted_events", 0))
cards[1].metric("Event cách ly", summary.get("rejected_events", 0))
cards[2].metric("Cửa sổ 5 phút", summary.get("window_count", 0))
cards[3].metric("Alert đang mở", len(summary.get("open_alerts", [])))

latest = {(row.get("component"), row.get("metric_name")): row
          for row in summary.get("latest", [])}
producer = latest.get(("producer", "delivery_counts"), {}).get("values", {})
bronze = latest.get(("kafka_to_bronze_dlq", "quality_counts"), {}).get("values", {})
silver = latest.get(("bronze_to_silver_incremental", "run_counts"), {}).get("values", {})

st.subheader("Các metric mới nhất")
columns = st.columns(3)
with columns[0]:
    st.write("**Producer**")
    st.metric("Acknowledged / attempted", f"{producer.get('acknowledged', '—')} / {producer.get('attempted', '—')}")
    st.caption(f"Failed: {producer.get('failed', '—')}")
with columns[1]:
    st.write("**Kafka → Bronze / DLQ**")
    st.metric("Input batch", bronze.get("batch_input_rows", "—"))
    st.caption(f"VALID {bronze.get('valid_count', '—')} · WARNING {bronze.get('warning_count', '—')} · INVALID {bronze.get('invalid_count', '—')}")
with columns[2]:
    st.write("**Bronze → Silver**")
    st.metric("Output / input run", f"{silver.get('output_rows', '—')} / {silver.get('input_rows', '—')}")
    st.caption(f"Non-empty batches: {silver.get('non_empty_batches', '—')}")

st.subheader("Chất lượng Bronze theo cửa sổ 5 phút")
quality_rows = [
    {"window_start": row["window_start"], **row["values"]}
    for row in history
    if row.get("component") == "kafka_to_bronze_dlq"
    and row.get("metric_name") == "quality_counts"
]
if quality_rows:
    quality = pd.DataFrame(quality_rows)
    quality["window_start"] = pd.to_datetime(quality["window_start"], utc=True)
    quality = quality.sort_values("window_start")
    st.line_chart(quality, x="window_start", y=["valid_count", "warning_count", "invalid_count"])
else:
    st.info("Chưa có metric chất lượng Bronze.")

st.subheader("Alerts")
if summary.get("open_alerts"):
    st.dataframe(pd.DataFrame(summary["open_alerts"]), hide_index=True)
else:
    st.caption("Không có alert đang mở.")
if alerts:
    st.write("Lịch sử chuyển trạng thái gần nhất")
    st.dataframe(pd.DataFrame(alerts[-20:][::-1]), hide_index=True)

with st.expander("Chi tiết các metric-window gần nhất"):
    rows = [
        {"window_start": row.get("window_start"), "component": row.get("component"),
         "metric_name": row.get("metric_name"), "status": row.get("status"),
         **row.get("values", {})}
        for row in history[-30:][::-1]
    ]
    st.dataframe(pd.DataFrame(rows), hide_index=True)
