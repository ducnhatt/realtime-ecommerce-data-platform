"""Validate the latest completed Bronze-to-Silver monitoring run."""

import argparse
import json
from pathlib import Path


EXPECTED = {"run_counts", "run_performance", "checkpoint_progress"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", default="monitoring/silver_stream_monitor.jsonl"
    )
    parser.add_argument("--expected-input-rows", type=int)
    args = parser.parse_args()

    events = [
        json.loads(line)
        for line in Path(args.path).read_text(encoding="utf-8").splitlines()
    ]
    events = [
        event for event in events
        if event.get("component") == "bronze_to_silver_incremental"
    ]
    if not events:
        raise RuntimeError("No Silver monitoring events found")
    run_id = events[-1]["run_id"]
    run_events = [event for event in events if event["run_id"] == run_id]
    names = {event["metric_name"] for event in run_events}
    if len(run_events) != 3 or names != EXPECTED:
        raise RuntimeError(f"Expected three success metrics, got {names}")
    if {event["status"] for event in run_events} != {"OK"}:
        raise RuntimeError("Latest Silver run is not OK")
    counts = next(
        event["values"] for event in run_events
        if event["metric_name"] == "run_counts"
    )
    if counts["input_rows"] != counts["output_rows"]:
        raise RuntimeError("Silver input/output counts differ")
    if (
        args.expected_input_rows is not None
        and counts["input_rows"] != args.expected_input_rows
    ):
        raise RuntimeError(
            f"Expected input_rows={args.expected_input_rows}, "
            f"actual={counts['input_rows']}"
        )
    print(
        "SILVER_MONITORING_CONTRACT_OK "
        f"run_id={run_id} input_rows={counts['input_rows']} "
        f"output_rows={counts['output_rows']} "
        f"non_empty_batches={counts['non_empty_batches']} events=3"
    )


if __name__ == "__main__":
    main()
