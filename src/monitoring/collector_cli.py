"""Run one bounded monitoring collection pass from host or Airflow."""

import argparse
import os
from pathlib import Path

from src.monitoring.collector_store import collect_once


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect the three monitoring JSONL sources once")
    parser.add_argument("--root", type=Path, default=Path(os.getenv("MONITORING_DIR", "monitoring")))
    parser.add_argument("--registry", type=Path,
                        default=Path(__file__).with_name("known_invalid_events_v1.json"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="validate and aggregate in memory (the default)")
    mode.add_argument("--commit", action="store_true",
                      help="write derived outputs and cursor; run only as the Airflow owner")
    args = parser.parse_args()
    dry_run = not args.commit
    result = collect_once(args.root, args.registry, dry_run=dry_run)
    marker = "MONITORING_COLLECTOR_DRY_RUN_OK" if dry_run else "MONITORING_COLLECTOR_OK"
    print(f"{marker} changed={result['changed']} accepted_events={result['accepted_events']} "
          f"rejected_events={result['rejected_events']} windows={result['windows']} "
          f"alerts_status={result['alerts_status']} alert_transitions={result['alert_transitions']} "
          f"root={args.root}")


if __name__ == "__main__":
    main()
