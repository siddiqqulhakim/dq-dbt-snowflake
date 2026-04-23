#!/usr/bin/env python3
"""
Ingest dbt test results into DataHub.

Prerequisites:
    pip install 'acryl-datahub[dbt]'

Usage:
    export DATAHUB_GMS_URL="https://your-datahub-instance:8080"
    export DATAHUB_TOKEN="your-access-token"
    python ingest_to_datahub.py [--skip-docs-generate]

Run this script from the demo_enterprise/ project root directory.
"""

import argparse
import os
import subprocess
import sys


def run_cmd(cmd: list[str]) -> None:
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Ingest dbt quality results into DataHub")
    parser.add_argument(
        "--skip-docs-generate",
        action="store_true",
        help="Skip running 'dbt docs generate' (use existing catalog.json)",
    )
    parser.add_argument(
        "--recipe",
        default="datahub_ingestion.yml",
        help="Path to the DataHub ingestion recipe (default: datahub_ingestion.yml)",
    )
    args = parser.parse_args()

    if not args.skip_docs_generate:
        print("==> Generating dbt catalog (dbt docs generate)...")
        run_cmd(["dbt", "docs", "generate"])

    if not os.path.exists("target/catalog.json"):
        print(
            "Warning: target/catalog.json not found. "
            "Run 'dbt docs generate' first or use --skip-docs-generate if catalog is optional.",
            file=sys.stderr,
        )

    print("==> Running DataHub ingestion...")
    run_cmd(["datahub", "ingest", "-c", args.recipe])

    print("Done. dbt test results have been ingested into DataHub.")


if __name__ == "__main__":
    main()
