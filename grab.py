"""
Run this script LOCALLY (not in Snowsight) to download all files from
the demo_enterprise workspace folder.

Prerequisites:
    pip install snowflake-connector-python

Usage:
    python download_all.py

It will prompt for your Snowflake credentials (or you can set env vars),
copy workspace files to a temp stage, GET them locally, then clean up.
"""

import os
import snowflake.connector

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "ARAYDYB-AL07847")
USER = os.environ.get("SNOWFLAKE_USER", "MSID9228")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "Ch@t1m3PVJ!!!!")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
ROLE = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

WORKSPACE_PATH = "snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/demo_enterprise"
TEMP_DB = "TEMP_DOWNLOAD_DB"
TEMP_SCHEMA = "PUBLIC"
TEMP_STAGE = "TEMP_DOWNLOAD_STAGE"
LOCAL_DIR = os.path.join(os.getcwd(), "demo_enterprise")

if not PASSWORD:
    import getpass
    PASSWORD = getpass.getpass(f"Snowflake password for {USER}@{ACCOUNT}: ")

print(f"Connecting to {ACCOUNT} as {USER}...")
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    warehouse=WAREHOUSE,
    role=ROLE,
)
cur = conn.cursor()

try:
    print("Setting up temporary stage...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {TEMP_DB}")
    cur.execute(f"USE DATABASE {TEMP_DB}")
    cur.execute(f"USE SCHEMA {TEMP_SCHEMA}")
    cur.execute(f"CREATE OR REPLACE STAGE {TEMP_STAGE} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')")

    print(f"Copying workspace files to stage...")
    cur.execute(f"""
        COPY FILES INTO @{TEMP_DB}.{TEMP_SCHEMA}.{TEMP_STAGE}/demo_enterprise
        FROM '{WORKSPACE_PATH}'
    """)

    print("Listing staged files...")
    cur.execute(f"LIST @{TEMP_DB}.{TEMP_SCHEMA}.{TEMP_STAGE}/demo_enterprise")
    files = cur.fetchall()
    print(f"Found {len(files)} files.")

    os.makedirs(LOCAL_DIR, exist_ok=True)

    for row in files:
        # row[0] is the full path, e.g., 'temp_download_db/public/temp_download_stage/demo_enterprise/file.txt'
        full_stage_path = row[0]

        # Get the part after 'demo_enterprise/' to recreate local folder structure
        relative = full_stage_path.split("/demo_enterprise/", 1)[-1]

        # Setup local paths
        local_subdir = os.path.abspath(os.path.join(LOCAL_DIR, os.path.dirname(relative)))
        local_path_fixed = local_subdir.replace('\\', '/')
        os.makedirs(local_subdir, exist_ok=True)

        file_name = os.path.basename(relative)

        # Use the absolute stage reference directly
        # We wrap it in @... to tell Snowflake it's a stage path
        stage_ref = f"@{full_stage_path}"

        print(f"  Downloading: {relative}")
        cur.execute(f"GET '{stage_ref}' 'file://{local_path_fixed}'")

    print(f"\nDone! Files saved to: {LOCAL_DIR}")

finally:
    print("Cleaning up temporary stage and database...")
    cur.execute(f"DROP STAGE IF EXISTS {TEMP_DB}.{TEMP_SCHEMA}.{TEMP_STAGE}")
    cur.execute(f"DROP DATABASE IF EXISTS {TEMP_DB}")
    cur.close()
    conn.close()
    print("Cleanup complete.")
