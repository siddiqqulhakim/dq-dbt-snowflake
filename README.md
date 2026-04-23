# demo_enterprise — dbt Project

A multi-layer (Bronze / Silver / Gold) dbt project that transforms data from three domains — **Finance**, **HR**, and **Operations** — with Snowflake-native governance (tags and dynamic masking policies).

---

## Prerequisites

### 1. Snowflake Objects

The following Snowflake objects **must exist before** running `dbt build`.

#### Warehouse

| Object | Purpose |
|--------|---------|
| `DEMO_ENTERPRISE_WH` | Warehouse used by dbt (set in `profiles.yml`) |

#### Databases

| Object | Purpose |
|--------|---------|
| `DEMO_ENTERPRISE_DB` | Source database containing raw data |
| `DEMO_ENTERPRISE_DBT_DB` | Target database where dbt materializes models |

#### Source Schemas (in `DEMO_ENTERPRISE_DB`)

| Schema | Tables Required |
|--------|-----------------|
| `FINANCE` | `entity_dim`, `activity_fact`, `financial_fact`, `location_dim`, `reference_status` |
| `HR` | `entity_dim`, `activity_fact`, `financial_fact`, `location_dim`, `reference_status` |
| `OPERATIONS` | `entity_dim`, `activity_fact`, `financial_fact`, `location_dim`, `reference_status` |

#### Target Schemas (in `DEMO_ENTERPRISE_DBT_DB`)

These schemas are referenced by the `generate_schema_name` macro and must be created before running dbt:

```sql
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOLD;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE;
```

#### Snowflake Tags (in `DEMO_ENTERPRISE_DBT_DB.PUBLIC`)

The `apply_snowflake_tags` post-hook applies three tags to every model. **These tags must be created before running any models:**

```sql
CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_layer
    COMMENT = 'Medallion layer: brz, slv, gld';

CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_domain
    COMMENT = 'Business domain: finance, hr, operations';

CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_context
    COMMENT = 'Model context / object type derived from model name';
```

> **This is the root cause of the `Tag does not exist` error.** All 9 failing Bronze models fail because these tags have not been created yet.

#### Masking Policies (in `DEMO_ENTERPRISE_DBT_DB.GOVERNANCE`)

The `apply_masking_policies` post-hook attaches dynamic masking policies to sensitive columns (email, phone, account_number, credit_card) in entity dimension models. Create the policies before running models that reference them:

```sql
CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_EMAIL
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_PHONE
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_ACCOUNT_NUMBER
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_CREDIT_CARD
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;
```

> Adjust the `CURRENT_ROLE()` logic to match your organization's access control requirements.

### 2. Role & Permissions

The `profiles.yml` is configured to use the `ACCOUNTADMIN` role. The executing role needs at minimum:

- `USAGE` on warehouse `DEMO_ENTERPRISE_WH`
- `USAGE` on database `DEMO_ENTERPRISE_DB` and all source schemas
- `CREATE TABLE`, `CREATE VIEW`, `USAGE` on database `DEMO_ENTERPRISE_DBT_DB` and target schemas (`PUBLIC`, `BRONZE`, `SILVER`, `GOLD`, `GOVERNANCE`)
- `APPLY TAG` privilege on the three tags
- `APPLY MASKING POLICY` privilege on the four masking policies

### 3. dbt Version

- **dbt-core**: `1.9.x`
- **dbt-snowflake adapter**: compatible with dbt-core 1.9

---

## Project Structure

```
demo_enterprise/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── macros/
│   ├── generate_schema_name.sql   # Routes models to BRONZE/SILVER/GOLD schemas
│   ├── apply_snowflake_tags.sql   # Post-hook: sets dbt_layer/domain/context tags
│   ├── apply_masking_policies.sql # Post-hook: attaches masking policies to PII columns
│   └── masking_policy_registry.sql
├── models/
│   ├── sources/                   # Source definitions (3 domains)
│   ├── BRONZE/                    # Raw views over source tables (9 models)
│   │   ├── FINANCE/
│   │   ├── HR/
│   │   └── OPERATIONS/
│   ├── SILVER/                    # Cleaned/deduplicated tables (9 models)
│   │   ├── FINANCE/
│   │   ├── HR/
│   │   └── OPERATIONS/
│   └── GOLD/                      # Aggregated business-level tables (3 models)
│       ├── FINANCE/
│       └── SUPPORT_DATA/MINING/
```

**Total models**: 21 (9 Bronze views + 9 Silver tables + 3 Gold tables)

---

## Quick Start

### 1. Create all prerequisite Snowflake objects

Run the full setup script below (requires `ACCOUNTADMIN` or equivalent):

```sql
-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS DEMO_ENTERPRISE_WH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Target database & schemas
CREATE DATABASE IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOLD;
CREATE SCHEMA IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE;

-- Tags
CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_layer
    COMMENT = 'Medallion layer: brz, slv, gld';
CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_domain
    COMMENT = 'Business domain: finance, hr, operations';
CREATE TAG IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.dbt_context
    COMMENT = 'Model context / object type derived from model name';

-- Masking policies
CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_EMAIL
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_PHONE
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_ACCOUNT_NUMBER
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;

CREATE MASKING POLICY IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.GOVERNANCE.MASK_CREDIT_CARD
    AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','DATA_ENGINEER') THEN val
         ELSE '***MASKED***' END;
                             
                             
-- test result
CREATE TABLE IF NOT EXISTS DEMO_ENTERPRISE_DBT_DB.PUBLIC.DBT_TEST_RESULTS (
    test_name         STRING,
    model_name        STRING,
    column_name       STRING,
    status            STRING,
    failures          NUMBER,
    execution_time_s  FLOAT,
    test_type         STRING,
    layer             STRING,
    domain            STRING,
    run_started_at    TIMESTAMP_NTZ,
    inserted_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2. Verify source data exists

Ensure `DEMO_ENTERPRISE_DB` has the three schemas (`FINANCE`, `HR`, `OPERATIONS`) each with tables: `entity_dim`, `activity_fact`, `financial_fact`, `location_dim`, `reference_status`.

### 3. Install packages & run

```bash
dbt deps
dbt build
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Tag 'DEMO_ENTERPRISE_DBT_DB.PUBLIC.DBT_LAYER' does not exist` | Tags not created | Run the `CREATE TAG` statements above |
| `Masking policy ... does not exist` | Policies not created | Run the `CREATE MASKING POLICY` statements above |
| `SQL compilation error: Object does not exist` on source tables | Source data missing | Load data into `DEMO_ENTERPRISE_DB.{FINANCE,HR,OPERATIONS}` |
| `Warehouse does not exist` | Warehouse not created | Run the `CREATE WAREHOUSE` statement above |
