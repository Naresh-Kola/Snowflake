# Snowflake OpenFlow: Complete Step-by-Step Guide (Source to Target)

## Table of Contents

1. [What is OpenFlow?](#1-what-is-openflow)
2. [Core Concepts](#2-core-concepts)
3. [Architecture Overview](#3-architecture-overview)
4. [Deployment Types](#4-deployment-types)
5. [Step-by-Step: Source to Target](#5-step-by-step-source-to-target)
6. [Available Connectors](#6-available-connectors)
7. [Monitoring and Troubleshooting](#7-monitoring-and-troubleshooting)
8. [POC: PostgreSQL CDC to Snowflake using OpenFlow](#8-poc-postgresql-cdc-to-snowflake-using-openflow)

---

## 1. What is OpenFlow?

OpenFlow is a **Snowflake-native data integration product** built on top of Apache NiFi. It allows you to move data from external sources (databases, SaaS apps, streaming platforms, file systems) into Snowflake **in real-time or near real-time** without writing custom ETL code.

**In simple terms:** OpenFlow is a drag-and-drop data pipeline tool that Snowflake hosts and manages for you. You pick your source, configure credentials, and data starts flowing into your Snowflake tables automatically.

### Why Use OpenFlow?

- **No infrastructure to manage** -- Snowflake runs it for you (in SPCS mode)
- **Pre-built connectors** -- Ready-made pipelines for popular sources like PostgreSQL, MySQL, Kafka, Salesforce, etc.
- **Real-time CDC support** -- Capture inserts, updates, and deletes as they happen
- **Visual flow design** -- Apache NiFi UI for building and monitoring data flows
- **Built-in backpressure** -- Automatically slows down ingestion if downstream cannot keep up
- **Data provenance** -- Full lineage tracking of every record from source to destination

---

## 2. Core Concepts

Before diving in, understand these foundational building blocks:

### FlowFile

A **FlowFile** is a single unit of data moving through the pipeline. Think of it as an envelope containing:

| Part | What It Is |
|------|-----------|
| **Content** | The actual data (a row, a JSON message, a file) |
| **Attributes** | Metadata about the data (filename, source table, timestamp, etc.) |

### Processor

A **Processor** is a worker that performs one specific task on a FlowFile:

- **Source processors** pull data in (e.g., read from PostgreSQL)
- **Transform processors** modify data (e.g., convert JSON to Avro)
- **Destination processors** push data out (e.g., write to Snowflake table)

### Connection

A **Connection** is the link between two processors. It contains a **queue** where FlowFiles wait to be processed. Connections have **backpressure** -- if the queue gets too full (default: 10,000 items or 1 GB), the upstream processor pauses automatically.

### Process Group

A **Process Group** is a container that bundles multiple processors, connections, and other components into one logical unit. A deployed connector is typically one Process Group.

### Controller Service

A **Controller Service** is a shared resource used by multiple processors -- for example, a database connection pool or a JSON parser. These must be **enabled** before processors can use them.

### Relationship

When a processor finishes working on a FlowFile, it sends the result to a **relationship** like `success` or `failure`. Connections are wired to specific relationships to define where data goes next.

---

## 3. Architecture Overview

```
+---------------------+       +---------------------------+       +------------------+
|                     |       |    SNOWFLAKE OPENFLOW     |       |                  |
|   SOURCE SYSTEMS    |       |                           |       |   SNOWFLAKE      |
|                     |       |  +---------------------+  |       |   (Target)       |
|  - PostgreSQL       | ----> |  | Source Processor     |  |       |                  |
|  - MySQL            |       |  | (reads data)         |  |       |  +------------+  |
|  - Kafka            |       |  +----------+----------+  |       |  |            |  |
|  - Salesforce       |       |             |              |       |  | Your       |  |
|  - Google Drive     |       |  +----------v----------+  |       |  | Tables     |  |
|  - SharePoint       |       |  | Transform Processor  |  |       |  |            |  |
|  - Jira             |       |  | (optional: convert,  |  |       |  +------------+  |
|  - Slack            |       |  |  filter, enrich)     |  |       |                  |
|  - etc.             |       |  +----------+----------+  |       |                  |
|                     |       |             |              | ----> |  Snowpipe        |
+---------------------+       |  +----------v----------+  |       |  Streaming       |
                              |  | Destination Processor|  |       |                  |
                              |  | (PutSnowpipe         |  |       +------------------+
                              |  |  Streaming)           |  |
                              |  +---------------------+  |
                              |                           |
                              |  Runtime (NiFi engine)    |
                              +---------------------------+
```

### Data Flow Summary

1. **Source Processor** reads data from your external system (database, API, file store)
2. **Transform Processors** (optional) convert formats, filter rows, enrich data
3. **Destination Processor** (`PutSnowpipeStreaming`) writes records into your Snowflake tables via Snowpipe Streaming
4. **Controller Services** provide shared resources like connection pools and parsers

---

## 4. Deployment Types

OpenFlow supports two deployment models:

### SPCS (Snowflake-managed) -- Recommended

| Aspect | Detail |
|--------|--------|
| **Where it runs** | Inside Snowflake's Snowpark Container Services |
| **Authentication to Snowflake** | Automatic (session token) |
| **Network access to sources** | Requires External Access Integration (EAI) |
| **URL pattern** | Starts with `of--` (e.g., `https://of--account.snowflakecomputing.app/...`) |
| **Management** | Snowflake handles infrastructure |

### BYOC (Bring Your Own Cloud)

| Aspect | Detail |
|--------|--------|
| **Where it runs** | Your own cloud account (AWS, Azure, GCP) |
| **Authentication to Snowflake** | Key-pair authentication |
| **Network access to sources** | Direct (no EAI needed) |
| **URL pattern** | Contains `snowflake-customer.app` |
| **Management** | You manage the infrastructure |

**For most new projects, SPCS is the recommended starting point.**

---

## 5. Step-by-Step: Source to Target

Here is the complete journey to get data flowing from a source system into Snowflake using OpenFlow.

### Step 1: Set Up the OpenFlow Environment

Before anything else, you need the OpenFlow infrastructure ready.

**What you need:**
- A Snowflake account with OpenFlow enabled
- An OpenFlow **Deployment** (created via the OpenFlow Control Plane UI)
- An OpenFlow **Runtime** (the NiFi engine that runs inside the deployment)
- CLI tools installed: `snow` (Snowflake CLI) and `nipyapi` (NiFi API client)

**Verify your tools:**
```bash
which snow && which nipyapi && which python3
```

**Test your Snowflake connection:**
```bash
snow connection test -c <YOUR_CONNECTION>
```

### Step 2: Configure Network Access (SPCS Only)

If your OpenFlow runtime is SPCS-managed, it runs inside Snowflake's network and **cannot reach external sources by default**. You must create an External Access Integration (EAI).

**a) Create a Network Rule** (defines which external hosts/ports are allowed):
```sql
USE ROLE SECURITYADMIN;

CREATE NETWORK RULE my_source_network_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('<source-host>:<port>');
```

Example for PostgreSQL:
```sql
CREATE NETWORK RULE postgres_openflow_network_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('my-postgres-server.example.com:5432');
```

**b) Create the External Access Integration:**
```sql
CREATE EXTERNAL ACCESS INTEGRATION postgres_openflow_eai
  ALLOWED_NETWORK_RULES = (postgres_openflow_network_rule)
  ENABLED = TRUE
  COMMENT = 'External Access Integration for OpenFlow PostgreSQL connectivity';
```

**c) Grant USAGE to your Runtime Role:**
```sql
GRANT USAGE ON INTEGRATION postgres_openflow_eai TO ROLE <runtime_role>;
```

**d) Attach EAI to Runtime** (done in the OpenFlow Control Plane UI):
1. Navigate to the OpenFlow Control Plane
2. Find your Runtime
3. Click "..." menu > "External access integrations"
4. Select the EAI and click Save

### Step 3: Deploy the Connector

OpenFlow provides pre-built connectors in the **Snowflake Openflow Connector Registry**. You deploy them using `nipyapi`.

**a) List available connectors:**
```bash
nipyapi --profile <profile> ci deploy_flow --help
```

**b) Deploy the connector** (e.g., PostgreSQL CDC):
```bash
nipyapi --profile <profile> ci deploy_flow \
  --bucket "Snowflake Openflow Connector Registry" \
  --flow "postgresql" \
  --comment "PostgreSQL CDC connector"
```

This creates a new Process Group on your NiFi canvas with all the processors pre-wired.

### Step 4: Configure Parameters

Every connector needs configuration -- source credentials, target database/schema, etc.

**a) View what parameters need to be set:**
```bash
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"
```

**b) Set parameters** (source connection details and Snowflake destination):
```bash
nipyapi --profile <profile> ci configure_inherited_params \
  --process_group_id "<pg-id>" \
  --parameters '{
    "source_host": "my-postgres-server.example.com",
    "source_port": "5432",
    "source_database": "mydb",
    "source_user": "replication_user",
    "source_password": "***",
    "snowflake_database": "MY_SF_DB",
    "snowflake_schema": "RAW",
    "snowflake_warehouse": "LOAD_WH"
  }'
```

The exact parameter names vary by connector -- check the connector documentation.

### Step 5: Grant Snowflake Permissions to the Runtime Role

The OpenFlow runtime role needs permission to write to your target tables:

```sql
GRANT USAGE ON DATABASE MY_SF_DB TO ROLE <runtime_role>;
GRANT USAGE ON SCHEMA MY_SF_DB.RAW TO ROLE <runtime_role>;
GRANT CREATE TABLE ON SCHEMA MY_SF_DB.RAW TO ROLE <runtime_role>;
```

### Step 6: Verify Configuration

Before starting, verify that everything is configured correctly.

**a) Verify controller services (connection pools, parsers):**
```bash
nipyapi --profile <profile> ci verify_config \
  --process_group_id "<pg-id>" \
  --verify_processors=false
```

**b) Enable controller services:**
After verification passes, enable the controller services so processors can use them.

**c) Verify processors:**
```bash
nipyapi --profile <profile> ci verify_config \
  --process_group_id "<pg-id>" \
  --verify_controllers=false
```

### Step 7: Start the Flow

```bash
nipyapi --profile <profile> ci start_flow --process_group_id "<pg-id>"
```

### Step 8: Validate Data is Flowing

**a) Check status:**
```bash
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"
```

You should see:
- `running_processors` > 0
- `invalid_processors` = 0
- `bulletin_errors` = 0

**b) Check your Snowflake table:**
```sql
SELECT COUNT(*) FROM MY_SF_DB.RAW.MY_TABLE;
SELECT * FROM MY_SF_DB.RAW.MY_TABLE LIMIT 10;
```

If records appear, your pipeline is live.

---

## 6. Available Connectors

### Ingress Connectors (External -> Snowflake)

| Category | Connector | Use Case |
|----------|-----------|----------|
| **Databases** | PostgreSQL CDC | Real-time replication from PostgreSQL |
| | MySQL CDC | Real-time replication from MySQL |
| | SQL Server CDC | Real-time replication from SQL Server |
| | Oracle CDC | Real-time replication from Oracle |
| | MongoDB | Data from MongoDB |
| **SaaS / CRM** | Salesforce | CRM data into Snowflake |
| | HubSpot | Marketing/CRM data |
| | Jira | Project management data |
| | Workday | HR data |
| | Microsoft Dataverse | Dynamics 365 data |
| **File / Document** | Google Drive | Unstructured docs (with Cortex) |
| | SharePoint | SharePoint documents and metadata |
| | Box | Box files and metadata |
| **Streaming** | Kafka (SASL) | Stream from Kafka topics |
| | Kinesis (JSON) | Stream from AWS Kinesis |
| **Productivity** | Google Sheets | Spreadsheet data |
| | Slack | Slack messages and channels |
| **Advertising** | Google Ads | Ad campaign data |
| | Meta Ads | Facebook/Instagram ad data |
| | LinkedIn Ads | LinkedIn campaign data |
| | Amazon Ads | Amazon advertising data |

### Egress Connectors (Snowflake -> External)

| Connector | Use Case |
|-----------|----------|
| Kafka Sink (SASL/IAM/mTLS) | Push data from Snowflake to Kafka |
| Snowflake to Box | Sync files to Box |

---

## 7. Monitoring and Troubleshooting

### Checking Flow Health

```bash
# Overall status
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"

# Check for error bulletins
nipyapi --profile <profile> bulletins get_bulletin_board

# List all processors and their states
nipyapi --profile <profile> canvas list_all_processors --pg_id "<pg-id>"
```

### Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `UnknownHostException` | EAI missing the source domain | Add host:port to your Network Rule |
| `SocketTimeoutException` | Port not in Network Rule, or blocked | Verify VALUE_LIST includes correct port |
| `401 Unauthorized` | Expired token or bad credentials | Refresh PAT or update source credentials |
| `invalid_processors > 0` | Controller services not enabled or bad config | Enable controllers, then re-verify |
| `bulletin_errors > 0` | Runtime errors during processing | Check bulletins for specific error messages |
| DATE column as VARCHAR | Missing `logicalType: date` in Avro schema | Set explicit schema with logicalType annotations |

### Key Operational Commands

| Action | Command |
|--------|---------|
| Stop a flow | `nipyapi --profile <p> ci stop_flow --process_group_id "<id>"` |
| Start a flow | `nipyapi --profile <p> ci start_flow --process_group_id "<id>"` |
| Get status | `nipyapi --profile <p> ci get_status --process_group_id "<id>"` |
| Check bulletins | `nipyapi --profile <p> bulletins get_bulletin_board` |
| Clean up a flow | `nipyapi --profile <p> ci cleanup --process_group_id "<id>"` |

---

## 8. POC: PostgreSQL CDC to Snowflake Using OpenFlow

This proof-of-concept demonstrates a real-time Change Data Capture (CDC) pipeline that captures every INSERT, UPDATE, and DELETE from a PostgreSQL database and replicates it into Snowflake.

### POC Goal

Replicate the `orders` and `customers` tables from a PostgreSQL database into Snowflake in real-time, so that any change in the source is reflected in Snowflake within seconds.

### Prerequisites

| Requirement | Detail |
|-------------|--------|
| PostgreSQL version | 10+ with logical replication enabled |
| PostgreSQL setting | `wal_level = logical` in `postgresql.conf` |
| PostgreSQL user | A user with `REPLICATION` privilege |
| Snowflake account | With OpenFlow enabled |
| OpenFlow deployment | SPCS runtime created and running |
| CLI tools | `snow` and `nipyapi` installed and configured |

### Step-by-Step POC Execution

#### 1. Prepare PostgreSQL Source

On your PostgreSQL server, ensure logical replication is enabled:

```sql
-- Check current WAL level (must be 'logical')
SHOW wal_level;

-- If not 'logical', update postgresql.conf:
--   wal_level = logical
-- Then restart PostgreSQL.

-- Create a dedicated replication user
CREATE ROLE openflow_repl WITH REPLICATION LOGIN PASSWORD 'SecurePass123!';

-- Grant access to the source tables
GRANT USAGE ON SCHEMA public TO openflow_repl;
GRANT SELECT ON public.orders TO openflow_repl;
GRANT SELECT ON public.customers TO openflow_repl;
```

#### 2. Create Sample Source Tables (If Testing)

```sql
-- On PostgreSQL
CREATE TABLE public.orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product     VARCHAR(100),
    quantity    INTEGER,
    total_price NUMERIC(10,2),
    order_date  DATE DEFAULT CURRENT_DATE,
    status      VARCHAR(20) DEFAULT 'PENDING',
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public.customers (
    customer_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(50),
    last_name     VARCHAR(50),
    email         VARCHAR(100),
    city          VARCHAR(50),
    created_at    TIMESTAMP DEFAULT NOW()
);
```

#### 3. Set Up Network Access (SPCS)

```sql
-- In Snowflake
USE ROLE SECURITYADMIN;

-- Create network rule for PostgreSQL host
CREATE NETWORK RULE postgres_poc_network_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('my-postgres-host.example.com:5432');

-- Create External Access Integration
CREATE EXTERNAL ACCESS INTEGRATION postgres_poc_eai
  ALLOWED_NETWORK_RULES = (postgres_poc_network_rule)
  ENABLED = TRUE
  COMMENT = 'EAI for OpenFlow PostgreSQL CDC POC';

-- Grant to runtime role
GRANT USAGE ON INTEGRATION postgres_poc_eai TO ROLE <your_runtime_role>;
```

Then attach the EAI to your Runtime via the OpenFlow Control Plane UI.

#### 4. Prepare Snowflake Target

```sql
-- Create target database and schema
CREATE DATABASE IF NOT EXISTS OPENFLOW_POC;
CREATE SCHEMA IF NOT EXISTS OPENFLOW_POC.RAW_CDC;

-- Grant permissions to the OpenFlow runtime role
GRANT USAGE ON DATABASE OPENFLOW_POC TO ROLE <your_runtime_role>;
GRANT USAGE ON SCHEMA OPENFLOW_POC.RAW_CDC TO ROLE <your_runtime_role>;
GRANT CREATE TABLE ON SCHEMA OPENFLOW_POC.RAW_CDC TO ROLE <your_runtime_role>;

-- Create a warehouse for OpenFlow operations
CREATE WAREHOUSE IF NOT EXISTS OPENFLOW_POC_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

GRANT USAGE ON WAREHOUSE OPENFLOW_POC_WH TO ROLE <your_runtime_role>;
```

#### 5. Deploy the PostgreSQL CDC Connector

```bash
# Deploy from the Snowflake connector registry
nipyapi --profile <profile> ci deploy_flow \
  --bucket "Snowflake Openflow Connector Registry" \
  --flow "postgresql" \
  --comment "POC: PostgreSQL CDC to Snowflake"
```

Note the Process Group ID returned -- you will need it for the following steps.

#### 6. Configure the Connector Parameters

```bash
nipyapi --profile <profile> ci configure_inherited_params \
  --process_group_id "<pg-id>" \
  --parameters '{
    "source_host": "my-postgres-host.example.com",
    "source_port": "5432",
    "source_database": "mydb",
    "source_user": "openflow_repl",
    "source_password": "SecurePass123!",
    "source_tables": "public.orders,public.customers",
    "snowflake_database": "OPENFLOW_POC",
    "snowflake_schema": "RAW_CDC",
    "snowflake_warehouse": "OPENFLOW_POC_WH"
  }'
```

#### 7. Verify and Enable

```bash
# Verify controller services
nipyapi --profile <profile> ci verify_config \
  --process_group_id "<pg-id>" \
  --verify_processors=false

# (Enable controller services after verification passes)

# Verify processors
nipyapi --profile <profile> ci verify_config \
  --process_group_id "<pg-id>" \
  --verify_controllers=false
```

#### 8. Start the Flow

```bash
# Start the CDC pipeline
nipyapi --profile <profile> ci start_flow --process_group_id "<pg-id>"

# Confirm it is running
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"
```

Expected output:
- `running_processors` > 0
- `invalid_processors` = 0
- `bulletin_errors` = 0

#### 9. Test with Live Data Changes

Now make changes on the PostgreSQL source and watch them appear in Snowflake.

**Insert records (on PostgreSQL):**
```sql
INSERT INTO public.customers (first_name, last_name, email, city)
VALUES
  ('Naresh', 'Kola', 'naresh@example.com', 'Hyderabad'),
  ('Priya',  'Sharma', 'priya@example.com', 'Mumbai');

INSERT INTO public.orders (customer_id, product, quantity, total_price, status)
VALUES
  (1, 'Laptop', 1, 75000.00, 'CONFIRMED'),
  (1, 'Mouse',  2, 1500.00,  'PENDING'),
  (2, 'Keyboard', 1, 3000.00, 'CONFIRMED');
```

**Verify in Snowflake (within seconds):**
```sql
-- Check customers
SELECT * FROM OPENFLOW_POC.RAW_CDC.CUSTOMERS;

-- Check orders
SELECT * FROM OPENFLOW_POC.RAW_CDC.ORDERS;
```

**Test UPDATE (on PostgreSQL):**
```sql
UPDATE public.orders SET status = 'SHIPPED', updated_at = NOW()
WHERE order_id = 1;
```

**Test DELETE (on PostgreSQL):**
```sql
DELETE FROM public.orders WHERE order_id = 3;
```

**Verify changes in Snowflake:**
```sql
-- Confirm the update is reflected
SELECT order_id, status, updated_at FROM OPENFLOW_POC.RAW_CDC.ORDERS;
```

#### 10. Monitor the Pipeline

```bash
# Check health
nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"

# Check for errors
nipyapi --profile <profile> bulletins get_bulletin_board
```

### POC Success Criteria

| Criteria | How to Verify |
|----------|---------------|
| Initial data load | `SELECT COUNT(*) FROM OPENFLOW_POC.RAW_CDC.ORDERS;` returns rows |
| Real-time inserts | New rows appear in Snowflake within seconds of PostgreSQL INSERT |
| Real-time updates | Modified rows in Snowflake reflect PostgreSQL UPDATE |
| Real-time deletes | Deleted rows are handled (check CDC delete markers) |
| No errors | `bulletin_errors = 0` in status check |
| Pipeline is stable | `running_processors > 0` and `invalid_processors = 0` |

### POC Cleanup (When Done)

```bash
# Stop the flow
nipyapi --profile <profile> ci stop_flow --process_group_id "<pg-id>"

# Clean up (removes the process group)
nipyapi --profile <profile> ci cleanup --process_group_id "<pg-id>"
```

```sql
-- Drop Snowflake objects
DROP SCHEMA IF EXISTS OPENFLOW_POC.RAW_CDC CASCADE;
DROP DATABASE IF EXISTS OPENFLOW_POC;
DROP WAREHOUSE IF EXISTS OPENFLOW_POC_WH;

-- Drop network objects
DROP INTEGRATION IF EXISTS postgres_poc_eai;
DROP NETWORK RULE IF EXISTS postgres_poc_network_rule;
```

---

## Summary

| Phase | What Happens |
|-------|-------------|
| **Setup** | Create deployment, runtime, install CLI tools |
| **Network** | Create EAI + Network Rule (SPCS only) so runtime can reach your source |
| **Deploy** | Pull a pre-built connector from the Snowflake registry |
| **Configure** | Set source credentials, target database/schema, and other parameters |
| **Verify** | Validate controller services and processors before starting |
| **Start** | Start the flow -- data begins flowing from source to Snowflake |
| **Monitor** | Check status, bulletins, and query your Snowflake tables |

OpenFlow takes the complexity out of real-time data ingestion by providing managed infrastructure, pre-built connectors, and a visual interface -- all running natively within the Snowflake ecosystem.
