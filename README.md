# GitHub CDC Data Engineering Pipeline

An end-to-end Data Engineering pipeline that ingests GitHub repository data, stores it in PostgreSQL, captures database changes using Change Data Capture (CDC), streams those changes through Apache Kafka, processes them with Apache Spark Structured Streaming, and generates analytical datasets in Parquet format.

## Architecture

```text
                         GitHub REST API
                               │
                               ▼
                       Python Ingestion
                               │
                               ▼
                           Raw JSON
                               │
                               ▼
                    Python Transformation
                               │
                               ▼
                          PostgreSQL
                               │
                         PostgreSQL WAL
                               │
                               ▼
                           Debezium
                               │
                               ▼
                        Apache Kafka
                         │          │
                         │          └──────────────► Python CDC Consumer
                         │
                         ▼
                Spark Structured Streaming
                         │
                         ▼
                  Normalized CDC Data
                         │
                         ▼
                 CDC Current-State Layer
                         │
                         ▼
                   Spark Analytics
                         │
                         ▼
                        Parquet
```

## Overview

This project demonstrates a complete batch + streaming data pipeline using publicly available GitHub data.

The pipeline:

1. Extracts GitHub repository, commit, issue, and pull-request data using the GitHub REST API.
2. Stores raw API responses as JSON.
3. Transforms the API data into structured datasets.
4. Loads the transformed data into PostgreSQL.
5. Enables PostgreSQL logical replication.
6. Uses Debezium to capture row-level database changes.
7. Publishes CDC events to Apache Kafka.
8. Processes Kafka events using Spark Structured Streaming.
9. Normalizes Debezium CDC events into table-specific datasets.
10. Builds current-state datasets by applying CDC semantics.
11. Generates repository-level analytics using Apache Spark.
12. Writes analytical datasets in Parquet format.
13. Runs automated data-quality checks.

## Technologies

* Python
* GitHub REST API
* PostgreSQL
* PostgreSQL Logical Replication
* Debezium
* Apache Kafka
* Apache Spark
* Spark Structured Streaming
* PySpark
* Docker / Docker Compose
* Parquet
* Git / GitHub

## Data Model

The PostgreSQL database contains four primary tables.

### Repositories

Stores repository metadata including:

* Repository ID
* Repository name
* Owner
* Description
* Language
* Stars
* Forks
* Watchers
* Open issues
* Repository timestamps
* Topics
* License

### Commits

Stores:

* Commit SHA
* Repository ID
* Author
* Author email
* Commit date
* Commit message

### Issues

Stores:

* Issue ID
* Issue number
* Repository ID
* Title
* State
* Author
* Creation/update timestamps
* Comments
* Labels
* URL

GitHub's Issues endpoint can also return pull requests, so pull-request records are filtered separately during ingestion.

### Pull Requests

Stores:

* Pull-request ID
* Pull-request number
* Repository ID
* Title
* State
* Author
* Creation/update timestamps
* URL

## CDC Pipeline

PostgreSQL is configured with:

```text
wal_level = logical
```

Debezium connects to PostgreSQL using the `pgoutput` logical replication plugin.

The CDC flow is:

```text
PostgreSQL Transaction
        │
        ▼
PostgreSQL WAL
        │
        ▼
Logical Replication
        │
        ▼
Debezium PostgreSQL Connector
        │
        ▼
Kafka Topic
        │
        ▼
Spark / Python Consumer
```

CDC operations are represented using:

| Operation | Meaning       |
| --------- | ------------- |
| `r`       | Snapshot/read |
| `c`       | Insert/create |
| `u`       | Update        |
| `d`       | Delete        |

The project also verifies live CDC behavior by updating a PostgreSQL record and observing the corresponding update event flow through Debezium and Kafka into Spark.

## Spark Structured Streaming

Spark consumes the Kafka topics:

```text
github_de.public.repositories
github_de.public.commits
github_de.public.issues
github_de.public.pull_requests
```

Debezium events are parsed and normalized into table-specific structures.

Processed streaming data is stored under:

```text
data/spark/processed/
```

with separate datasets for:

```text
repositories/
commits/
issues/
pull_requests/
```

## CDC Current-State Processing

CDC streams contain historical events rather than only the latest state.

For example:

```text
r → initial repository state
u → repository updated
u → repository updated again
```

The current-state layer identifies the latest event for each primary key and removes records whose latest event represents a deletion.

This produces clean current-state datasets:

```text
data/spark/current_state/
├── repositories/
├── commits/
├── issues/
└── pull_requests/
```

## Analytics

Spark generates repository-level analytical datasets including:

### Repository Popularity

Repository metadata with:

* Stars
* Forks
* Watchers
* Open issues
* Programming language

### Language Distribution

Aggregates repositories by programming language and calculates:

* Repository count
* Average stars
* Total stars

### Commit Activity

Calculates commit counts by repository.

### Issue Analytics

Calculates:

* Total issues
* Open issues
* Closed issues
* Average issue comments

### Pull Request Analytics

Calculates:

* Total pull requests
* Open pull requests
* Closed pull requests

### Final Repository Analytics

The final analytical dataset joins repository metadata with development activity:

```text
Repository
    │
    ├── Stars
    ├── Forks
    ├── Watchers
    ├── Language
    │
    ├── Commit Count
    │
    ├── Issue Count
    ├── Open Issues
    ├── Closed Issues
    ├── Average Issue Comments
    │
    ├── Pull Request Count
    ├── Open Pull Requests
    └── Closed Pull Requests
```

The final dataset is written as Parquet.

## Data Quality

Automated Spark data-quality checks validate:

* Expected row counts
* Duplicate primary keys
* Null primary keys
* Referential integrity

Current validation results:

```text
Repositories:      10 / 10
Commits:        1000 / 1000
Issues:           185 / 185
Pull Requests:  1000 / 1000

Duplicate primary keys: PASS
Null primary keys:      PASS
Referential integrity:  PASS

Overall: ALL CHECKS PASSED
```

## Project Dataset

The project currently processes:

* 10 GitHub repositories
* 1,000 commits
* 185 issues
* 1,000 pull requests

Repositories include projects such as:

* facebook/react
* tensorflow/tensorflow
* microsoft/vscode
* huggingface/transformers
* langchain-ai/langchain
* vercel/next.js
* kubernetes/kubernetes
* pytorch/pytorch
* apache/spark
* apache/kafka

## Project Structure

```text
github_de_project/
│
├── src/
│   ├── github_api.py
│   ├── repositories.py
│   │
│   ├── ingest_repositories.py
│   ├── ingest_commits.py
│   ├── ingest_issues.py
│   ├── ingest_pull_requests.py
│   │
│   ├── transform_repositories.py
│   ├── transform_commits.py
│   ├── transform_issues.py
│   ├── transform_pull_requests.py
│   │
│   ├── load_repositories.py
│   ├── load_commits.py
│   ├── load_issues.py
│   ├── load_pull_requests.py
│   │
│   ├── kafka_consumer.py
│   │
│   ├── spark_cdc_stream.py
│   ├── spark_current_state.py
│   ├── spark_analytics.py
│   ├── spark_final_analytics.py
│   │
│   └── data_quality.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── cdc/
│   └── spark/
│
├── docker-compose.cdc.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

Generated data under `data/` is excluded from Git using `.gitignore`.

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd github_de_project
```

### 2. Create a Python environment

```bash
python -m venv .venv
```

Activate it on Windows:

```powershell
.venv\Scripts\Activate.ps1
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure GitHub authentication

Create a `.env` file:

```text
GITHUB_TOKEN=your_github_token_here
```

Never commit the `.env` file.

### 5. Start the CDC infrastructure

```powershell
docker compose -f docker-compose.cdc.yml up -d
```

The stack contains:

```text
Kafka
Debezium Connect
pgAdmin
```

### 6. PostgreSQL

Create the project database:

```text
github_de
```

PostgreSQL must be configured for logical replication:

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

Restart PostgreSQL after changing the setting.

### 7. Run ingestion and transformation

Run the corresponding ingestion and transformation scripts from `src/`.

Load the transformed datasets into PostgreSQL using the provided loading scripts.

### 8. Start Debezium

Register the PostgreSQL connector with the Debezium Connect REST API.

The connector captures changes from:

```text
repositories
commits
issues
pull_requests
```

### 9. Start Spark CDC streaming

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 `
  src\spark_cdc_stream.py
```

### 10. Build current-state datasets

```powershell
spark-submit src\spark_current_state.py
```

### 11. Generate analytics

```powershell
spark-submit src\spark_analytics.py
```

Then generate the final repository-level dataset:

```powershell
spark-submit src\spark_final_analytics.py
```

### 12. Run data-quality checks

```powershell
spark-submit src\data_quality.py
```

Expected result:

```text
ALL DATA QUALITY CHECKS PASSED
```

## Key Learning Outcomes

This project demonstrates practical experience with:

* Batch data ingestion
* REST API ingestion
* Data transformation
* Relational data modeling
* PostgreSQL
* Logical replication
* Change Data Capture
* Debezium
* Apache Kafka
* Event streaming
* Spark Structured Streaming
* CDC state management
* Distributed data processing
* Parquet
* Data quality validation
* Docker-based infrastructure

## Limitations

This project is a local development and learning implementation rather than a production deployment.

The current dataset contains a limited number of GitHub records and is intended to demonstrate the architecture and data flow.

Pull-request ingestion currently uses the GitHub pull-request listing endpoint, so detailed metrics such as additions, deletions, changed files, and review comments are not populated in the current dataset.

## Future Improvements

Potential extensions include:

* Incremental GitHub API ingestion
* Larger historical datasets
* Detailed pull-request metrics
* Kafka topic partitioning experiments
* Schema Registry and Avro/Protobuf serialization
* Apache Iceberg or Delta Lake
* Airflow orchestration
* Cloud deployment
* Dashboarding with Power BI or Streamlit
* Monitoring and pipeline observability
* Automated CI/CD
* Containerized Spark execution
