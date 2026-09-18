\# GitHub CDC Data Engineering Pipeline



An end-to-end Data Engineering pipeline that ingests GitHub repository data, stores it in PostgreSQL, captures database changes using Change Data Capture (CDC), streams those changes through Apache Kafka, processes them with Apache Spark Structured Streaming, and generates analytical datasets in Parquet format.



\## Architecture



```text

&#x20;                        GitHub REST API

&#x20;                              │

&#x20;                              ▼

&#x20;                      Python Ingestion

&#x20;                              │

&#x20;                              ▼

&#x20;                          Raw JSON

&#x20;                              │

&#x20;                              ▼

&#x20;                   Python Transformation

&#x20;                              │

&#x20;                              ▼

&#x20;                         PostgreSQL

&#x20;                              │

&#x20;                        PostgreSQL WAL

&#x20;                              │

&#x20;                              ▼

&#x20;                          Debezium

&#x20;                              │

&#x20;                              ▼

&#x20;                       Apache Kafka

&#x20;                        │          │

&#x20;                        │          └──────────────► Python CDC Consumer

&#x20;                        │

&#x20;                        ▼

&#x20;               Spark Structured Streaming

&#x20;                        │

&#x20;                        ▼

&#x20;                 Normalized CDC Data

&#x20;                        │

&#x20;                        ▼

&#x20;                CDC Current-State Layer

&#x20;                        │

&#x20;                        ▼

&#x20;                  Spark Analytics

&#x20;                        │

&#x20;                        ▼

&#x20;                       Parquet

```



\## Overview



This project demonstrates a complete batch + streaming data pipeline using publicly available GitHub data.



The pipeline:



1\. Extracts GitHub repository, commit, issue, and pull-request data using the GitHub REST API.

2\. Stores raw API responses as JSON.

3\. Transforms the API data into structured datasets.

4\. Loads the transformed data into PostgreSQL.

5\. Enables PostgreSQL logical replication.

6\. Uses Debezium to capture row-level database changes.

7\. Publishes CDC events to Apache Kafka.

8\. Processes Kafka events using Spark Structured Streaming.

9\. Normalizes Debezium CDC events into table-specific datasets.

10\. Builds current-state datasets by applying CDC semantics.

11\. Generates repository-level analytics using Apache Spark.

12\. Writes analytical datasets in Parquet format.

13\. Runs automated data-quality checks.



\## Technologies



\* Python

\* GitHub REST API

\* PostgreSQL

\* PostgreSQL Logical Replication

\* Debezium

\* Apache Kafka

\* Apache Spark

\* Spark Structured Streaming

\* PySpark

\* Docker / Docker Compose

\* Parquet

\* Git / GitHub



\## Data Model



The PostgreSQL database contains four primary tables.



\### Repositories



Stores repository metadata including:



\* Repository ID

\* Repository name

\* Owner

\* Description

\* Language

\* Stars

\* Forks

\* Watchers

\* Open issues

\* Repository timestamps

\* Topics

\* License



\### Commits



Stores:



\* Commit SHA

\* Repository ID

\* Author

\* Author email

\* Commit date

\* Commit message



\### Issues



Stores:



\* Issue ID

\* Issue number

\* Repository ID

\* Title

\* State

\* Author

\* Creation/update timestamps

\* Comments

\* Labels

\* URL



GitHub's Issues endpoint can also return pull requests, so pull-request records are filtered separately during ingestion.



\### Pull Requests



Stores:



\* Pull-request ID

\* Pull-request number

\* Repository ID

\* Title

\* State

\* Author

\* Creation/update timestamps

\* URL



\## CDC Pipeline



PostgreSQL is configured with:



```text

wal\_level = logical

```



Debezium connects to PostgreSQL using the `pgoutput` logical replication plugin.



The CDC flow is:



```text

PostgreSQL Transaction

&#x20;       │

&#x20;       ▼

PostgreSQL WAL

&#x20;       │

&#x20;       ▼

Logical Replication

&#x20;       │

&#x20;       ▼

Debezium PostgreSQL Connector

&#x20;       │

&#x20;       ▼

Kafka Topic

&#x20;       │

&#x20;       ▼

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



\## Spark Structured Streaming



Spark consumes the Kafka topics:



```text

github\_de.public.repositories

github\_de.public.commits

github\_de.public.issues

github\_de.public.pull\_requests

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

pull\_requests/

```



\## CDC Current-State Processing



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

data/spark/current\_state/

├── repositories/

├── commits/

├── issues/

└── pull\_requests/

```



\## Analytics



Spark generates repository-level analytical datasets including:



\### Repository Popularity



Repository metadata with:



\* Stars

\* Forks

\* Watchers

\* Open issues

\* Programming language



\### Language Distribution



Aggregates repositories by programming language and calculates:



\* Repository count

\* Average stars

\* Total stars



\### Commit Activity



Calculates commit counts by repository.



\### Issue Analytics



Calculates:



\* Total issues

\* Open issues

\* Closed issues

\* Average issue comments



\### Pull Request Analytics



Calculates:



\* Total pull requests

\* Open pull requests

\* Closed pull requests



\### Final Repository Analytics



The final analytical dataset joins repository metadata with development activity:



```text

Repository

&#x20;   │

&#x20;   ├── Stars

&#x20;   ├── Forks

&#x20;   ├── Watchers

&#x20;   ├── Language

&#x20;   │

&#x20;   ├── Commit Count

&#x20;   │

&#x20;   ├── Issue Count

&#x20;   ├── Open Issues

&#x20;   ├── Closed Issues

&#x20;   ├── Average Issue Comments

&#x20;   │

&#x20;   ├── Pull Request Count

&#x20;   ├── Open Pull Requests

&#x20;   └── Closed Pull Requests

```



The final dataset is written as Parquet.



\## Data Quality



Automated Spark data-quality checks validate:



\* Expected row counts

\* Duplicate primary keys

\* Null primary keys

\* Referential integrity



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



\## Project Dataset



The project currently processes:



\* 10 GitHub repositories

\* 1,000 commits

\* 185 issues

\* 1,000 pull requests



Repositories include projects such as:



\* facebook/react

\* tensorflow/tensorflow

\* microsoft/vscode

\* huggingface/transformers

\* langchain-ai/langchain

\* vercel/next.js

\* kubernetes/kubernetes

\* pytorch/pytorch

\* apache/spark

\* apache/kafka



\## Project Structure



```text

github\_de\_project/

│

├── src/

│   ├── github\_api.py

│   ├── repositories.py

│   │

│   ├── ingest\_repositories.py

│   ├── ingest\_commits.py

│   ├── ingest\_issues.py

│   ├── ingest\_pull\_requests.py

│   │

│   ├── transform\_repositories.py

│   ├── transform\_commits.py

│   ├── transform\_issues.py

│   ├── transform\_pull\_requests.py

│   │

│   ├── load\_repositories.py

│   ├── load\_commits.py

│   ├── load\_issues.py

│   ├── load\_pull\_requests.py

│   │

│   ├── kafka\_consumer.py

│   │

│   ├── spark\_cdc\_stream.py

│   ├── spark\_current\_state.py

│   ├── spark\_analytics.py

│   ├── spark\_final\_analytics.py

│   │

│   └── data\_quality.py

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



\## Setup



\### 1. Clone the repository



```bash

git clone <repository-url>

cd github\_de\_project

```



\### 2. Create a Python environment



```bash

python -m venv .venv

```



Activate it on Windows:



```powershell

.venv\\Scripts\\Activate.ps1

```



\### 3. Install Python dependencies



```bash

pip install -r requirements.txt

```



\### 4. Configure GitHub authentication



Create a `.env` file:



```text

GITHUB\_TOKEN=your\_github\_token\_here

```



Never commit the `.env` file.



\### 5. Start the CDC infrastructure



```powershell

docker compose -f docker-compose.cdc.yml up -d

```



The stack contains:



```text

Kafka

Debezium Connect

pgAdmin

```



\### 6. PostgreSQL



Create the project database:



```text

github\_de

```



PostgreSQL must be configured for logical replication:



```sql

ALTER SYSTEM SET wal\_level = 'logical';

```



Restart PostgreSQL after changing the setting.



\### 7. Run ingestion and transformation



Run the corresponding ingestion and transformation scripts from `src/`.



Load the transformed datasets into PostgreSQL using the provided loading scripts.



\### 8. Start Debezium



Register the PostgreSQL connector with the Debezium Connect REST API.



The connector captures changes from:



```text

repositories

commits

issues

pull\_requests

```



\### 9. Start Spark CDC streaming



```powershell

spark-submit `

&#x20; --packages org.apache.spark:spark-sql-kafka-0-10\_2.13:4.2.0 `

&#x20; src\\spark\_cdc\_stream.py

```



\### 10. Build current-state datasets



```powershell

spark-submit src\\spark\_current\_state.py

```



\### 11. Generate analytics



```powershell

spark-submit src\\spark\_analytics.py

```



Then generate the final repository-level dataset:



```powershell

spark-submit src\\spark\_final\_analytics.py

```



\### 12. Run data-quality checks



```powershell

spark-submit src\\data\_quality.py

```



Expected result:



```text

ALL DATA QUALITY CHECKS PASSED

```



\## Key Learning Outcomes



This project demonstrates practical experience with:



\* Batch data ingestion

\* REST API ingestion

\* Data transformation

\* Relational data modeling

\* PostgreSQL

\* Logical replication

\* Change Data Capture

\* Debezium

\* Apache Kafka

\* Event streaming

\* Spark Structured Streaming

\* CDC state management

\* Distributed data processing

\* Parquet

\* Data quality validation

\* Docker-based infrastructure



\## Limitations



This project is a local development and learning implementation rather than a production deployment.



The current dataset contains a limited number of GitHub records and is intended to demonstrate the architecture and data flow.



Pull-request ingestion currently uses the GitHub pull-request listing endpoint, so detailed metrics such as additions, deletions, changed files, and review comments are not populated in the current dataset.



\## Future Improvements



Potential extensions include:



\* Incremental GitHub API ingestion

\* Larger historical datasets

\* Detailed pull-request metrics

\* Kafka topic partitioning experiments

\* Schema Registry and Avro/Protobuf serialization

\* Apache Iceberg or Delta Lake

\* Airflow orchestration

\* Cloud deployment

\* Dashboarding with Power BI or Streamlit

\* Monitoring and pipeline observability

\* Automated CI/CD

\* Containerized Spark execution



