from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    get_json_object,
    coalesce,
    when,
    lit
)

KAFKA_BOOTSTRAP = "localhost:29092"

TOPICS = [
    "github_de.public.repositories",
    "github_de.public.commits",
    "github_de.public.issues",
    "github_de.public.pull_requests",
]

OUTPUT_PATH = "data/spark/cdc_events"
CHECKPOINT_PATH = "data/spark/checkpoint"

spark = (
    SparkSession.builder
    .appName("GitHubCDCStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(TOPICS))
    .option("startingOffsets", "earliest")
    .load()
)

events = raw_stream.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("value")
)

parsed = events.select(
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",

    coalesce(
        get_json_object(col("value"), "$.op"),
        get_json_object(col("value"), "$.payload.op")
    ).alias("operation"),

    coalesce(
        get_json_object(col("value"), "$.ts_ms"),
        get_json_object(col("value"), "$.payload.ts_ms")
    ).alias("event_timestamp"),

    coalesce(
        get_json_object(col("value"), "$.before"),
        get_json_object(col("value"), "$.payload.before")
    ).alias("before"),

    coalesce(
        get_json_object(col("value"), "$.after"),
        get_json_object(col("value"), "$.payload.after")
    ).alias("after")
)

# -----------------------------
# Repository CDC
# -----------------------------

repositories = parsed.filter(
    col("topic") == "github_de.public.repositories"
).select(
    lit("repositories").alias("table_name"),
    "operation",
    "event_timestamp",
    "kafka_timestamp",
    "partition",
    "offset",

    get_json_object(col("after"), "$.repository_id").cast("long").alias("repository_id"),
    get_json_object(col("after"), "$.name").alias("name"),
    get_json_object(col("after"), "$.full_name").alias("full_name"),
    get_json_object(col("after"), "$.owner").alias("owner"),
    get_json_object(col("after"), "$.description").alias("description"),
    get_json_object(col("after"), "$.language").alias("language"),
    get_json_object(col("after"), "$.stars").cast("int").alias("stars"),
    get_json_object(col("after"), "$.forks").cast("int").alias("forks"),
    get_json_object(col("after"), "$.open_issues").cast("int").alias("open_issues"),
    get_json_object(col("after"), "$.watchers").cast("int").alias("watchers"),
    get_json_object(col("after"), "$.archived").cast("boolean").alias("archived")
)

# -----------------------------
# Commit CDC
# -----------------------------

commits = parsed.filter(
    col("topic") == "github_de.public.commits"
).select(
    lit("commits").alias("table_name"),
    "operation",
    "event_timestamp",
    "kafka_timestamp",
    "partition",
    "offset",

    get_json_object(col("after"), "$.commit_sha").alias("commit_sha"),
    get_json_object(col("after"), "$.repository_id").cast("long").alias("repository_id"),
    get_json_object(col("after"), "$.author").alias("author"),
    get_json_object(col("after"), "$.author_email").alias("author_email"),
    get_json_object(col("after"), "$.commit_date").alias("commit_date"),
    get_json_object(col("after"), "$.message").alias("message")
)

# -----------------------------
# Issue CDC
# -----------------------------

issues = parsed.filter(
    col("topic") == "github_de.public.issues"
).select(
    lit("issues").alias("table_name"),
    "operation",
    "event_timestamp",
    "kafka_timestamp",
    "partition",
    "offset",

    get_json_object(col("after"), "$.issue_id").cast("long").alias("issue_id"),
    get_json_object(col("after"), "$.issue_number").cast("int").alias("issue_number"),
    get_json_object(col("after"), "$.repository_id").cast("long").alias("repository_id"),
    get_json_object(col("after"), "$.title").alias("title"),
    get_json_object(col("after"), "$.state").alias("state"),
    get_json_object(col("after"), "$.author").alias("author"),
    get_json_object(col("after"), "$.comments").cast("int").alias("comments"),
    get_json_object(col("after"), "$.url").alias("url")
)

# -----------------------------
# Pull Request CDC
# -----------------------------

pull_requests = parsed.filter(
    col("topic") == "github_de.public.pull_requests"
).select(
    lit("pull_requests").alias("table_name"),
    "operation",
    "event_timestamp",
    "kafka_timestamp",
    "partition",
    "offset",

    get_json_object(col("after"), "$.pull_request_id").cast("long").alias("pull_request_id"),
    get_json_object(col("after"), "$.number").cast("int").alias("number"),
    get_json_object(col("after"), "$.repository_id").cast("long").alias("repository_id"),
    get_json_object(col("after"), "$.title").alias("title"),
    get_json_object(col("after"), "$.state").alias("state"),
    get_json_object(col("after"), "$.author").alias("author"),
    get_json_object(col("after"), "$.comments").cast("int").alias("comments"),
    get_json_object(col("after"), "$.review_comments").cast("int").alias("review_comments"),
    get_json_object(col("after"), "$.commits").cast("int").alias("commits"),
    get_json_object(col("after"), "$.changed_files").cast("int").alias("changed_files"),
    get_json_object(col("after"), "$.additions").cast("int").alias("additions"),
    get_json_object(col("after"), "$.deletions").cast("int").alias("deletions"),
    get_json_object(col("after"), "$.url").alias("url")
)

# -----------------------------
# Write each table separately
# -----------------------------

def start_query(dataframe, table_name):
    return (
        dataframe.writeStream
        .format("json")
        .option("path", f"data/spark/processed/{table_name}")
        .option("checkpointLocation", f"data/spark/checkpoint/{table_name}")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )


queries = [
    start_query(repositories, "repositories"),
    start_query(commits, "commits"),
    start_query(issues, "issues"),
    start_query(pull_requests, "pull_requests")
]

print("Spark CDC streaming started...")
print("Kafka:", KAFKA_BOOTSTRAP)
print("Processing tables:")
print("  - repositories")
print("  - commits")
print("  - issues")
print("  - pull_requests")

for query in queries:
    query.awaitTermination()