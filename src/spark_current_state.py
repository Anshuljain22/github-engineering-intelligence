from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    to_timestamp
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("GitHubCDCCurrentState")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BASE_PATH = "data/spark/processed"
OUTPUT_PATH = "data/spark/current_state"


def get_current_state(path, primary_key):
    df = spark.read.json(f"{BASE_PATH}/{path}/*.json")

    df = df.withColumn(
        "event_timestamp_long",
        col("event_timestamp").cast("long")
    )

    window = Window.partitionBy(primary_key).orderBy(
        col("event_timestamp_long").desc(),
        col("kafka_timestamp").desc(),
        col("partition").desc(),
        col("offset").desc()
    )

    latest = (
        df
        .withColumn("row_number", row_number().over(window))
        .filter(col("row_number") == 1)
        .drop("row_number", "event_timestamp_long")
    )

    return latest.filter(col("operation") != "d")


repositories = get_current_state(
    "repositories",
    "repository_id"
)

commits = get_current_state(
    "commits",
    "commit_sha"
)

issues = get_current_state(
    "issues",
    "issue_id"
)

pull_requests = get_current_state(
    "pull_requests",
    "pull_request_id"
)


repositories.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/repositories"
)

commits.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/commits"
)

issues.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/issues"
)

pull_requests.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/pull_requests"
)


print("\n========================================")
print("CURRENT STATE COUNTS")
print("========================================")

print("Repositories:", repositories.count())
print("Commits:", commits.count())
print("Issues:", issues.count())
print("Pull Requests:", pull_requests.count())


print("\n========================================")
print("CURRENT REPOSITORIES")
print("========================================")

repositories.select(
    "repository_id",
    "full_name",
    "language",
    "stars",
    "forks"
).orderBy(
    col("stars").desc()
).show(20, truncate=False)


print("\n========================================")
print("CURRENT STATE WRITTEN TO")
print("========================================")

print(OUTPUT_PATH)

spark.stop()