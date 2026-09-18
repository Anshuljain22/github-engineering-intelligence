from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum,
    desc,
    when
)

spark = (
    SparkSession.builder
    .appName("GitHubCDCAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE_PATH = "data/spark/processed"
OUTPUT_PATH = "data/spark/analytics"


# ==========================================
# READ PROCESSED DATA
# ==========================================

repositories = spark.read.json(
    f"{BASE_PATH}/repositories/*.json"
)

commits = spark.read.json(
    f"{BASE_PATH}/commits/*.json"
)

issues = spark.read.json(
    f"{BASE_PATH}/issues/*.json"
)

pull_requests = spark.read.json(
    f"{BASE_PATH}/pull_requests/*.json"
)


# ==========================================
# 1. REPOSITORY POPULARITY
# ==========================================

repository_popularity = (
    repositories
    .filter(col("operation") != "d")
    .select(
        "repository_id",
        "name",
        "full_name",
        "owner",
        "language",
        "stars",
        "forks",
        "open_issues",
        "watchers"
    )
    .orderBy(desc("stars"))
)

repository_popularity.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/repository_popularity"
)


# ==========================================
# 2. LANGUAGE DISTRIBUTION
# ==========================================

language_distribution = (
    repositories
    .filter(
        (col("operation") != "d") &
        col("language").isNotNull()
    )
    .groupBy("language")
    .agg(
        count("*").alias("repository_count"),
        avg("stars").alias("average_stars"),
        sum("stars").alias("total_stars")
    )
    .orderBy(desc("repository_count"))
)

language_distribution.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/language_distribution"
)


# ==========================================
# 3. COMMIT ACTIVITY
# ==========================================

commit_activity = (
    commits
    .filter(col("operation") != "d")
    .groupBy("repository_id")
    .agg(
        count("*").alias("commit_count")
    )
    .orderBy(desc("commit_count"))
)

commit_activity.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/commit_activity"
)


# ==========================================
# 4. ISSUE ANALYTICS
# ==========================================

issue_analytics = (
    issues
    .filter(col("operation") != "d")
    .groupBy("repository_id")
    .agg(
        count("*").alias("issue_count"),

        sum(
            when(col("state") == "open", 1)
            .otherwise(0)
        ).alias("open_issues"),

        sum(
            when(col("state") == "closed", 1)
            .otherwise(0)
        ).alias("closed_issues"),

        avg("comments").alias("average_comments")
    )
    .orderBy(desc("issue_count"))
)

issue_analytics.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/issue_analytics"
)


# ==========================================
# 5. PULL REQUEST ANALYTICS
# ==========================================

pull_request_analytics = (
    pull_requests
    .filter(col("operation") != "d")
    .groupBy("repository_id")
    .agg(
        count("*").alias("pull_request_count"),

        sum(
            when(col("state") == "open", 1)
            .otherwise(0)
        ).alias("open_pull_requests"),

        sum(
            when(col("state") == "closed", 1)
            .otherwise(0)
        ).alias("closed_pull_requests")
    )
    .orderBy(desc("pull_request_count"))
)

pull_request_analytics.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/pull_request_analytics"
)


# ==========================================
# 6. CDC OPERATION STATISTICS
# ==========================================

repository_cdc = (
    repositories
    .groupBy("operation")
    .agg(
        count("*").alias("event_count")
    )
    .withColumn(
        "table_name",
        when(col("operation").isNotNull(), "repositories")
    )
)

commit_cdc = (
    commits
    .groupBy("operation")
    .agg(
        count("*").alias("event_count")
    )
    .withColumn(
        "table_name",
        when(col("operation").isNotNull(), "commits")
    )
)

issue_cdc = (
    issues
    .groupBy("operation")
    .agg(
        count("*").alias("event_count")
    )
    .withColumn(
        "table_name",
        when(col("operation").isNotNull(), "issues")
    )
)

pull_request_cdc = (
    pull_requests
    .groupBy("operation")
    .agg(
        count("*").alias("event_count")
    )
    .withColumn(
        "table_name",
        when(col("operation").isNotNull(), "pull_requests")
    )
)

cdc_statistics = (
    repository_cdc
    .unionByName(commit_cdc)
    .unionByName(issue_cdc)
    .unionByName(pull_request_cdc)
    .select(
        "table_name",
        "operation",
        "event_count"
    )
    .orderBy("table_name", "operation")
)

cdc_statistics.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/cdc_statistics"
)


# ==========================================
# DISPLAY RESULTS
# ==========================================

print("\n========================================")
print("REPOSITORY POPULARITY")
print("========================================")

repository_popularity.show(10, truncate=False)


print("\n========================================")
print("LANGUAGE DISTRIBUTION")
print("========================================")

language_distribution.show(20, truncate=False)


print("\n========================================")
print("COMMIT ACTIVITY")
print("========================================")

commit_activity.show(10, truncate=False)


print("\n========================================")
print("ISSUE ANALYTICS")
print("========================================")

issue_analytics.show(10, truncate=False)


print("\n========================================")
print("PULL REQUEST ANALYTICS")
print("========================================")

pull_request_analytics.show(10, truncate=False)


print("\n========================================")
print("CDC STATISTICS")
print("========================================")

cdc_statistics.show(20, truncate=False)


print("\n========================================")
print("Analytics written to:")
print(OUTPUT_PATH)
print("========================================")

spark.stop()