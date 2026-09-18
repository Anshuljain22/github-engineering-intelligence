from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    when,
    coalesce,
    lit,
    round
)


spark = (
    SparkSession.builder
    .appName("GitHubFinalAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BASE_PATH = "data/spark/current_state"
OUTPUT_PATH = "data/spark/analytics_final"


# ==========================================
# READ CURRENT STATE
# ==========================================

repositories = spark.read.parquet(
    f"{BASE_PATH}/repositories"
)

commits = spark.read.parquet(
    f"{BASE_PATH}/commits"
)

issues = spark.read.parquet(
    f"{BASE_PATH}/issues"
)

pull_requests = spark.read.parquet(
    f"{BASE_PATH}/pull_requests"
)


# ==========================================
# COMMIT ANALYTICS
# ==========================================

commit_activity = (
    commits
    .groupBy("repository_id")
    .agg(
        count("*").alias("commit_count")
    )
)


# ==========================================
# ISSUE ANALYTICS
# ==========================================

issue_activity = (
    issues
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

        avg("comments").alias("average_issue_comments")
    )
)


# ==========================================
# PULL REQUEST ANALYTICS
# ==========================================

pr_activity = (
    pull_requests
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
)


# ==========================================
# FINAL REPOSITORY DATASET
# ==========================================

final_analytics = (
    repositories.alias("r")

    .join(
        commit_activity.alias("c"),
        col("r.repository_id") == col("c.repository_id"),
        "left"
    )

    .join(
        issue_activity.alias("i"),
        col("r.repository_id") == col("i.repository_id"),
        "left"
    )

    .join(
        pr_activity.alias("p"),
        col("r.repository_id") == col("p.repository_id"),
        "left"
    )

    .select(
        col("r.repository_id"),
        col("r.full_name"),
        col("r.owner"),
        col("r.language"),
        col("r.stars"),
        col("r.forks"),
        col("r.watchers"),
        col("r.open_issues").alias("github_open_issues"),

        coalesce(
            col("c.commit_count"),
            lit(0)
        ).alias("commit_count"),

        coalesce(
            col("i.issue_count"),
            lit(0)
        ).alias("issue_count"),

        coalesce(
            col("i.open_issues"),
            lit(0)
        ).alias("open_issues"),

        coalesce(
            col("i.closed_issues"),
            lit(0)
        ).alias("closed_issues"),

        round(
            coalesce(
                col("i.average_issue_comments"),
                lit(0.0)
            ),
            2
        ).alias("average_issue_comments"),

        coalesce(
            col("p.pull_request_count"),
            lit(0)
        ).alias("pull_request_count"),

        coalesce(
            col("p.open_pull_requests"),
            lit(0)
        ).alias("open_pull_requests"),

        coalesce(
            col("p.closed_pull_requests"),
            lit(0)
        ).alias("closed_pull_requests")
    )
)


# ==========================================
# WRITE FINAL DATASET
# ==========================================

final_analytics = final_analytics.orderBy(
    col("stars").desc()
)

final_analytics.write.mode("overwrite").parquet(
    OUTPUT_PATH
)


# ==========================================
# DISPLAY
# ==========================================

print("\n========================================")
print("FINAL REPOSITORY ANALYTICS")
print("========================================")

final_analytics.show(
    20,
    truncate=False
)


print("\n========================================")
print("FINAL ROW COUNT")
print("========================================")

print(
    "Rows:",
    final_analytics.count()
)


print("\n========================================")
print("FINAL ANALYTICS WRITTEN TO")
print("========================================")

print(OUTPUT_PATH)


spark.stop()