from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit


spark = (
    SparkSession.builder
    .appName("GitHubDataQuality")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BASE_PATH = "data/spark/current_state"


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
# EXPECTED COUNTS
# ==========================================

expected_counts = {
    "repositories": 10,
    "commits": 1000,
    "issues": 185,
    "pull_requests": 1000
}


actual_counts = {
    "repositories": repositories.count(),
    "commits": commits.count(),
    "issues": issues.count(),
    "pull_requests": pull_requests.count()
}


print("\n========================================")
print("ROW COUNT CHECKS")
print("========================================")

all_count_checks_passed = True

for table, expected in expected_counts.items():

    actual = actual_counts[table]

    status = "PASS" if actual == expected else "FAIL"

    if actual != expected:
        all_count_checks_passed = False

    print(
        f"{table:15} "
        f"expected={expected:<5} "
        f"actual={actual:<5} "
        f"[{status}]"
    )


# ==========================================
# PRIMARY KEY DUPLICATE CHECK
# ==========================================

print("\n========================================")
print("DUPLICATE PRIMARY KEY CHECKS")
print("========================================")


def check_duplicates(df, column_name, table_name):

    duplicate_count = (
        df.groupBy(column_name)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    status = "PASS" if duplicate_count == 0 else "FAIL"

    print(
        f"{table_name:15} "
        f"duplicates={duplicate_count:<5} "
        f"[{status}]"
    )

    return duplicate_count == 0


duplicate_checks = [
    check_duplicates(
        repositories,
        "repository_id",
        "repositories"
    ),

    check_duplicates(
        commits,
        "commit_sha",
        "commits"
    ),

    check_duplicates(
        issues,
        "issue_id",
        "issues"
    ),

    check_duplicates(
        pull_requests,
        "pull_request_id",
        "pull_requests"
    )
]


# ==========================================
# NULL CHECKS
# ==========================================

print("\n========================================")
print("NULL PRIMARY KEY CHECKS")
print("========================================")


def check_nulls(df, column_name, table_name):

    null_count = (
        df.filter(col(column_name).isNull())
        .count()
    )

    status = "PASS" if null_count == 0 else "FAIL"

    print(
        f"{table_name:15} "
        f"nulls={null_count:<5} "
        f"[{status}]"
    )

    return null_count == 0


null_checks = [
    check_nulls(
        repositories,
        "repository_id",
        "repositories"
    ),

    check_nulls(
        commits,
        "commit_sha",
        "commits"
    ),

    check_nulls(
        issues,
        "issue_id",
        "issues"
    ),

    check_nulls(
        pull_requests,
        "pull_request_id",
        "pull_requests"
    )
]


# ==========================================
# REFERENTIAL INTEGRITY
# ==========================================

print("\n========================================")
print("REFERENTIAL INTEGRITY CHECKS")
print("========================================")


repository_ids = repositories.select(
    "repository_id"
).distinct()


def check_repository_reference(df, table_name):

    orphan_count = (
        df
        .select("repository_id")
        .distinct()
        .join(
            repository_ids,
            "repository_id",
            "left_anti"
        )
        .count()
    )

    status = "PASS" if orphan_count == 0 else "FAIL"

    print(
        f"{table_name:15} "
        f"orphan_repository_ids={orphan_count:<5} "
        f"[{status}]"
    )

    return orphan_count == 0


referential_checks = [
    check_repository_reference(
        commits,
        "commits"
    ),

    check_repository_reference(
        issues,
        "issues"
    ),

    check_repository_reference(
        pull_requests,
        "pull_requests"
    )
]


# ==========================================
# FINAL RESULT
# ==========================================

print("\n========================================")
print("DATA QUALITY RESULT")
print("========================================")


all_checks = (
    [all_count_checks_passed]
    + duplicate_checks
    + null_checks
    + referential_checks
)


if all(all_checks):
    print("ALL DATA QUALITY CHECKS PASSED")
else:
    print("DATA QUALITY CHECKS FAILED")


print("========================================")


spark.stop()