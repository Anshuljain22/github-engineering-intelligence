import json
from pathlib import Path

from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw/issues")
PROCESSED_DATA_DIR = Path("data/processed/issues")


def transform_issue(issue, repository):
    return {
        "issue_id": issue.get("id"),
        "issue_number": issue.get("number"),
        "repository": repository,
        "title": issue.get("title"),
        "state": issue.get("state"),
        "author": (issue.get("user") or {}).get("login"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "comments": issue.get("comments"),
        "labels": [
            label.get("name")
            for label in issue.get("labels", [])
        ],
        "url": issue.get("html_url")
    }


def transform_all_issues():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    skipped_prs = 0

    for repo in REPOSITORIES:
        filename = repo.replace("/", "_") + ".json"
        input_path = RAW_DATA_DIR / filename
        output_path = PROCESSED_DATA_DIR / filename

        with open(input_path, "r", encoding="utf-8") as file:
            issues = json.load(file)

        transformed_issues = []

        for issue in issues:
            if "pull_request" in issue:
                skipped_prs += 1
                continue

            transformed_issues.append(
                transform_issue(issue, repo)
            )

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(transformed_issues, file, indent=2)

        total += len(transformed_issues)

        print(
            f"Transformed {len(transformed_issues)} issues → {output_path}"
        )

    print(f"\nTotal issues: {total}")
    print(f"Pull requests skipped: {skipped_prs}")


if __name__ == "__main__":
    transform_all_issues()