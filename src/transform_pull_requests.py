import json
from pathlib import Path

from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw/pull_requests")
PROCESSED_DATA_DIR = Path("data/processed/pull_requests")


def transform_pull_request(pr, repository):
    user = pr.get("user") or {}

    return {
        "pull_request_id": pr.get("id"),
        "number": pr.get("number"),
        "repository": repository,
        "title": pr.get("title"),
        "state": pr.get("state"),
        "author": user.get("login"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "closed_at": pr.get("closed_at"),
        "merged_at": pr.get("merged_at"),
        "comments": pr.get("comments"),
        "review_comments": pr.get("review_comments"),
        "commits": pr.get("commits"),
        "changed_files": pr.get("changed_files"),
        "additions": pr.get("additions"),
        "deletions": pr.get("deletions"),
        "url": pr.get("html_url")
    }


def transform_all_pull_requests():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    total = 0

    for repo in REPOSITORIES:
        filename = repo.replace("/", "_") + ".json"

        input_path = RAW_DATA_DIR / filename
        output_path = PROCESSED_DATA_DIR / filename

        with open(input_path, "r", encoding="utf-8") as file:
            pull_requests = json.load(file)

        transformed_prs = [
            transform_pull_request(pr, repo)
            for pr in pull_requests
        ]

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(transformed_prs, file, indent=2)

        total += len(transformed_prs)

        print(
            f"Transformed {len(transformed_prs)} PRs → {output_path}"
        )

    print(f"\nTotal pull requests: {total}")


if __name__ == "__main__":
    transform_all_pull_requests()