import json
from pathlib import Path

from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw/commits")
PROCESSED_DATA_DIR = Path("data/processed/commits")


def transform_commit(commit, repository):
    commit_data = commit.get("commit", {})
    author = commit_data.get("author") or {}
    github_author = commit.get("author") or {}

    return {
        "commit_sha": commit.get("sha"),
        "repository": repository,
        "author": github_author.get("login") or author.get("name"),
        "author_email": author.get("email"),
        "commit_date": author.get("date"),
        "message": commit_data.get("message")
    }


def transform_all_commits():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        filename = repo.replace("/", "_") + ".json"
        input_path = RAW_DATA_DIR / filename
        output_path = PROCESSED_DATA_DIR / filename

        with open(input_path, "r", encoding="utf-8") as file:
            commits = json.load(file)

        transformed_commits = [
            transform_commit(commit, repo)
            for commit in commits
        ]

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(transformed_commits, file, indent=2)

        print(f"Transformed {len(transformed_commits)} commits → {output_path}")


if __name__ == "__main__":
    transform_all_commits()