import json
from pathlib import Path

from github_api import get_paginated_data
from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw/commits")


def ingest_commits():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        print(f"\nFetching commits for {repo}...")

        url = f"https://api.github.com/repos/{repo}/commits"

        params = {
            "per_page": 100
        }

        commits = get_paginated_data(
            url,
            params,
            max_pages=1
        )

        filename = repo.replace("/", "_") + ".json"
        output_path = RAW_DATA_DIR / filename

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(commits, file, indent=2)

        print(f"Saved {len(commits)} commits → {output_path}")


if __name__ == "__main__":
    ingest_commits()