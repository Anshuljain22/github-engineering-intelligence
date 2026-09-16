import json
from pathlib import Path

from github_api import get_paginated_data
from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw/pull_requests")


def ingest_pull_requests():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        print(f"\nFetching pull requests for {repo}...")

        url = f"https://api.github.com/repos/{repo}/pulls"

        params = {
            "per_page": 100,
            "state": "all",
            "sort": "updated",
            "direction": "desc"
        }

        pull_requests = get_paginated_data(
            url,
            params,
            max_pages=1
        )

        filename = repo.replace("/", "_") + ".json"
        output_path = RAW_DATA_DIR / filename

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(pull_requests, file, indent=2)

        print(
            f"Saved {len(pull_requests)} pull requests → {output_path}"
        )


if __name__ == "__main__":
    ingest_pull_requests()