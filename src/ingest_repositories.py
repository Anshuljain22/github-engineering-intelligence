import json
from pathlib import Path

from github_api import get_repository
from repositories import REPOSITORIES


RAW_DATA_DIR = Path("data/raw")


def ingest_repositories():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for repo in REPOSITORIES:
        print(f"Fetching {repo}...")

        data = get_repository(repo)

        filename = repo.replace("/", "_") + ".json"
        output_path = RAW_DATA_DIR / filename

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)

        print(f"Saved: {output_path}")


if __name__ == "__main__":
    ingest_repositories()