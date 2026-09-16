import json
from pathlib import Path


RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")


def transform_repository(data):
    owner = data.get("owner") or {}
    license_data = data.get("license") or {}

    return {
        "repository_id": data.get("id"),
        "name": data.get("name"),
        "full_name": data.get("full_name"),
        "owner": owner.get("login"),
        "description": data.get("description"),
        "html_url": data.get("html_url"),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "pushed_at": data.get("pushed_at"),
        "language": data.get("language"),
        "stars": data.get("stargazers_count"),
        "forks": data.get("forks_count"),
        "open_issues": data.get("open_issues_count"),
        "watchers": data.get("watchers_count"),
        "size": data.get("size"),
        "archived": data.get("archived"),
        "has_issues": data.get("has_issues"),
        "has_discussions": data.get("has_discussions"),
        "default_branch": data.get("default_branch"),
        "topics": data.get("topics", []),
        "license": license_data.get("spdx_id")
    }


def transform_all_repositories():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for file_path in RAW_DATA_DIR.glob("*.json"):
        with open(file_path, "r", encoding="utf-8") as file:
            raw_data = json.load(file)

        transformed_data = transform_repository(raw_data)

        output_path = PROCESSED_DATA_DIR / file_path.name

        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(transformed_data, file, indent=2)

        print(f"Transformed: {file_path.name}")


if __name__ == "__main__":
    transform_all_repositories()