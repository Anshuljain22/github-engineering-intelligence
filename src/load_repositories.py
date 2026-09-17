import json
from pathlib import Path

import psycopg


PROCESSED_DATA_DIR = Path("data/processed")

DB_CONFIG = {
    "dbname": "github_de",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


def load_repositories():
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            for file_path in PROCESSED_DATA_DIR.glob("*.json"):
                with open(file_path, "r", encoding="utf-8") as file:
                    repo = json.load(file)

                cur.execute(
                    """
                    INSERT INTO repositories (
                        repository_id,
                        name,
                        full_name,
                        owner,
                        description,
                        html_url,
                        created_at,
                        updated_at,
                        pushed_at,
                        language,
                        stars,
                        forks,
                        open_issues,
                        watchers,
                        size,
                        archived,
                        has_issues,
                        has_discussions,
                        default_branch,
                        topics,
                        license
                    )
                    VALUES (
                        %(repository_id)s,
                        %(name)s,
                        %(full_name)s,
                        %(owner)s,
                        %(description)s,
                        %(html_url)s,
                        %(created_at)s,
                        %(updated_at)s,
                        %(pushed_at)s,
                        %(language)s,
                        %(stars)s,
                        %(forks)s,
                        %(open_issues)s,
                        %(watchers)s,
                        %(size)s,
                        %(archived)s,
                        %(has_issues)s,
                        %(has_discussions)s,
                        %(default_branch)s,
                        %(topics)s,
                        %(license)s
                    )
                    ON CONFLICT (repository_id)
                    DO UPDATE SET
                        stars = EXCLUDED.stars,
                        forks = EXCLUDED.forks,
                        open_issues = EXCLUDED.open_issues,
                        watchers = EXCLUDED.watchers,
                        updated_at = EXCLUDED.updated_at,
                        pushed_at = EXCLUDED.pushed_at
                    """,
                    repo
                )

                print(f"Loaded: {repo['full_name']}")

        conn.commit()


if __name__ == "__main__":
    load_repositories()