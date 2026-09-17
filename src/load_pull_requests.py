import json
from pathlib import Path

import psycopg


PROCESSED_DATA_DIR = Path("data/processed/pull_requests")

DB_CONFIG = {
    "dbname": "github_de",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


def load_pull_requests():
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT repository_id, full_name
                FROM repositories
            """)

            repository_map = {
                full_name: repository_id
                for repository_id, full_name in cur.fetchall()
            }

            for file_path in PROCESSED_DATA_DIR.glob("*.json"):

                with open(file_path, "r", encoding="utf-8") as file:
                    pull_requests = json.load(file)

                loaded = 0
                skipped = 0

                for pr in pull_requests:

                    repository_id = repository_map.get(
                        pr["repository"]
                    )

                    if repository_id is None:
                        print(
                            f"Skipping PR {pr['pull_request_id']}: "
                            f"repository not found"
                        )
                        skipped += 1
                        continue

                    cur.execute(
                        """
                        INSERT INTO pull_requests (
                            pull_request_id,
                            number,
                            repository_id,
                            title,
                            state,
                            author,
                            created_at,
                            updated_at,
                            closed_at,
                            merged_at,
                            comments,
                            review_comments,
                            commits,
                            changed_files,
                            additions,
                            deletions,
                            url
                        )
                        VALUES (
                            %(pull_request_id)s,
                            %(number)s,
                            %(repository_id)s,
                            %(title)s,
                            %(state)s,
                            %(author)s,
                            %(created_at)s,
                            %(updated_at)s,
                            %(closed_at)s,
                            %(merged_at)s,
                            %(comments)s,
                            %(review_comments)s,
                            %(commits)s,
                            %(changed_files)s,
                            %(additions)s,
                            %(deletions)s,
                            %(url)s
                        )
                        ON CONFLICT (pull_request_id)
                        DO UPDATE SET
                            number = EXCLUDED.number,
                            repository_id = EXCLUDED.repository_id,
                            title = EXCLUDED.title,
                            state = EXCLUDED.state,
                            author = EXCLUDED.author,
                            updated_at = EXCLUDED.updated_at,
                            closed_at = EXCLUDED.closed_at,
                            merged_at = EXCLUDED.merged_at,
                            comments = EXCLUDED.comments,
                            review_comments = EXCLUDED.review_comments,
                            commits = EXCLUDED.commits,
                            changed_files = EXCLUDED.changed_files,
                            additions = EXCLUDED.additions,
                            deletions = EXCLUDED.deletions,
                            url = EXCLUDED.url
                        """,
                        {
                            **pr,
                            "repository_id": repository_id
                        }
                    )

                    loaded += 1

                print(
                    f"Loaded: {file_path.name} "
                    f"({loaded} pull requests, {skipped} skipped)"
                )

        conn.commit()


if __name__ == "__main__":
    load_pull_requests()