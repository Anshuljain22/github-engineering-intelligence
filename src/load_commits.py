import json
from pathlib import Path

import psycopg


PROCESSED_DATA_DIR = Path("data/processed/commits")

DB_CONFIG = {
    "dbname": "github_de",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


def load_commits():
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
                    commits = json.load(file)

                for commit in commits:

                    repository_id = repository_map.get(
                        commit["repository"]
                    )

                    if repository_id is None:
                        print(
                            f"Skipping commit {commit['commit_sha']}: "
                            f"repository not found"
                        )
                        continue

                    cur.execute(
                        """
                        INSERT INTO commits (
                            commit_sha,
                            repository_id,
                            author,
                            author_email,
                            commit_date,
                            message
                        )
                        VALUES (
                            %(commit_sha)s,
                            %(repository_id)s,
                            %(author)s,
                            %(author_email)s,
                            %(commit_date)s,
                            %(message)s
                        )
                        ON CONFLICT (commit_sha)
                        DO UPDATE SET
                            repository_id = EXCLUDED.repository_id,
                            author = EXCLUDED.author,
                            author_email = EXCLUDED.author_email,
                            commit_date = EXCLUDED.commit_date,
                            message = EXCLUDED.message
                        """,
                        {
                            **commit,
                            "repository_id": repository_id
                        }
                    )

                print(
                    f"Loaded: {file_path.name} "
                    f"({len(commits)} commits)"
                )

        conn.commit()


if __name__ == "__main__":
    load_commits()