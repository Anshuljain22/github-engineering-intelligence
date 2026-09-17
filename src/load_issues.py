import json
from pathlib import Path

import psycopg


PROCESSED_DATA_DIR = Path("data/processed/issues")

DB_CONFIG = {
    "dbname": "github_de",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


def load_issues():
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
                    issues = json.load(file)

                loaded = 0
                skipped = 0

                for issue in issues:

                    repository_id = repository_map.get(
                        issue["repository"]
                    )

                    if repository_id is None:
                        print(
                            f"Skipping issue {issue['issue_id']}: "
                            f"repository not found"
                        )
                        skipped += 1
                        continue

                    cur.execute(
                        """
                        INSERT INTO issues (
                            issue_id,
                            issue_number,
                            repository_id,
                            title,
                            state,
                            author,
                            created_at,
                            updated_at,
                            closed_at,
                            comments,
                            labels,
                            url
                        )
                        VALUES (
                            %(issue_id)s,
                            %(issue_number)s,
                            %(repository_id)s,
                            %(title)s,
                            %(state)s,
                            %(author)s,
                            %(created_at)s,
                            %(updated_at)s,
                            %(closed_at)s,
                            %(comments)s,
                            %(labels)s,
                            %(url)s
                        )
                        ON CONFLICT (issue_id)
                        DO UPDATE SET
                            issue_number = EXCLUDED.issue_number,
                            repository_id = EXCLUDED.repository_id,
                            title = EXCLUDED.title,
                            state = EXCLUDED.state,
                            author = EXCLUDED.author,
                            updated_at = EXCLUDED.updated_at,
                            closed_at = EXCLUDED.closed_at,
                            comments = EXCLUDED.comments,
                            labels = EXCLUDED.labels,
                            url = EXCLUDED.url
                        """,
                        {
                            **issue,
                            "repository_id": repository_id
                        }
                    )

                    loaded += 1

                print(
                    f"Loaded: {file_path.name} "
                    f"({loaded} issues, {skipped} skipped)"
                )

        conn.commit()


if __name__ == "__main__":
    load_issues()