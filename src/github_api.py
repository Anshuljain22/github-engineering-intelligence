import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}


def get_repository(repo):
    url = f"https://api.github.com/repos/{repo}"

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    return response.json()


def get_paginated_data(url, params=None, max_pages=None):
    all_data = []
    page = 1

    if params is None:
        params = {}

    while True:
        if max_pages is not None and page > max_pages:
            break

        params["page"] = page

        response = requests.get(
            url,
            headers=HEADERS,
            params=params
        )

        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_data.extend(data)

        print(f"Fetched page {page}: {len(data)} records")

        page += 1

    return all_data

if __name__ == "__main__":
    repository = get_repository("pytorch/pytorch")

    print("Repository:", repository["full_name"])
    print("Stars:", repository["stargazers_count"])
    print("Forks:", repository["forks_count"])
    print("Language:", repository["language"])