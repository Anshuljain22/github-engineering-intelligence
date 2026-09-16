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


if __name__ == "__main__":
    repository = get_repository("pytorch/pytorch")

    print("Repository:", repository["full_name"])
    print("Stars:", repository["stargazers_count"])
    print("Forks:", repository["forks_count"])
    print("Language:", repository["language"])