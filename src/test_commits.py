from github_api import get_paginated_data


url = "https://api.github.com/repos/pytorch/pytorch/commits"

params = {
    "per_page": 10
}

commits = get_paginated_data(
    url,
    params,
    max_pages=3
)

print("Total commits fetched:", len(commits))