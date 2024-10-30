import requests
import pandas as pd
import time

# GitHub API URL
BASE_URL = "https://api.github.com"
# Your GitHub personal access token
TOKEN = "******"

# Headers for the requests
headers = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_users_in_berlin(min_followers=200, location="Berlin"):
    users = []
    page = 1

    while True:
        # Use the search endpoint to get a list of users matching the criteria
        query = f"location:{location} followers:>{min_followers}"
        url = f"{BASE_URL}/search/users?q={query}&per_page=100&page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print("Error fetching users:", response.json())
            break

        # Extract user data from the search results
        data = response.json().get("items", [])
        if not data:
            break
        
        # For each user in search results, retrieve full profile using /users/{username} endpoint
        for user in data:
            user_details_url = f"{BASE_URL}/users/{user['login']}"
            user_response = requests.get(user_details_url, headers=headers)
            
            if user_response.status_code == 200:
                users.append(user_response.json())
            else:
                print(f"Error fetching details for user {user['login']}")

        page += 1
        time.sleep(1)  # Avoid hitting rate limits

    return users


def get_user_repos(username):
    repos = []
    page = 1

    while len(repos) < 500:
        url = f"{BASE_URL}/users/{username}/repos?per_page=100&page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error fetching repos for {username}: {response.json()}")
            break
        
        data = response.json()
        if not data:
            break

        repos.extend(data)
        page += 1
        time.sleep(1)
        
    return repos[:500]  # Limit to 500 repos if there are more


def clean_company_name(company_name):
    if not company_name:
        return ""
    return company_name.strip().lstrip("@").upper()

def create_users_csv(users):
    user_data = []
    for user in users:
        # Use .get() to handle missing values safely
        user_data.append([
            user.get("login"),
            user.get("name"),
            clean_company_name(user.get("company")),
            user.get("location"),
            user.get("email"),
            user.get("hireable"),
            user.get("bio"),
            user.get("public_repos"),
            user.get("followers"),
            user.get("following"),
            user.get("created_at")
        ])
    
    # Write to users.csv
    users_df = pd.DataFrame(user_data, columns=[
        "login", "name", "company", "location", "email", "hireable",
        "bio", "public_repos", "followers", "following", "created_at"
    ])
    users_df.to_csv("users.csv", index=False)
    print("users.csv created with full details")


def create_repos_csv(repos):
    repo_data = []
    for repo in repos:
        repo_data.append([
            repo["owner"]["login"],
            repo["full_name"],
            repo["created_at"],
            repo["stargazers_count"],
            repo["watchers_count"],
            repo["language"],
            repo["has_projects"],
            repo["has_wiki"],
            repo.get("license", {}).get("key", "")
        ])
    
    repos_df = pd.DataFrame(repo_data, columns=[
        "login", "full_name", "created_at", "stargazers_count", "watchers_count",
        "language", "has_projects", "has_wiki", "license_name"
    ])
    repos_df.to_csv("repositories.csv", index=False)

if __name__ == "__main__":
    # Fetch detailed user profiles
    users = get_users_in_berlin()
    create_users_csv(users)

    # Fetch repositories for each user
    all_repos = []
    for user in users:
        username = user["login"]
        repos = get_user_repos(username)
        all_repos.extend(repos)
    
    create_repos_csv(all_repos)
    print("CSV files created successfully!")
