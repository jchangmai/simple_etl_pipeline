import os
import glob
import json
import sys
import requests

# All sensitive values come from environment variables (GitHub Secrets)
DBT_CLOUD_URL = os.environ.get("DBT_CLOUD_HOST_URL", "https://cloud.getdbt.com")
API_TOKEN = os.environ.get("DBT_CLOUD_API_TOKEN")
ACCOUNT_ID = os.environ.get("DBT_CLOUD_ACCOUNT_ID")
PROJECT_ID = os.environ.get("DBT_CLOUD_PROJECT_ID")
ENVIRONMENT_ID = os.environ.get("DBT_CLOUD_ENVIRONMENT_ID")

if not API_TOKEN:
    print("Error: DBT_CLOUD_API_TOKEN is not set.", file=sys.stderr)
    sys.exit(1)

if not ACCOUNT_ID:
    print("Error: DBT_CLOUD_ACCOUNT_ID is not set.", file=sys.stderr)
    sys.exit(1)

if not PROJECT_ID:
    print("Error: DBT_CLOUD_PROJECT_ID is not set.", file=sys.stderr)
    sys.exit(1)

if not ENVIRONMENT_ID:
    print("Error: DBT_CLOUD_ENVIRONMENT_ID is not set.", file=sys.stderr)
    sys.exit(1)

headers = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json"
}

def get_existing_jobs():
    """Fetch all existing jobs for the account."""
    url = f"{DBT_CLOUD_URL}/api/v2/accounts/{ACCOUNT_ID}/jobs/"
    jobs = {}
    offset = 0
    limit = 100

    while True:
        resp = requests.get(url, headers=headers, params={"offset": offset, "limit": limit})
        if resp.status_code != 200:
            print(f"Failed to fetch jobs: {resp.status_code} - {resp.text}", file=sys.stderr)
            sys.exit(1)
        
        data = resp.json().get("data", [])
        for job in data:
            jobs[job["name"]] = job["id"]
        
        if len(data) < limit:
            break
        offset += limit

    return jobs

def create_or_check_job(filepath, existing_jobs):
    """Read a JSON job definition and create it if it does not exist."""
    print(f"\nProcessing: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        job_config = json.load(f)

    job_name = job_config.get("name")
    if not job_name:
        print(f"Warning: Skipping {filepath} because 'name' field is missing.")
        return

    # Inject sensitive values from environment variables
    job_config["account_id"] = int(ACCOUNT_ID)
    job_config["project_id"] = int(PROJECT_ID)
    job_config["environment_id"] = int(ENVIRONMENT_ID)

    if job_name in existing_jobs:
        existing_id = existing_jobs[job_name]
        print(f"Job '{job_name}' already exists (Job ID: {existing_id}). Skipping creation.")
        return

    # Create new job
    url = f"{DBT_CLOUD_URL}/api/v2/accounts/{ACCOUNT_ID}/jobs/"
    print(f"Creating job '{job_name}' in dbt Cloud...")
    resp = requests.post(url, headers=headers, json=job_config)

    if resp.status_code in [200, 201]:
        created_job = resp.json().get("data", {})
        job_id = created_job.get("id")
        print(f"Successfully created job '{job_name}'! New Job ID: {job_id}")
        existing_jobs[job_name] = job_id
    else:
        print(f"Failed to create job '{job_name}': {resp.status_code} - {resp.text}", file=sys.stderr)
        sys.exit(1)

def main():
    jobs_dir = os.path.join(os.path.dirname(__file__), "..", "dbtcloud_jobs")
    json_pattern = os.path.join(jobs_dir, "**", "*.json")
    json_files = glob.glob(json_pattern, recursive=True)

    if not json_files:
        print(f"No job JSON files found in {jobs_dir}")
        return

    print(f"Found {len(json_files)} job definition file(s).")
    existing_jobs = get_existing_jobs()
    print(f"Found {len(existing_jobs)} existing job(s) in dbt Cloud account {ACCOUNT_ID}.")

    for filepath in json_files:
        create_or_check_job(filepath, existing_jobs)

if __name__ == "__main__":
    main()
