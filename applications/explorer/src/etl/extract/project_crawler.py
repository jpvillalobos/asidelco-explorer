import pandas as pd
import requests
import random
import os
import time
from bs4 import BeautifulSoup
from time import sleep, strftime
from datetime import datetime
from collections import defaultdict

# Input/Output Paths
input_excel = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/input/2024-errors2.xlsx"
output_dir = "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/data/output/projects/html"
error_log_path = os.path.join(output_dir, "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/projects_crawl_errors-2.log")
crawl_log_path = os.path.join(output_dir, "/home/jpvillalobos/cloudpipelines.it/projects/cfia-explorer/projects_crawl_journey-2.log")

# Setup
os.makedirs(output_dir, exist_ok=True)
df = pd.read_excel(input_excel)
project_ids = df["Proyecto"].dropna().tolist()
session = requests.Session()
url = "https://servicios.cfia.or.cr/ConsultaProyecto/"

# Fetch page for form state
response = session.get(url)
soup = BeautifulSoup(response.text, "html.parser")
viewstate = soup.select_one("#__VIEWSTATE")["value"]
eventvalidation = soup.select_one("#__EVENTVALIDATION")["value"]
viewstategen = soup.select_one("#__VIEWSTATEGENERATOR")["value"]

# Headers
headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,es;q=0.8,es-ES;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://servicios.cfia.or.cr",
    "referer": url,
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "x-microsoftajax": "Delta=true",
    "x-requested-with": "XMLHttpRequest"
}

# Track counters
seen_ids = defaultdict(int)
responses_dict = {}
total_projects = len(project_ids)

# Retry setup
MAX_RETRIES = 3
retry_counts = defaultdict(int)

# Logging function
def log_message(message):
    timestamp = strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(crawl_log_path, "a") as log_file:
        log_file.write(log_entry + "\n")

start_time = time.time()  # Start the timer

# Crawling loop
for index, pid in enumerate(project_ids, start=1):
    try:
        pid = str(int(pid))
    except ValueError:
        log_message(f"Invalid project ID format: {pid}")
        continue

    seen_ids[pid] += 1
    suffix = f"_{seen_ids[pid]}" if seen_ids[pid] > 1 else ""
    filename = os.path.join(output_dir, f"{pid}{suffix}.html")

    # Calculate elapsed time
    elapsed = int(time.time() - start_time)
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{hours:02}:{minutes:02}:{seconds:02}"

    log_message(f"Processing ({index}/{total_projects}) - Project ID: {pid} - Elapsed time: {time_str}")

    payload = {
        "ScriptManager1": "UpdatePanel1|btnConsultar",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstategen,
        "__EVENTVALIDATION": eventvalidation,
        "identificadores": "radioNumProyecto",
        "txtValor": pid,
        "__ASYNCPOST": "true",
        "btnConsultar": "Consultar"
    }

    attempt = 0
    success = False
    while attempt < MAX_RETRIES and not success:
        try:
            result = session.post(url, headers=headers, data=payload)
            result.raise_for_status()

            html_response = result.text
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html_response)

            responses_dict[pid] = html_response
            log_message(f"Saved response to: {filename}")
            success = True

        except Exception as e:
            attempt += 1
            retry_counts[pid] += 1
            log_message(f"Attempt {attempt} failed for {pid}: {e}")
            if attempt == MAX_RETRIES:
                with open(error_log_path, "a") as error_file:
                    error_file.write(f"{strftime('%Y-%m-%d %H:%M:%S')} - Failed to retrieve project {pid} after {MAX_RETRIES} attempts.\n")
                log_message(f"Abandoning project {pid} after {MAX_RETRIES} attempts.")

    # Randomized short delay
    delay = random.uniform(0.1, 0.5)
    print(f"Waiting {delay:.2f}s before next request...")
    sleep(delay)

# Final summary
total_elapsed = int(time.time() - start_time)
h, rem = divmod(total_elapsed, 3600)
m, s = divmod(rem, 60)
print(f"\nAll queries completed in {h}h {m}m {s}s.")
log_message("All project queries completed.")
log_message(f"Error log written to: {error_log_path}")
