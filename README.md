# Automated Unsplash Media Ingestion Pipeline (Kitten Collector)

## 📌 Project Overview
This project is an automated data engineering pipeline designed to interact with external REST APIs, orchestrate file-system storage workflows, and capture state snapshots. Using Apache Airflow, the pipeline extracts unstructured asset metadata (JSON) via curl, processes the responses using Python, dynamically downloads binary image files, and captures system metrics for monitoring purposes.

## 🏗️ Architecture & Tech Stack
The workflow combines shell scripting and standard Python utilities to deliver a decoupled extraction and transformation process, fully containerized using Docker:

```text
[Unsplash API]
       │ (Bash Operator: curl)
       ▼
[/tmp/images.json] ────► [Python Operator] ────► [/opt/airflow/images/] ────► [Local Machine]
 (Staging Schema)         (Parsing & Download)     (Docker Volume Sync)        (./images/ folder)
```

*   **Orchestration:** **Apache Airflow 2.7.0** configured in `standalone` mode to handle pipeline task dependencies.
*   **Containerization:** **Docker & Docker Compose** to spin up isolated environments with mapped storage layers.
*   **API Security:** **Airflow Variables** securely store and inject authorization tokens (`unsplash_api_key`) at runtime.
*   **Extraction Layer:** **BashOperator** executes shell-level HTTP requests using `curl` to pull asset payloads.
*   **Processing Layer:** **PythonOperator** uses standard libraries (`urllib`, `json`, `pathlib`) to parse data and download physical binary payloads.

## 🗂️ Repository Structure
```text
├── dags/
│   └── kitten_dags.py   # Airflow DAG definition and execution logic
├── images/                   # Local folder synced with Docker container to store downloads
├── docker-compose.yml        # Provided container configurations
└── README.md                 # Project documentation
```

## 🚀 How to Run Locally

### Prerequisites
*   [Docker Desktop](https://docker.com) installed and running.
*   An **Unsplash Developer API Key** (Free tier available at [Unsplash Developers](https://unsplash.com)).

### Step 1: Set Up Airflow Variables (Security)
To prevent hardcoding sensitive credentials in source code, this pipeline relies on Airflow's built-in key management:
1. Open the Airflow Web UI at **`http://localhost:8080`**.
2. Go to **Admin > Variables** in the top navigation bar.
3. Click the **+** (Add a new record) icon.
4. Set the **Key** as `unsplash_api_key` and paste your secret token into the **Val** field.
5. Click **Save**.

### Step 2: Spin Up the Infrastructure
Launch your isolated Airflow environment in detached background mode using your compose configuration:
```bash
docker compose up -d
```

### Step 3: Deploy and Unpause the DAG
1. Toggle the `kitten_collector` DAG from **Off** to **On**.
2. Click the **Play** button to trigger a manual execution run.

### Step 4: Verify Collected Assets
Because of the volume binding (`./images:/opt/airflow/images`) configured inside the `docker-compose.yml` file, any file written by the pipeline container is instantly synced back onto your host machine.
* Check your local project folder under **`./images`** to view your downloaded pictures.

## 📊 Key Data Engineering Principles Demonstrated
*   **Persistent Storage Volumes:** Handled container data ephemerality by establishing a Docker host bind mount, syncing critical pipeline assets out of isolated workers into local folders.
*   **Secure Credential Management:** Enforced production-grade security by using programmatic environment injection (`Variable.get`) rather than hardcoding secret API keys.
*   **Staging & Multi-Operator Orchestration:** Implemented a staging pattern where a low-overhead `BashOperator` handles network-heavy API data staging, while a dedicated `PythonOperator` handles compute-heavy parsing logic.
*   **Defensive Code Design:** Built deep logical error checking using structured `try/except` blocks alongside API structure evaluation (`isinstance(data, dict)`) to safely catch corrupt responses or token limits.




