# 🐱 Automated Kitten Data Pipeline (Apache Airflow & Docker)

A production-grade data engineering pipeline orchestrated by **Apache Airflow** running within a isolated **Docker** container ecosystem. This workflow interacts with the Unsplash API to dynamically retrieve, parse, and store media assets while maintaining isolated health and storage state logs.

---

## 🏗️ Pipeline Architecture & DAG Topology

The DAG (`kitten_collector`) is scheduled to execute on a daily interval (`@daily`). It utilizes an optimized task execution flow that splits linear extraction steps into parallel reporting tracks upon successful media downloads.

```text
                  [ pipeline_notification ]  (Console Logs)
                /
[ fetch_images ] -> [ save_images ]
                \
                  [ write_results_to_file ]  (Persistent Storage Snapshot)

**Task Definitions**:
1. fetch_images (BashOperator): Issues a secure curl request to the Unsplash API utilizing an decoupled environment variable wrapper. It downloads fresh metadata configuration files into an internal container staging area (/tmp/images.json).

2. save_images (PythonOperator): Invokes an internal Python worker function that parses the raw API JSON structure, handles response object formatting, validates keys, and down-streams image payloads to target paths using urllib.request.

3. pipeline_notification (BashOperator): A lightweight downstream confirmation layer executing silent terminal strings upon workflow completion.

4. write_results_to_file (BashOperator): Executes in parallel to the notification track. It creates a robust administrative snapshot of the image storage directory, records file sizes/permissions, generates a system timestamp, and appends the structural footprint to a flat log file (/tmp/airflow/kitten_state.txt).

**🛠️ Environment Initialization & Deployment**
_Prerequisites_
Docker Desktop installed and actively running.

An active Unsplash Developer Account API Access Token.

_1. Initialize Containers_
Deploy the multi-service orchestration footprint from your terminal root folder:

Bash
docker compose up -d

2. Configure Airflow Administrative Access
Since the internal metadata engine is lightweight and isolated, spin up your administrative user via the direct Docker execution CLI:

Bash
docker exec -it kitten_pipeline-airflow-1 airflow users create \
    --username admin \
    --firstname Control \
    --lastname Center \
    --role Admin \
    --email admin@example.com \
    --password admin

3. Inject Runtime Variables Securely
To protect access hashes from version tracking exposures, tokens are handled via the web application state tier:

Open your browser and navigate to http://localhost:8080.

Authenticate using your created administrative credentials.

Access Admin -> Variables from the primary application navbar.

Click Add a new record (+) and configure the tracking properties precisely:

Key: unsplash_api_key

Val: your_actual_unsplash_developer_secret_token_here

📁 Repository Structure
Plaintext
KITTEN_PIPELINE/
├── dags/
│   └── kitten_dags.py       # Core Python DAG workflow & pipeline configuration
├── docker-compose.yaml      # Multi-service Docker infrastructure declaration
├── .gitignore               # Excludes binary JPG image clutter from Git cache
└── README.md                # System deployment documentation (This file)

🛡️ Security & Fault Tolerance
Token Decoupling: API authorization keys are stored dynamically inside the relational metadata storage framework and are called at runtime rather than hard-coded into the repository files.

Storage Safety: The local .gitignore is pre-configured to suppress media assets (*.jpg / images/), ensuring testing payloads are never pushed to public repositories.
