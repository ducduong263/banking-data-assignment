# banking-data-assignment

## Project Overview

This project implements a simplified, secure, and compliant data platform for a banking system. The primary goal is to demonstrate proficiency in data modeling, data generation, implementing data quality checks, and orchestrating these processes, all while adhering to the principles outlined in regulatory requirements like Vietnam's 2345/QĐ-NHNN 2023.

The platform consists of:
1.  **PostgreSQL Database**: A relational database to store banking data, including customer information, accounts, transactions, and authentication logs.
2.  **Data Generation Script**: A Python script (`generate_data.py`) that populates the database with realistic, synthetic data, including edge cases for risk analysis.
3.  **Data Quality & Risk Monitoring**: A suite of Python scripts (`data_quality_standards.py`, `monitoring_audit.py`) that perform various checks for data integrity, format validity, and compliance with predefined risk rules.
4.  **Workflow Orchestration**: An Apache Airflow DAG (`banking_dq_dag.py`) that automates the data generation and quality audit processes on a daily schedule.
5.  **Containerized Environment**: The entire stack (PostgreSQL, Airflow) is managed using Docker and Docker Compose for easy setup and consistent execution.

---

## Setup Instructions

### Prerequisites
* Python 3.12
* Docker

### Environment Configuration

The entire environment is containerized, simplifying the setup process.

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/ducduong263/banking-data-assignment
    cd banking-data-assignment
    ```

2.  **Build and Run the Services**:
    Use Docker Compose to build the images and start all the services (PostgreSQL databases for the application and Airflow, Airflow webserver, scheduler, and initialization service).

    ```bash
    docker-compose up -d
    ```
---

## How to Run the DAG

The project uses **Apache Airflow** to schedule and monitor the data quality pipeline.

1.  **Access the Airflow UI**:
    Once the Docker containers are running, open your web browser and navigate to:
    [**http://localhost:8080**](http://localhost:8080)

2.  **Login**:
    Use the default credentials created by the `airflow-init` service:
    * **Username**: `airflow`
    * **Password**: `airflow`

3.  **Find and Run the DAG**:
    * On the Airflow dashboard, you will see a DAG named `banking_data_quality_dag`.
    * By default, this DAG is scheduled to run daily at `00:00`. It is paused by default upon creation.
    * To enable the schedule, click the toggle switch on the left side of the DAG name.
    * To run the DAG manually, click the "Play" button (▶️) on the right side.

4.  **DAG Tasks**:
    The `banking_data_quality_dag` consists of two main tasks:
    * `run_data_generation`: Executes the `src/generate_data.py` script to clear and repopulate the `banking_postgres_db` with fresh, synthetic data.
    * `run_data_quality_checks`: Executes the `src/monitoring_audit.py` script to run all data quality and risk checks against the newly generated data.

5.  **Check Logs**:
    * The results of the data quality audit are logged to the console of the `banking_airflow_scheduler` container. You can view this using `docker logs banking_airflow_scheduler`.
    * A detailed log file is also generated inside the `/logs` directory for each run (e.g., `logs/audit_log_YYYYMMDD_HHMMSS.txt`).

---
### Accessing the Database
You can connect to the banking database to view the sample data using any SQL client tool like TablePlus, DBeaver, or pgAdmin.

Use the following connection details:
* **Host**: `localhost`
* **Port**: `5432`
* **User**: `db_user`
* **Password**: `db_password`
* **Database Name**: `banking_db`
