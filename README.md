# Healthcare Analytics Lab: OLTP to Star Schema

## Overview
This project demonstrates the transformation of a normalized **OLTP (Online Transaction Processing)** healthcare database into an optimized **Star Schema (Data Warehouse)** for analytical queries.

It covers the full data engineering lifecycle:
1.  **Setting up** a normalized database (MySQL).
2.  **Analyzing performance** of complex business queries (Joins, Self-Joins).
3.  **Designing** a dimensional model (Star Schema).
4.  **Implementing ETL** (Extract, Transform, Load) pipelines in Python.
5.  **Verifying** performance improvements.

## Project Structure

### 1. Setup & OLTP (Source System)
*   `oltp_setup.sql`: DDL and sample data for the normalized tables (patients, encounters, billing, etc.).
*   `setup_db.py`: Python script to create the database and load the OLTP data.

### 2. Analysis (The Problem)
*   `analyze_queries.py`: Runs 4 business questions against the OLTP schema and measures execution time.
*   `run_explain.py`: Runs `EXPLAIN` on the queries to visualize execution plans.
*   `query_analysis.txt`: Detailed breakdown of performance bottlenecks (e.g., Row Explosion, O(N^2) Self-Joins).
*   `q1_solution.sql` - `q4_solution.sql`: Standalone SQL scripts for the 4 analytical questions.

### 3. Design & ETL (The Solution)
*   `design_decisions.txt`: Documentation of design choices (Fact Grain, Dim Tables, Bridge Tables).
*   `star_schema.sql`: DDL for the Data Warehouse tables (`fact_encounters`, `dim_patient`, etc.).
*   `etl_design.txt`: Pseudocode/Logic for the ETL process.
*   `etl_process.py`: Python script that performs the ETL, including handling Many-to-Many relationships and pre-aggregating metrics.

### 4. Verification & Reflection
*   `run_dw_analysis.py`: Runs the rewritten queries against the Star Schema to verify speedups.
*   `star_schema_queries.txt`: Syntax comparison of OLTP vs. Star Schema queries.
*   `reflection.md`: Final analysis of trade-offs, gains, and lessons learned.

## How to Run

### Prerequisites
*   MySQL Server running locally or accessible.
*   Python 3.x with `mysql-connector-python`.

### Steps
1.  **Clone the Repo**:
    ```bash
    git clone https://github.com/Xenongt1/Data-modelling.git
    cd Data-modelling
    ```

2.  **Configure Credentials**:
    Set environment variables for your MySQL connection:
    ```bash
    export DB_USER=root
    export DB_PASSWORD=your_password
    export DB_NAME=medical_lab
    ```

3.  **Setup and Load OLTP**:
    ```bash
    python3 setup_db.py
    ```

4.  **Run Performance Analysis**:
    ```bash
    python3 analyze_queries.py
    python3 run_explain.py
    ```

5.  **Run ETL (Create Star Schema)**:
    ```bash
    python3 etl_process.py
    ```

6.  **Verify Results**:
    ```bash
    python3 run_dw_analysis.py
    ```

## Key Findings
*   **30-Day Readmission Rate**: Improved by **~2.6x** by moving complex self-join logic into the ETL phase.
*   **Revenue Analysis**: Improved by **~1.6x** by pre-aggregating billing data into the Fact table.
*   **Complexity**: Star Schema queries are generally simpler and more readable, though they require maintenance of the ETL pipeline.
