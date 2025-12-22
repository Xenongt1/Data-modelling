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

### `database_setup/`
*   `oltp_setup.sql`: DDL and sample data for the normalized tables.
*   `setup_db.py`: Script to create the `medical_lab` database and load OLTP data.

### `analysis/`
*   **`oltp_analysis/`**: Performance analysis of the source system.
    *   `analyze_queries.py`: Measures execution time of OLTP queries.
    *   `run_explain.py`: Runs `EXPLAIN` to visualize execution plans.
    *   `query_analysis.txt`: Detailed bottleneck analysis.
    *   **`solutions/`**: Standalone SQL for the 4 business questions (`q1` - `q4`).
*   **`dw_analysis/`**: Verification of the Star Schema.
    *   `run_dw_analysis.py`: Verifies performance of the new Star Schema.
    *   `star_schema_queries.txt`: Query comparison.
    *   `debug_nulls.py`: Data integrity check script.

### `data_warehouse/`
*   `star_schema.sql`: DDL for the Fact and Dimension tables.
*   `etl_process.py`: Python ETL pipeline (Extracts from OLTP -> Transforms -> Loads to Star Schema).
*   `etl_design.txt`: Documentation of ETL logic.
*   `design_decisions.txt`: Documentation of modeling choices.

### `docs/`
*   `reflection.md`: Final analysis of trade-offs and lessons learned.

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
    python3 database_setup/setup_db.py
    ```

4.  **Run Performance Analysis**:
    ```bash
    python3 analysis/oltp_analysis/analyze_queries.py
    python3 analysis/oltp_analysis/run_explain.py
    ```

5.  **Run ETL (Create Star Schema)**:
    ```bash
    python3 data_warehouse/etl_process.py
    ```

6.  **Verify Results**:
    ```bash
    python3 analysis/dw_analysis/run_dw_analysis.py
    ```

## Key Findings
*   **30-Day Readmission Rate**: Improved by **~2.6x** by moving complex self-join logic into the ETL phase.
*   **Revenue Analysis**: Improved by **~1.6x** by pre-aggregating billing data into the Fact table.
*   **Complexity**: Star Schema queries are generally simpler and more readable, though they require maintenance of the ETL pipeline.
