# Healthcare Analytics Lab: OLTP to Star Schema

Welcome to the Medical Data Warehouse project! This lab demonstrates the migration of a slow, normalized (3NF) healthcare database into a high-performance Star Schema Data Warehouse.

## Project Structure

- **`scripts/`**: Python scripts for data generation, analysis, and ETL.
- **`sql/`**: SQL files defining the database schemas.
- **`docs/`**: Documentation, design decisions, and performance reports.

## Getting Started

### Prerequisites
- **MySQL Server** installed and running locally.
- **Python 3.x**
- **Libraries**: `mysql-connector-python`, `faker`

### Setup Environment
```bash
pip install mysql-connector-python faker
```

### Configuration
The scripts look for environment variables to connect to your local MySQL database.
**Default User**: `root`
**Default Password**: (Empty)

To run with your specific password, prefix the commands like this:
```bash
export DB_PASSWORD='YourPasswordHere'
```

---

## How to Run

### Step 1: Generate Data (OLTP)
Create the `medical_oltp` database and populate it with 30,000 synthetic patient encounters.
```bash
cd scripts
DB_PASSWORD='YourPasswordHere' python3 generate_data.py
```

### Step 2: Analyze OLTP Performance
Run the business queries against the normalized database to see the baseline performance (and slowness).
```bash
DB_PASSWORD='YourPasswordHere' python3 run_oltp_analysis.py
```

### Step 3: Run ETL Pipeline
Create the `medical_dw` database (Star Schema) and transform/load the data.
```bash
DB_PASSWORD='YourPasswordHere' python3 etl_logic.py
```

### Step 4: Validate Star Schema Performance
Run the optimized queries against the Data Warehouse to benchmark the improvements.
```bash
DB_PASSWORD='YourPasswordHere' python3 run_dw_analysis.py
```

---

## Key Findings
- **Complex Joins**: Queries involving many-to-many relationships (e.g., Diagnosis-Procedures) run **~60% faster** in the Star Schema.
- **Self-Joins**: Readmission rate calculations are significantly optimized (**~70% faster**).
- **Simplicity**: The Star Schema eliminates complex join chains, making SQL easier to write and maintain.

For a full breakdown, see `docs/reflection.md`.
