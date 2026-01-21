# Healthcare Analytics Lab: OLTP to Star Schema

Welcome to the Medical Data Warehouse project! This lab demonstrates the migration of a slow, normalized (3NF) healthcare database into a high-performance Star Schema Data Warehouse.

## Project Structure

- **`scripts/`**:
    - `generate_data.py`: Python script to generate synthetic patient data and populate the OLTP database.
- **`sql/`**:
    - `oltp_setup.sql`: DDL to create the normalized `medical_oltp` database schema.
    - `DML.sql`: Analytical queries to benchmark the OLTP database performance.
    - `star_schema.sql`: DDL to create the `medical_dw` Star Schema.
    - `production_etl.sql`: SQL script to transform and load data from OLTP to DW.
    - `DML2.sql`: Optimized analytical queries for the Star Schema.
- **`docs/`**: Documentation, design decisions (`design_decisions.txt`), and analysis reports.

## Getting Started

### Prerequisites
- **MySQL Server** installed and running locally.
- **Python 3.x**
- **Libraries**: `mysql-connector-python`, `faker` (for data generation).

### Setup Environment
```bash
pip install mysql-connector-python faker dotenv
```

### Configuration
The generation script looks for environment variables to connect to your local MySQL database.
**Default User**: `root`
**Default Password**: (Empty)

You can create a `.env` file in the root directory or export variables directly:
```bash
export DB_PASSWORD='YourPasswordHere'
```

---

## How to Run

### Step 1: Generate Data (OLTP)
Create the `medical_oltp` database and populate it with synthetic patient encounters.
```bash
cd scripts
python generate_data.py
```

### Step 2: Analyze OLTP Performance
Run the business queries against the normalized database to see the baseline performance.
Execute the SQL commands in `sql/DML.sql` using your preferred MySQL client (Workbench, DBeaver, or CLI).

**CLI Example:**
```bash
mysql -u root -p < sql/DML.sql
```

### Step 3: Run ETL Pipeline
Create the `medical_dw` database (Star Schema) and transform/load the data.
Execute `star_schema.sql` first to create the tables, then `production_etl.sql` to load the data.

**CLI Example:**
```bash
mysql -u root -p < sql/star_schema.sql
mysql -u root -p < sql/production_etl.sql
```

### Step 4: Validate Star Schema Performance
Run the optimized queries against the Data Warehouse to benchmark the improvements.
Execute `sql/DML2.sql`.

**CLI Example:**
```bash
mysql -u root -p < sql/DML2.sql
```

---

## Key Findings
- **Complex Joins**: Queries involving many-to-many relationships (e.g., Diagnosis-Procedures) run **~60% faster** in the Star Schema.
- **Self-Joins**: Readmission rate calculations are significantly optimized (**~70% faster**).
- **Simplicity**: The Star Schema eliminates complex join chains, making SQL easier to write and maintain.

For a full breakdown, see `docs/reflection.md`.
