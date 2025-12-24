"""
Data Warehouse Performance Analysis
-----------------------------------
Executes optimized analytical queries against the Star Schema (`medical_dw`).
Utilizes MySQL's native `SET profiling = 1` to capture comparison metrics.

Output:
    Logs the execution time and row counts for each query to 'execution.log'.
"""

import mysql.connector
import os
import time
import logging

# Configure Logging
logging.basicConfig(
    filename='execution.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

def get_connection():
    """Establishes connection to the DW database."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database='medical_dw'
    )

def run_analysis():
    """
    Runs 4 optimized SQL queries and logs their performance.
    Matches the same business questions as the OLTP analysis for fair comparison.
    """
    logging.info("Starting DW Performance Analysis...")
    conn = get_connection()
    cursor = conn.cursor()
    
    # Enable profiling
    cursor.execute("SET profiling = 1")
    
    queries = [
        {
            "name": "Q1: Monthly Encounters by Specialty (Star)",
            "sql": """
                SELECT 
                    d.month_name,
                    s.specialty_name,
                    t.encounter_type_name,
                    COUNT(f.encounter_key) as total_encounters,
                    COUNT(DISTINCT f.patient_key) as unique_patients
                FROM fact_encounters f
                JOIN dim_date d ON f.date_key = d.date_key
                JOIN dim_specialty s ON f.specialty_key = s.specialty_key
                JOIN dim_encounter_type t ON f.encounter_type_key = t.encounter_type_key
                GROUP BY d.year, d.month, d.month_name, s.specialty_name, t.encounter_type_name
            """
        },
        {
            "name": "Q2: Top Diagnosis-Procedure Pairs (Star)",
            "sql": """
                SELECT 
                    dd.icd10_code,
                    dp.cpt_code,
                    COUNT(bd.encounter_key) as overlap_count
                FROM bridge_encounter_diagnoses bd
                JOIN bridge_encounter_procedures bp ON bd.encounter_key = bp.encounter_key
                JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
                JOIN dim_procedure dp ON bp.procedure_key = dp.procedure_key
                GROUP BY 1, 2
                ORDER BY 3 DESC
                LIMIT 10
            """
        },
        {
            "name": "Q3: 30-Day Readmission Rate (Star)",
            "sql": """
                SELECT 
                    s.specialty_name,
                    COUNT(DISTINCT f1.encounter_key) as index_admissions,
                    COUNT(DISTINCT f2.encounter_key) as readmissions,
                    (COUNT(DISTINCT f2.encounter_key) / COUNT(DISTINCT f1.encounter_key)) * 100 as rate
                FROM fact_encounters f1
                JOIN dim_specialty s ON f1.specialty_key = s.specialty_key
                JOIN dim_date d1 ON f1.date_key = d1.date_key
                LEFT JOIN fact_encounters f2 ON f1.patient_key = f2.patient_key
                    AND f2.encounter_key > f1.encounter_key -- Optimization: Ensure we look forward
                    AND f2.date_key > f1.date_key
                JOIN dim_date d2 ON f2.date_key = d2.date_key
                WHERE f1.is_inpatient_flag = 1
                    AND d2.full_date <= DATE_ADD(d1.full_date, INTERVAL 30 DAY)
                GROUP BY 1
                ORDER BY 4 DESC
            """
        },
        {
            "name": "Q4: Revenue by Specialty & Month (Star)",
            "sql": """
                SELECT 
                    d.month_name,
                    s.specialty_name,
                    SUM(f.allowed_amount) as total_revenue
                FROM fact_encounters f
                JOIN dim_date d ON f.date_key = d.date_key
                JOIN dim_specialty s ON f.specialty_key = s.specialty_key
                GROUP BY d.year, d.month, d.month_name, s.specialty_name
                ORDER BY d.year, d.month, 3 DESC
            """
        }
    ]
    
    # Log Header
    logging.info(f"{'Query Name':<40} | {'Duration (s)':<15} | {'Rows Returned':<15}")
    logging.info("-" * 80)
    
    results = []
    
    for q in queries:
        start_wall = time.perf_counter()
        cursor.execute(q['sql'])
        rows = cursor.fetchall()
        end_wall = time.perf_counter()
        
        cursor.execute("SHOW PROFILES")
        profiles = cursor.fetchall()
        db_duration = profiles[-1][1]
        
        # Log Result
        logging.info(f"{q['name']:<40} | {float(db_duration):<15.4f} | {len(rows):<15}")
        results.append((q['name'], db_duration))

    cursor.close()
    conn.close()
    logging.info("DW Analysis Complete.")

if __name__ == "__main__":
    run_analysis()
