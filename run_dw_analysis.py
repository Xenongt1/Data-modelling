import os
import sys
import time
import mysql.connector

def get_db_connection():
    host = os.environ.get('DB_HOST', 'localhost')
    user = os.environ.get('DB_USER', 'root')
    password = os.environ.get('DB_PASSWORD', '')
    db_name = os.environ.get('DB_NAME', 'medical_lab')
    
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    return conn

def run_query(cursor, name, sql):
    print(f"--- Running: {name} ---")
    start_time = time.time()
    try:
        cursor.execute(sql)
        _ = cursor.fetchall()
        end_time = time.time()
        duration = end_time - start_time
        print(f"Execution Time: {duration:.4f} seconds")
        print(f"Rows returned: {cursor.rowcount}")
        return duration
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}")
        return None

def analyze_dw_queries():
    conn = get_db_connection()
    cursor = conn.cursor()

    queries = [
        {
            "name": "Q1 (Star): Monthly Encounters by Specialty",
            "sql": """
                SELECT 
                    d.year, d.month,
                    p.specialty_name,
                    et.encounter_type_name,
                    COUNT(*) AS total_encounters,
                    COUNT(DISTINCT f.patient_key) AS unique_patients
                FROM fact_encounters f
                JOIN dim_date d ON f.date_key = d.date_key
                JOIN dim_provider p ON f.provider_key = p.provider_key
                JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
                GROUP BY 1, 2, 3, 4
            """
        },
        {
            "name": "Q2 (Star): Top Diagnosis-Procedure Pairs",
            "sql": """
                SELECT 
                    diag.icd10_code, 
                    proc.cpt_code, 
                    COUNT(*) AS encounter_count
                FROM fact_encounters f
                JOIN bridge_encounter_diagnoses bd ON f.fact_encounter_id = bd.fact_encounter_id
                JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
                JOIN bridge_encounter_procedures bp ON f.fact_encounter_id = bp.fact_encounter_id
                JOIN dim_procedure proc ON bp.procedure_key = proc.procedure_key
                GROUP BY 1, 2
                ORDER BY 3 DESC 
                LIMIT 10
            """
        },
        {
            "name": "Q3 (Star): 30-Day Readmission Rate",
            "sql": """
                SELECT 
                    p.specialty_name,
                    COUNT(DISTINCT CASE WHEN et.encounter_type_name = 'Inpatient' THEN f.encounter_id END) AS total_inpatient_discharges,
                    SUM(f.is_readmission) AS readmissions,
                    SUM(f.is_readmission) / COUNT(DISTINCT CASE WHEN et.encounter_type_name = 'Inpatient' THEN f.encounter_id END) * 100 as rate
                FROM fact_encounters f
                JOIN dim_provider p ON f.provider_key = p.provider_key
                JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
                GROUP BY 1
            """
        },
        {
            "name": "Q4 (Star): Revenue by Specialty & Month",
            "sql": """
                SELECT 
                    d.year, d.month,
                    p.specialty_name, 
                    SUM(f.total_allowed_amount) AS total_revenue
                FROM fact_encounters f
                JOIN dim_date d ON f.date_key = d.date_key
                JOIN dim_provider p ON f.provider_key = p.provider_key
                GROUP BY 1, 2, 3
            """
        }
    ]

    results = []
    
    for q in queries:
        duration = run_query(cursor, q["name"], q["sql"])
        results.append((q["name"], duration))
        print("\n")

    conn.close()
    
    with open("dw_query_analysis_results.txt", "w") as f:
        for name, duration in results:
            f.write(f"{name}: {duration:.4f} seconds\n")

if __name__ == "__main__":
    analyze_dw_queries()
