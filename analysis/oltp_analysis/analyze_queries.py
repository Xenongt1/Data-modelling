import os
import sys
import time
import mysql.connector

def get_db_connection():
    host = os.environ.get('DB_HOST', 'localhost')
    user = os.environ.get('DB_USER', 'root')
    password = os.environ.get('DB_PASSWORD', '')
    db_name = os.environ.get('DB_NAME', 'medical_lab')
    
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        sys.exit(1)

def run_query(cursor, name, sql):
    print(f"--- Running: {name} ---")
    start_time = time.time()
    try:
        cursor.execute(sql)
        # Fetch all to force full execution/retrieval
        _ = cursor.fetchall()
        end_time = time.time()
        duration = end_time - start_time
        print(f"Execution Time: {duration:.4f} seconds")
        print(f"Rows returned: {cursor.rowcount}")
        return duration
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}")
        return None

def analyze_queries():
    conn = get_db_connection()
    cursor = conn.cursor()

    queries = [
        {
            "name": "Q1: Monthly Encounters by Specialty",
            "sql": """
                SELECT 
                    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
                    s.specialty_name,
                    e.encounter_type,
                    COUNT(*) AS total_encounters,
                    COUNT(DISTINCT e.patient_id) AS unique_patients
                FROM encounters e
                JOIN providers p ON e.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                GROUP BY 1, 2, 3
            """
        },
        {
            "name": "Q2: Top Diagnosis-Procedure Pairs",
            "sql": """
                SELECT 
                    d.icd10_code, 
                    p.cpt_code, 
                    COUNT(*) AS encounter_count
                FROM encounters e
                JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
                JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
                JOIN encounter_procedures ep ON e.encounter_id = ep.encounter_id
                JOIN procedures p ON ep.procedure_id = p.procedure_id
                GROUP BY 1, 2
                ORDER BY 3 DESC 
                LIMIT 10
            """
        },
        {
            "name": "Q3: 30-Day Readmission Rate",
            "sql": """
                SELECT 
                    s.specialty_name,
                    COUNT(DISTINCT e1.encounter_id) AS total_inpatient_discharges,
                    COUNT(DISTINCT e2.encounter_id) AS readmissions_within_30_days,
                    COUNT(DISTINCT e2.encounter_id) / COUNT(DISTINCT e1.encounter_id) * 100 AS readmission_rate
                FROM encounters e1
                JOIN providers p ON e1.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                LEFT JOIN encounters e2 ON e1.patient_id = e2.patient_id 
                    AND e2.encounter_date > e1.discharge_date 
                    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
                WHERE e1.encounter_type = 'Inpatient'
                GROUP BY 1
            """
        },
        {
            "name": "Q4: Revenue by Specialty & Month",
            "sql": """
                SELECT 
                    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
                    s.specialty_name, 
                    SUM(b.allowed_amount) AS total_revenue
                FROM billing b
                JOIN encounters e ON b.encounter_id = e.encounter_id
                JOIN providers p ON e.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                GROUP BY 1, 2
            """
        }
    ]

    results = []
    
    # Run EXPLAIN for each query (Optional, just to see if we can catch it, but simplified for now we just time it)
    # Actually, let's just run them.
    
    for q in queries:
        duration = run_query(cursor, q["name"], q["sql"])
        results.append((q["name"], duration))
        print("\n")

    conn.close()
    
    with open("query_analysis_results.txt", "w") as f:
        for name, duration in results:
            f.write(f"{name}: {duration:.4f} seconds\n")

if __name__ == "__main__":
    analyze_queries()
