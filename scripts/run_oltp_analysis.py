import mysql.connector
import os
import time

# Configuration
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database='medical_oltp'
    )

def run_analysis():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Enable profiling for this session
    cursor.execute("SET profiling = 1")
    
    queries = [
        {
            "name": "Q1: Monthly Encounters by Specialty",
            "description": "Join Encounters -> Providers -> Specialties. Group by Month, Specialty, Type.",
            "sql": """
                SELECT 
                    DATE_FORMAT(e.encounter_date, '%Y-%m') as month,
                    s.specialty_name,
                    e.encounter_type,
                    COUNT(e.encounter_id) as total_encounters,
                    COUNT(DISTINCT e.patient_id) as unique_patients
                FROM encounters e
                JOIN providers p ON e.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                GROUP BY 1, 2, 3
                ORDER BY 1, 2, 3
            """
        },
        {
            "name": "Q2: Top Diagnosis-Procedure Pairs",
            "description": "Join Diagnosis Jct -> Procedure Jct via Encounter. High Row Explosion risk.",
            "sql": """
                SELECT 
                    d.icd10_code,
                    p.cpt_code,
                    COUNT(e.encounter_id) as overlap_count
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
            "description": "Self-join on Encounters to find returns within 30 days of inpatient discharge.",
            "sql": """
                SELECT 
                    s.specialty_name,
                    COUNT(DISTINCT initial.encounter_id) as index_admissions,
                    COUNT(DISTINCT readmit.encounter_id) as readmissions,
                    (COUNT(DISTINCT readmit.encounter_id) / COUNT(DISTINCT initial.encounter_id)) * 100 as readmission_rate
                FROM encounters initial
                JOIN providers p ON initial.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                LEFT JOIN encounters readmit ON initial.patient_id = readmit.patient_id
                    AND readmit.encounter_date > initial.discharge_date
                    AND readmit.encounter_date <= DATE_ADD(initial.discharge_date, INTERVAL 30 DAY)
                WHERE initial.encounter_type = 'Inpatient'
                GROUP BY 1
                ORDER BY 4 DESC
            """
        },
        {
            "name": "Q4: Revenue by Specialty & Month",
            "description": "Billing -> Encounters -> Providers -> Specialties. Aggregating money.",
            "sql": """
                SELECT 
                    DATE_FORMAT(b.claim_date, '%Y-%m') as month,
                    s.specialty_name,
                    SUM(b.allowed_amount) as total_revenue
                FROM billing b
                JOIN encounters e ON b.encounter_id = e.encounter_id
                JOIN providers p ON e.provider_id = p.provider_id
                JOIN specialties s ON p.specialty_id = s.specialty_id
                WHERE b.claim_status = 'Paid'
                GROUP BY 1, 2
                ORDER BY 1, 3 DESC
            """
        }
    ]
    
    print(f"{'Query Name':<40} | {'Duration (s)':<15} | {'Rows Returned':<15}")
    print("-" * 80)
    
    results = []
    
    for q in queries:
        # We also measure wall time just in case profiling misses something or is confusing
        start_wall = time.perf_counter()
        cursor.execute(q['sql'])
        rows = cursor.fetchall()  # Fetch all to force execution
        end_wall = time.perf_counter()
        wall_duration = end_wall - start_wall
        
        # Get Profile
        cursor.execute("SHOW PROFILES")
        profiles = cursor.fetchall()
        # The last one is ours
        # Profile row: [Query_ID, Duration, Query]
        db_duration = profiles[-1][1]
        
        print(f"{q['name']:<40} | {float(db_duration):<15.4f} | {len(rows):<15}")
        
        results.append({
            "name": q['name'],
            "duration": db_duration,
            "rows": len(rows),
            "sql": q['sql'],
            "desc": q['description']
        })

    cursor.close()
    conn.close()
    
    return results

if __name__ == "__main__":
    run_analysis()
