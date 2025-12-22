import os
import sys
import datetime
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

def execute_sql_file(cursor, filename):
    print(f"Executing {filename}...")
    with open(filename, 'r') as f:
        sql = f.read()
    
    # Simple split by semicolon.
    statements = sql.split(';')
    for stmt in statements:
        if stmt.strip():
            cursor.execute(stmt)

def etl_process():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Create Star Schema Tables
    execute_sql_file(cursor, 'star_schema.sql')
    conn.commit()
    print("Star Schema tables created.")
    
    # 2. Load Dimensions
    
    print("Loading dim_encounter_type...")
    cursor.execute("INSERT INTO dim_encounter_type (encounter_type_name) SELECT DISTINCT encounter_type FROM encounters")
    
    print("Loading dim_department...")
    cursor.execute("""
        INSERT INTO dim_department (department_id, department_name, floor, capacity)
        SELECT department_id, department_name, floor, capacity FROM departments
    """)
    
    print("Loading dim_specialty (into dim_provider mostly, but just in case we need standalone, wait, we decided against dim_specialty standalone)")
    # We decided dim_provider contains specialty_name.
    
    print("Loading dim_provider...")
    cursor.execute("""
        INSERT INTO dim_provider (provider_id, provider_name, credential, specialty_name)
        SELECT 
            p.provider_id, 
            CONCAT(p.first_name, ' ', p.last_name), 
            p.credential,
            s.specialty_name
        FROM providers p
        JOIN specialties s ON p.specialty_id = s.specialty_id
    """)

    print("Loading dim_diagnosis...")
    cursor.execute("""
        INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
        SELECT diagnosis_id, icd10_code, icd10_description FROM diagnoses
    """)

    print("Loading dim_procedure...")
    cursor.execute("""
        INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
        SELECT procedure_id, cpt_code, cpt_description FROM procedures
    """)
    
    print("Loading dim_patient...")
    # Calculate age in Python or SQL? SQL is easier for now.
    cursor.execute("""
        INSERT INTO dim_patient (patient_id, first_name, last_name, full_name, date_of_birth, gender, mrn, current_age)
        SELECT 
            patient_id, first_name, last_name, 
            CONCAT(first_name, ' ', last_name),
            date_of_birth, gender, mrn,
            TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE())
        FROM patients
    """)
    
    print("Loading dim_date...")
    # Populate date dimension for a reasonable range (2020-2025 based on sample data)
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2025, 12, 31)
    current_date = start_date
    
    batch_data = []
    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d'))
        is_weekend = 1 if current_date.weekday() >= 5 else 0
        batch_data.append((
            date_key, 
            current_date, 
            current_date.year, 
            current_date.month, 
            current_date.strftime('%B'), 
            (current_date.month - 1) // 3 + 1, 
            is_weekend
        ))
        current_date += datetime.timedelta(days=1)
        
    cursor.executemany("""
        INSERT INTO dim_date (date_key, full_date, year, month, month_name, quarter, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    
    conn.commit()
    print("Dimensions loaded.")

    # 3. Load Fact Table
    print("Loading fact_encounters...")
    
    # We need to calculate is_readmission. We can fetch all encounters, sort them in python, calculate it, then insert.
    cursor.execute("""
        SELECT 
            e.encounter_id,
            e.patient_id,
            e.provider_id,
            e.department_id,
            e.encounter_type,
            e.encounter_date,
            e.discharge_date,
            IFNULL(b.claim_amount, 0),
            IFNULL(b.allowed_amount, 0)
        FROM encounters e
        LEFT JOIN billing b ON e.encounter_id = b.encounter_id
        ORDER BY e.patient_id, e.encounter_date
    """)
    
    encounters = cursor.fetchall()
    
    # Pre-fetch dimension lookups to avoid N+1 queries
    # Helper to build lookup dict
    def get_lookup(table, key_col, val_col):
        cursor.execute(f"SELECT {key_col}, {val_col} FROM {table}")
        return {row[1]: row[0] for row in cursor.fetchall()}
    
    patient_map = get_lookup("dim_patient", "patient_key", "patient_id")
    provider_map = get_lookup("dim_provider", "provider_key", "provider_id")
    dept_map = get_lookup("dim_department", "department_key", "department_id")
    type_map = get_lookup("dim_encounter_type", "encounter_type_key", "encounter_type_name")
    
    # For counts, we can do subqueries or separate fetches. Let's do separate fetches and map.
    cursor.execute("SELECT encounter_id, COUNT(*) FROM encounter_diagnoses GROUP BY encounter_id")
    diag_counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.execute("SELECT encounter_id, COUNT(*) FROM encounter_procedures GROUP BY encounter_id")
    proc_counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    fact_rows = []
    
    # Logic for readmission
    # Format: {patient_id: last_inpatient_discharge_date}
    patient_last_discharge = {}
    
    for row in encounters:
        enc_id, pat_id, prov_id, dept_id, enc_type, enc_date, disch_date, claim, allowed = row
        
        # Keys
        pat_key = patient_map.get(pat_id)
        prov_key = provider_map.get(prov_id)
        dept_key = dept_map.get(dept_id)
        type_key = type_map.get(enc_type)
        date_key = int(enc_date.strftime('%Y%m%d'))
        
        # Metrics
        d_count = diag_counts.get(enc_id, 0)
        p_count = proc_counts.get(enc_id, 0)
        
        # LOS
        if disch_date and enc_date:
            los = (disch_date - enc_date).total_seconds() / 3600.0
        else:
            los = 0
            
        # Readmission Logic
        is_readmission = 0
        if pat_id in patient_last_discharge:
            last_disch = patient_last_discharge[pat_id]
            # Check if within 30 days
            if enc_date and last_disch:
                delta = enc_date - last_disch
                if 0 <= delta.days <= 30:
                    is_readmission = 1
        
        # Update history if this is inpatient
        if enc_type == 'Inpatient' and disch_date:
            patient_last_discharge[pat_id] = disch_date
            
        fact_rows.append((
            enc_id, date_key, pat_key, prov_key, dept_key, type_key,
            claim, allowed, d_count, p_count, is_readmission, los
        ))
        
    cursor.executemany("""
        INSERT INTO fact_encounters (
            encounter_id, date_key, patient_key, provider_key, department_key, encounter_type_key,
            total_claim_amount, total_allowed_amount, diagnosis_count, procedure_count, is_readmission, length_of_stay_hours
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, fact_rows)
    
    conn.commit()
    print(f"Fact table loaded. {len(fact_rows)} rows.")
    
    # 4. Load Bridge Tables
    print("Loading bridge_encounter_diagnoses...")
    
    # We need fact_encounter_id map
    cursor.execute("SELECT encounter_id, fact_encounter_id FROM fact_encounters")
    fact_map = {row[0]: row[1] for row in cursor.fetchall()}
    
    diag_map = get_lookup("dim_diagnosis", "diagnosis_key", "diagnosis_id")
    
    cursor.execute("SELECT encounter_id, diagnosis_id, diagnosis_sequence FROM encounter_diagnoses")
    ed_rows = cursor.fetchall()
    bridge_d_rows = []
    for r in ed_rows:
        eid, did, seq = r
        fid = fact_map.get(eid)
        dkey = diag_map.get(did)
        if fid and dkey:
            bridge_d_rows.append((fid, dkey, seq))
            
    cursor.executemany("INSERT INTO bridge_encounter_diagnoses (fact_encounter_id, diagnosis_key, diagnosis_sequence) VALUES (%s, %s, %s)", bridge_d_rows)

    print("Loading bridge_encounter_procedures...")
    proc_map = get_lookup("dim_procedure", "procedure_key", "procedure_id")
    
    cursor.execute("SELECT encounter_id, procedure_id FROM encounter_procedures")
    ep_rows = cursor.fetchall()
    bridge_p_rows = []
    for r in ep_rows:
        eid, pid = r
        fid = fact_map.get(eid)
        pkey = proc_map.get(pid)
        if fid and pkey:
            bridge_p_rows.append((fid, pkey))
            
    cursor.executemany("INSERT INTO bridge_encounter_procedures (fact_encounter_id, procedure_key) VALUES (%s, %s)", bridge_p_rows)
    
    conn.commit()
    print("Bridge tables loaded.")
    conn.close()

if __name__ == "__main__":
    etl_process()
