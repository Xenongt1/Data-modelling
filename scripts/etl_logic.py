import mysql.connector
import os
import datetime
import time

# Configuration
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

def get_connection(database=None):
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=database,
        allow_local_infile=True
    )

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def etl_process():
    # 0. Initialize DW Schema
    log("Initializing DW Schema...")
    conn = get_connection()
    cursor = conn.cursor()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, '../sql/star_schema.sql')
    
    with open(sql_file_path, 'r') as f:
        # Split by ; but handle potential complexity if needed. 
        # For this file, simple split is fine.
        sql_commands = f.read().split(';')
        for cmd in sql_commands:
            if cmd.strip():
                cursor.execute(cmd)
    conn.commit()
    cursor.close()
    conn.close()

    # Connector for Work
    oltp_conn = get_connection('medical_oltp')
    dw_conn = get_connection('medical_dw')
    oltp_cursor = oltp_conn.cursor(dictionary=True)
    dw_cursor = dw_conn.cursor()

    try:
        # 1. Load dim_date
        log("Loading dim_date...")
        start_date = datetime.date(2020, 1, 1)
        end_date = datetime.date(2030, 12, 31)
        delta = datetime.timedelta(days=1)
        
        date_data = []
        curr = start_date
        while curr <= end_date:
            date_key = int(curr.strftime('%Y%m%d'))
            date_data.append((
                date_key, curr, curr.year, curr.month, 
                curr.strftime('%B'), (curr.month-1)//3 + 1, 
                curr.weekday(), curr.strftime('%A')
            ))
            curr += delta
        
        dw_cursor.executemany("""
            INSERT INTO dim_date (date_key, full_date, year, month, month_name, quarter, day_of_week, day_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, date_data)
        dw_conn.commit()

        # 2. Load dim_patient
        log("Loading dim_patient...")
        oltp_cursor.execute("SELECT * FROM patients")
        patients = oltp_cursor.fetchall()
        p_data = []
        for p in patients:
            age = (datetime.date.today() - p['date_of_birth']).days // 365
            p_data.append((p['patient_id'], p['first_name'], p['last_name'], p['date_of_birth'], age, p['gender'], p['mrn']))
        
        dw_cursor.executemany("""
            INSERT INTO dim_patient (patient_id, first_name, last_name, dob, current_age, gender, mrn)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, p_data)
        dw_conn.commit()

        # 3. Load dim_specialty (Direct Copy)
        log("Loading dim_specialty...")
        oltp_cursor.execute("SELECT * FROM specialties")
        specs = oltp_cursor.fetchall()
        dw_cursor.executemany("INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code) VALUES (%s, %s, %s)",
                             [(x['specialty_id'], x['specialty_name'], x['specialty_code']) for x in specs])
        dw_conn.commit()

        # 4. Load dim_department (Direct Copy)
        log("Loading dim_department...")
        oltp_cursor.execute("SELECT * FROM departments")
        depts = oltp_cursor.fetchall()
        dw_cursor.executemany("INSERT INTO dim_department (department_id, department_name) VALUES (%s, %s)",
                             [(x['department_id'], x['department_name']) for x in depts])
        dw_conn.commit()

        # 5. Load dim_provider (Denormalized)
        log("Loading dim_provider...")
        oltp_cursor.execute("""
            SELECT p.provider_id, p.first_name, p.last_name, p.credential, s.specialty_name, d.department_name
            FROM providers p
            JOIN specialties s ON p.specialty_id = s.specialty_id
            JOIN departments d ON p.department_id = d.department_id
        """)
        provs = oltp_cursor.fetchall()
        dw_cursor.executemany("""
            INSERT INTO dim_provider (provider_id, provider_name, credential, specialty_name, department_name)
            VALUES (%s, %s, %s, %s, %s)
        """, [(x['provider_id'], f"{x['first_name']} {x['last_name']}", x['credential'], x['specialty_name'], x['department_name']) for x in provs])
        dw_conn.commit()
        
        # 6. Load dim_encounter_type
        log("Loading dim_encounter_type...")
        oltp_cursor.execute("SELECT DISTINCT encounter_type FROM encounters")
        types = oltp_cursor.fetchall()
        dw_cursor.executemany("INSERT INTO dim_encounter_type (encounter_type_name) VALUES (%s)", [(x['encounter_type'],) for x in types])
        dw_conn.commit()

        # 7. Load dim_diagnosis & dim_procedure
        log("Loading reference dimensions...")
        oltp_cursor.execute("SELECT * FROM diagnoses")
        diags = oltp_cursor.fetchall()
        dw_cursor.executemany("INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description) VALUES (%s, %s, %s)",
                             [(x['diagnosis_id'], x['icd10_code'], x['icd10_description']) for x in diags])
        
        oltp_cursor.execute("SELECT * FROM procedures")
        procs = oltp_cursor.fetchall()
        dw_cursor.executemany("INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description) VALUES (%s, %s, %s)",
                             [(x['procedure_id'], x['cpt_code'], x['cpt_description']) for x in procs])
        dw_conn.commit()

        # 8. Load Fact Table (fact_encounters)
        log("Loading fact_encounters...")
        # Dictionary Caches for Keys
        def get_map(table, key_col, val_col):
            dw_cursor.execute(f"SELECT {key_col}, {val_col} FROM {table}")
            return {row[1]: row[0] for row in dw_cursor.fetchall()}
        
        patient_map = get_map('dim_patient', 'patient_key', 'patient_id')
        provider_map = get_map('dim_provider', 'provider_key', 'provider_id')
        specialty_map = get_map('dim_specialty', 'specialty_key', 'specialty_id')
        dept_map = get_map('dim_department', 'department_key', 'department_id')
        type_map = get_map('dim_encounter_type', 'encounter_type_key', 'encounter_type_name')

        # Extract Source Data optimized
        query = """
            SELECT 
                e.encounter_id, e.patient_id, e.provider_id, e.encounter_date, 
                e.discharge_date, e.department_id, e.encounter_type,
                b.claim_amount, b.allowed_amount,
                p.specialty_id -- We need this for the redundant key
            FROM encounters e
            LEFT JOIN billing b ON e.encounter_id = b.encounter_id
            JOIN providers p ON e.provider_id = p.provider_id
        """
        oltp_cursor.execute(query)
        encounters = oltp_cursor.fetchall()
        
        fact_data = []
        for e in encounters:
            dt = e['encounter_date']
            d_key = int(dt.strftime('%Y%m%d'))
            
            los = 0
            if e['discharge_date']:
                los = (e['discharge_date'] - dt).total_seconds() / 86400.0
            
            p_key = patient_map.get(e['patient_id'])
            prov_key = provider_map.get(e['provider_id'])
            spec_key = specialty_map.get(e['specialty_id'])
            dept_key = dept_map.get(e['department_id'])
            type_key = type_map.get(e['encounter_type'])
            
            is_inpatient = 1 if e['encounter_type'] == 'Inpatient' else 0
            
            fact_data.append((
                e['encounter_id'], p_key, prov_key, d_key, spec_key, dept_key, type_key,
                los, e['claim_amount'] or 0, e['allowed_amount'] or 0, is_inpatient
            ))
            
        # Bulk Insert Fact
        # Chunking just in case
        chunk_size = 5000
        for i in range(0, len(fact_data), chunk_size):
            dw_cursor.executemany("""
                INSERT INTO fact_encounters 
                (encounter_id, patient_key, provider_key, date_key, specialty_key, department_key, encounter_type_key,
                 length_of_stay_days, claim_amount, allowed_amount, is_inpatient_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, fact_data[i:i+chunk_size])
        dw_conn.commit()
        
        # 9. Load Bridge Tables
        log("Loading Bridges...")
        
        # Need encounter_id -> encounter_key map
        dw_cursor.execute("SELECT encounter_key, encounter_id FROM fact_encounters")
        enc_map = {row[1]: row[0] for row in dw_cursor.fetchall()}
        
        diagnosis_map = get_map('dim_diagnosis', 'diagnosis_key', 'diagnosis_id')
        procedure_map = get_map('dim_procedure', 'procedure_key', 'procedure_id')
        
        # Diagnoses Bridge
        oltp_cursor.execute("SELECT encounter_id, diagnosis_id, diagnosis_sequence FROM encounter_diagnoses")
        ed_rows = oltp_cursor.fetchall()
        bridge_d_data = []
        for row in ed_rows:
            ek = enc_map.get(row['encounter_id'])
            dk = diagnosis_map.get(row['diagnosis_id'])
            if ek and dk:
                bridge_d_data.append((ek, dk, row['diagnosis_sequence']))
        
        for i in range(0, len(bridge_d_data), chunk_size):
            dw_cursor.executemany("INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence) VALUES (%s, %s, %s)", bridge_d_data[i:i+chunk_size])

        # Procedures Bridge
        oltp_cursor.execute("SELECT encounter_id, procedure_id, procedure_date FROM encounter_procedures")
        ep_rows = oltp_cursor.fetchall()
        bridge_p_data = []
        for row in ep_rows:
            ek = enc_map.get(row['encounter_id'])
            pk = procedure_map.get(row['procedure_id'])
            pd_key = int(row['procedure_date'].strftime('%Y%m%d')) if row['procedure_date'] else None
            if ek and pk:
                bridge_p_data.append((ek, pk, pd_key))
                
        for i in range(0, len(bridge_p_data), chunk_size):
            dw_cursor.executemany("INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key) VALUES (%s, %s, %s)", bridge_p_data[i:i+chunk_size])

        dw_conn.commit()
        log("ETL Complete Successfully.")
        
    except mysql.connector.Error as err:
        log(f"ETL Failed: {err}")
    finally:
        oltp_cursor.close()
        oltp_conn.close()
        dw_cursor.close()
        dw_conn.close()

if __name__ == "__main__":
    etl_process()
