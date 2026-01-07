"""
Healthcare Data Generator
------------------------
This script functions as the 'Builder' for the OLTP database.
It handles the creation of the schema and the generation of synthetic, 
but realistic, healthcare data for testing performance.

Dependencies:
    - mysql-connector-python
    - faker
    - random

Environment Variables:
    - DB_HOST (default: localhost)
    - DB_USER (default: root)
    - DB_PASSWORD (required for secure access)
"""

import mysql.connector
import os
import random
import logging
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

if not DB_PASSWORD:
    logging.warning("DB_PASSWORD environment variable not set. Attempting connection with empty password.")

def get_connection():
    """Establishes a connection to the MySQL server."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        allow_local_infile=True
    )

def execute_sql_file(filename, cursor):
    """
    Reads and executes a SQL script file containing DDL commands.
    
    Args:
        filename (str): Path to the .sql file.
        cursor (MySQLCursor): Use this cursor to execute commands.
    """
    with open(filename, 'r') as f:
        statements = f.read().split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)

def generate_data():
    """
    Main execution function.
    1. Initializes the database schema.
    2. Generates reference data (Specialties, Departments).
    3. Generates Providers.
    4. Generates Patients (9,000).
    5. Generates Encounters (30,000).
    6. Generates Clinical/Billing details (Diagnoses, Procedures).
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    logging.info("Starting Data Generation Process...")
    logging.info("Initializing database schema...")
    
    # Locate the SQL file relative to this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dir, '../sql/oltp_setup.sql')
    
    try:
        execute_sql_file(sql_file_path, cursor)
    except mysql.connector.Error as err:
        logging.error(f"Error executing SQL script: {err}")
        return

    conn.commit()
    logging.info("Database initialized successfully.")

    # Reconnect to the specific database
    cursor.close()
    conn.close()
    
    conn = get_connection()
    conn.database = 'medical_oltp'
    cursor = conn.cursor()

    fake = Faker()

    # 1. Specialties & Departments (Static-ish data)
    logging.info("Populating reference tables (Specialties, Departments)...")
    specialties = [
        ('Cardiology', 'CARD'), ('Internal Medicine', 'IM'), ('Emergency', 'ER'),
        ('Orthopedics', 'ORTHO'), ('Pediatrics', 'PED'), ('Neurology', 'NEURO')
    ]
    cursor.executemany("INSERT INTO specialties (specialty_name, specialty_code) VALUES (%s, %s)", specialties)
    
    departments = [
        ('Cardiology Unit', 3, 20), ('Internal Medicine', 2, 30), ('Emergency', 1, 45),
        ('Orthopedics Wing', 4, 15), ('Pediatrics Ward', 5, 25), ('Neurology Centers', 6, 10)
    ]
    cursor.executemany("INSERT INTO departments (department_name, floor, capacity) VALUES (%s, %s, %s)", departments)
    conn.commit()

    # 2. Providers
    logging.info("Generating Providers...")
    providers_data = []
    for _ in range(50):
        fname = fake.first_name()
        lname = fake.last_name()
        cred = random.choice(['MD', 'DO', 'NP', 'PA'])
        spec_id = random.randint(1, len(specialties))
        dept_id = spec_id # Simplified 1:1 map for data generation purposes
        providers_data.append((fname, lname, cred, spec_id, dept_id))
    cursor.executemany("INSERT INTO providers (first_name, last_name, credential, specialty_id, department_id) VALUES (%s, %s, %s, %s, %s)", providers_data)
    conn.commit()

    # 3. Patients (9000)
    logging.info("Generating 9,000 Patients...")
    patients_data = []
    for i in range(1, 9001):
        fname = fake.first_name()
        lname = fake.last_name()
        dob = fake.date_of_birth(minimum_age=0, maximum_age=90)
        gender = random.choice(['M', 'F'])
        mrn = f'MRN{i:05d}'
        patients_data.append((fname, lname, dob, gender, mrn))
    
    # Insert in batches for performance
    batch_size = 1000
    for i in range(0, len(patients_data), batch_size):
        batch = patients_data[i:i+batch_size]
        cursor.executemany("INSERT INTO patients (first_name, last_name, date_of_birth, gender, mrn) VALUES (%s, %s, %s, %s, %s)", batch)
    conn.commit()

    # 4. Diagnoses & Procedures (Reference)
    logging.info("Populating Diagnoses and Procedures catalogs...")
    diagnoses = [
        ('I10', 'Essential (primary) hypertension'), ('E11.9', 'Type 2 diabetes mellitus without complications'),
        ('J01.90', 'Acute sinusitis, unspecified'), ('Z00.00', 'Encounter for general adult medical examination'),
        ('M54.5', 'Low back pain'), ('J20.9', 'Acute bronchitis, unspecified'),
        ('I50.9', 'Heart failure, unspecified'), ('E78.5', 'Hyperlipidemia, unspecified')
    ]
    cursor.executemany("INSERT INTO diagnoses (icd10_code, icd10_description) VALUES (%s, %s)", diagnoses)

    procedures = [
        ('99213', 'Office or other outpatient visit...'), ('99214', 'Office or other outpatient visit...'),
        ('93000', 'Electrocardiogram, routine ECG with at least 12 leads'),
        ('71046', 'Radiologic examination, chest, 2 views'),
        ('85025', 'Blood count; complete (CBC), automated'),
        ('80053', 'Comprehensive metabolic panel')
    ]
    cursor.executemany("INSERT INTO procedures (cpt_code, cpt_description) VALUES (%s, %s)", procedures)
    conn.commit()

    # 5. Encounters (30,000)
    logging.info("Generating 30,000 Encounters...")
    encounters_data = []
    
    # We need lists of valid IDs to link keys correctly
    patient_ids = list(range(1, 9001))
    provider_ids = list(range(1, 51))
    
    for _ in range(30000):
        pid = random.choice(patient_ids)
        prov_id = random.choice(provider_ids)
        etype = random.choice(['Outpatient', 'Outpatient', 'Outpatient', 'Inpatient', 'ER']) # Weighted distribution
        
        visit_date = fake.date_time_between(start_date='-2y', end_date='now')
        if etype == 'Inpatient':
            discharge = visit_date + timedelta(days=random.randint(1, 10))
        elif etype == 'ER':
            discharge = visit_date + timedelta(hours=random.randint(2, 12))
        else:
            discharge = visit_date + timedelta(minutes=random.randint(15, 60))
            
        dept_id = random.randint(1, len(departments))
        encounters_data.append((pid, prov_id, etype, visit_date, discharge, dept_id))

    for i in range(0, len(encounters_data), batch_size):
        batch = encounters_data[i:i+batch_size]
        cursor.executemany("INSERT INTO encounters (patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id) VALUES (%s, %s, %s, %s, %s, %s)", batch)
    conn.commit()

    # 6. Junction Tables & Billing
    logging.info("Generating Clinical & Billing Data...")
    
    # Fetch encounter details to generate linked data
    cursor.execute("SELECT encounter_id, encounter_date, encounter_type FROM encounters")
    all_encounters = cursor.fetchall()
    
    enc_diag_data = []
    enc_proc_data = []
    billing_data = []
    
    diag_ids = list(range(1, len(diagnoses) + 1))
    proc_ids = list(range(1, len(procedures) + 1))
    
    for enc_id, enc_date, enc_type in all_encounters:
        # Generate 1-3 Diagnoses
        num_diags = random.randint(1, 3)
        selected_diags = random.sample(diag_ids, num_diags)
        for i, did in enumerate(selected_diags):
            enc_diag_data.append((enc_id, did, i+1))
            
        # Generate 1-2 Procedures
        num_procs = random.randint(1, 2)
        selected_procs = random.sample(proc_ids, num_procs)
        for pid in selected_procs:
            enc_proc_data.append((enc_id, pid, enc_date.date()))
            
        # Generate Billing Record
        amount = random.uniform(100, 5000)
        if enc_type == 'Inpatient': amount *= 5
        allowed = amount * random.uniform(0.6, 0.9)
        claim_date = enc_date.date() + timedelta(days=random.randint(1, 30))
        status = random.choice(['Paid', 'Paid', 'Pending', 'Denied'])
        
        billing_data.append((enc_id, round(amount, 2), round(allowed, 2), claim_date, status))

    # Bulk Insert clinical/billing
    for i in range(0, len(enc_diag_data), batch_size):
        cursor.executemany("INSERT INTO encounter_diagnoses (encounter_id, diagnosis_id, diagnosis_sequence) VALUES (%s, %s, %s)", enc_diag_data[i:i+batch_size])
        
    for i in range(0, len(enc_proc_data), batch_size):
        cursor.executemany("INSERT INTO encounter_procedures (encounter_id, procedure_id, procedure_date) VALUES (%s, %s, %s)", enc_proc_data[i:i+batch_size])

    for i in range(0, len(billing_data), batch_size):
        cursor.executemany("INSERT INTO billing (encounter_id, claim_amount, allowed_amount, claim_date, claim_status) VALUES (%s, %s, %s, %s, %s)", billing_data[i:i+batch_size])
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data generation complete!")

if __name__ == "__main__":
    generate_data()
