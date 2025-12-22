import os
import sys
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

def check_nulls():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("--- Checking Fact Encounters ---")
    columns = [
        "date_key", "patient_key", "provider_key", 
        "department_key", "encounter_type_key",
        "total_claim_amount", "total_allowed_amount", 
        "diagnosis_count", "procedure_count", 
        "is_readmission", "length_of_stay_hours"
    ]
    
    for col in columns:
        cursor.execute(f"SELECT COUNT(*) FROM fact_encounters WHERE {col} IS NULL")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"FAILED: {col} has {count} NULL values.")
        else:
            print(f"OK: {col} has no NULLs.")

    print("\n--- Checking Sample Data from Fact Encounters ---")
    cursor.execute("SELECT * FROM fact_encounters LIMIT 5")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    conn.close()

if __name__ == "__main__":
    check_nulls()
