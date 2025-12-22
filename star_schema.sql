-- Part 3: Star Schema DDL

-- Drop tables if they exist
DROP TABLE IF EXISTS bridge_encounter_procedures;

DROP TABLE IF EXISTS bridge_encounter_diagnoses;

DROP TABLE IF EXISTS fact_encounters;

DROP TABLE IF EXISTS dim_procedure;

DROP TABLE IF EXISTS dim_diagnosis;

DROP TABLE IF EXISTS dim_encounter_type;

DROP TABLE IF EXISTS dim_department;

DROP TABLE IF EXISTS dim_provider;

DROP TABLE IF EXISTS dim_patient;

DROP TABLE IF EXISTS dim_date;

-- 1. Date Dimension
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- Format YYYYMMDD
    full_date DATE,
    year INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    is_weekend BOOLEAN
);

-- 2. Patient Dimension
CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT, -- Natural Key
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    date_of_birth DATE,
    gender CHAR(1),
    mrn VARCHAR(20),
    current_age INT,
    INDEX idx_patient_id (patient_id)
);

-- 3. Provider Dimension
CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT, -- Natural Key
    provider_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_name VARCHAR(100), -- Denormalized from specialties
    INDEX idx_provider_id (provider_id)
);

-- 4. Department Dimension
CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT, -- Natural Key
    department_name VARCHAR(100),
    floor INT,
    capacity INT,
    INDEX idx_department_id (department_id)
);

-- 5. Encounter Type Dimension
CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_name VARCHAR(50)
);

-- 6. Diagnosis Dimension
CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT, -- Natural Key
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200),
    INDEX idx_diagnosis_id (diagnosis_id)
);

-- 7. Procedure Dimension (Renamed to singular for consistency)
CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT, -- Natural Key
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200),
    INDEX idx_procedure_id (procedure_id)
);

-- 8. Fact Encounters
CREATE TABLE fact_encounters (
    fact_encounter_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id INT, -- Natural Key, useful for back-tracing

-- Foreign Keys to Dimensions
date_key INT,
patient_key INT,
provider_key INT,
department_key INT,
encounter_type_key INT,

-- Metrics (Pre-aggregated)
total_claim_amount DECIMAL(12, 2) DEFAULT 0,
total_allowed_amount DECIMAL(12, 2) DEFAULT 0,
diagnosis_count INT DEFAULT 0,
procedure_count INT DEFAULT 0,

-- Calculated Flags
is_readmission BOOLEAN DEFAULT 0,

-- Time Calculations
length_of_stay_hours DECIMAL(10, 2),
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_provider (provider_key)
);

-- 9. Bridge Tables
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    fact_encounter_id INT,
    diagnosis_key INT,
    diagnosis_sequence INT,
    FOREIGN KEY (fact_encounter_id) REFERENCES fact_encounters (fact_encounter_id),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis (diagnosis_key)
);

CREATE TABLE bridge_encounter_procedures (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    fact_encounter_id INT,
    procedure_key INT,
    FOREIGN KEY (fact_encounter_id) REFERENCES fact_encounters (fact_encounter_id),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure (procedure_key)
);