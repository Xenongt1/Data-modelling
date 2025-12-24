-- Part 3: Star Schema DDL

DROP DATABASE IF EXISTS medical_dw;

CREATE DATABASE medical_dw;

USE medical_dw;

-- Dimensions
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    full_date DATE,
    year INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    day_of_week INT,
    day_name VARCHAR(20)
) ENGINE = InnoDB;

CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT, -- Natural Key
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    current_age INT,
    gender CHAR(1),
    mrn VARCHAR(20),
    INDEX idx_patient_id (patient_id)
) ENGINE = InnoDB;

CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT, -- Natural Key
    provider_name VARCHAR(200),
    credential VARCHAR(20),
    specialty_name VARCHAR(100), -- Denormalized
    department_name VARCHAR(100), -- Denormalized
    INDEX idx_provider_id (provider_id)
) ENGINE = InnoDB;

CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT, -- Natural Key
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    INDEX idx_specialty_id (specialty_id)
) ENGINE = InnoDB;

CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT, -- Natural Key
    department_name VARCHAR(100),
    INDEX idx_dept_id (department_id)
) ENGINE = InnoDB;

CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_name VARCHAR(50) UNIQUE
) ENGINE = InnoDB;

CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT, -- Natural Key
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200),
    INDEX idx_diag_id (diagnosis_id)
) ENGINE = InnoDB;

CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT, -- Natural Key
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200),
    INDEX idx_proc_id (procedure_id)
) ENGINE = InnoDB;

-- Fact Table
CREATE TABLE fact_encounters (
    encounter_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id INT, -- Natural Key

-- Foreign Keys to Dimensions
patient_key INT,
provider_key INT,
date_key INT,
specialty_key INT, -- Redundant but useful for high-performance specialty aggregation
department_key INT,
encounter_type_key INT,

-- Metrics
length_of_stay_days DECIMAL(10,2),
    claim_amount DECIMAL(12,2),
    allowed_amount DECIMAL(12,2),
    is_inpatient_flag TINYINT,
    
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    
    INDEX idx_date (date_key),
    INDEX idx_specialty (specialty_key)
) ENGINE=InnoDB;

-- Bridge Tables for Many-to-Many
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    diagnosis_key INT,
    diagnosis_sequence INT,
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters (encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis (diagnosis_key),
    INDEX idx_enc_diag (encounter_key, diagnosis_key)
) ENGINE = InnoDB;

CREATE TABLE bridge_encounter_procedures (
    bridge_id INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    procedure_key INT,
    procedure_date_key INT,
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters (encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure (procedure_key),
    FOREIGN KEY (procedure_date_key) REFERENCES dim_date (date_key),
    INDEX idx_enc_proc (encounter_key, procedure_key)
) ENGINE = InnoDB;