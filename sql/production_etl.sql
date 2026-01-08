-- Production ETL Script for Medical Data Warehouse
-- ------------------------------------------------
-- Optimized for Performance using Set-Based SQL Operations

USE medical_dw;

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE dim_patient;

INSERT INTO
    dim_patient (
        patient_id,
        first_name,
        last_name,
        dob,
        current_age,
        gender,
        mrn
    )
SELECT
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    TIMESTAMPDIFF(
        YEAR,
        date_of_birth,
        CURDATE()
    ),
    gender,
    mrn
FROM medical_oltp.patients;

-- dim_specialty
TRUNCATE TABLE dim_specialty;

INSERT INTO
    dim_specialty (
        specialty_id,
        specialty_name,
        specialty_code
    )
SELECT
    specialty_id,
    specialty_name,
    specialty_code
FROM medical_oltp.specialties;

-- dim_department
TRUNCATE TABLE dim_department;

INSERT INTO
    dim_department (
        department_id,
        department_name
    )
SELECT department_id, department_name
FROM medical_oltp.departments;

-- dim_encounter_type
TRUNCATE TABLE dim_encounter_type;

INSERT INTO
    dim_encounter_type (encounter_type_name)
SELECT DISTINCT
    encounter_type
FROM medical_oltp.encounters;

-- dim_diagnosis
TRUNCATE TABLE dim_diagnosis;

INSERT INTO
    dim_diagnosis (
        diagnosis_id,
        icd10_code,
        icd10_description
    )
SELECT
    diagnosis_id,
    icd10_code,
    icd10_description
FROM medical_oltp.diagnoses;

-- dim_procedure
TRUNCATE TABLE dim_procedure;

INSERT INTO
    dim_procedure (
        procedure_id,
        cpt_code,
        cpt_description
    )
SELECT
    procedure_id,
    cpt_code,
    cpt_description
FROM medical_oltp.procedures;

-- dim_provider (Denormalized)
TRUNCATE TABLE dim_provider;

INSERT INTO
    dim_provider (
        provider_id,
        provider_name,
        credential,
        specialty_name,
        department_name
    )
SELECT p.provider_id, CONCAT(
        p.first_name, ' ', p.last_name
    ), p.credential, s.specialty_name, d.department_name
FROM medical_oltp.providers p
    JOIN medical_oltp.specialties s ON p.specialty_id = s.specialty_id
    JOIN medical_oltp.departments d ON p.department_id = d.department_id;

-- 3. Populate Fact Table
-- ----------------------
TRUNCATE TABLE fact_encounters;

INSERT INTO
    fact_encounters (
        encounter_id,
        patient_key,
        provider_key,
        date_key,
        specialty_key,
        department_key,
        encounter_type_key,
        length_of_stay,
        total_claim_amount,
        total_allowed_amount,
        diagnosis_count,
        procedure_count,
        is_inpatient_flag,
        encounter_date,
        discharge_date,
        claim_date
    )
SELECT
    e.encounter_id,
    d_pat.patient_key,
    d_prov.provider_key,
    CAST(
        DATE_FORMAT(e.encounter_date, '%Y%m%d') AS UNSIGNED
    ) as date_key,
    d_spec.specialty_key,
    d_dept.department_key,
    d_type.encounter_type_key,
    CASE
        WHEN e.discharge_date IS NULL THEN 0
        ELSE ROUND(
            TIMESTAMPDIFF(
                SECOND,
                e.encounter_date,
                e.discharge_date
            ) / 86400.0
        )
    END as length_of_stay,
    COALESCE(b.total_claim, 0),
    COALESCE(b.total_allowed, 0),
    (
        SELECT COUNT(*)
        FROM medical_oltp.encounter_diagnoses ed
        WHERE
            ed.encounter_id = e.encounter_id
    ),
    (
        SELECT COUNT(*)
        FROM medical_oltp.encounter_procedures ep
        WHERE
            ep.encounter_id = e.encounter_id
    ),
    CASE
        WHEN e.encounter_type = 'Inpatient' THEN 1
        ELSE 0
    END,
    e.encounter_date,
    e.discharge_date,
    b.claim_date
FROM
    medical_oltp.encounters e
    LEFT JOIN (
        SELECT
            encounter_id,
            MAX(claim_date) as claim_date, -- Use MAX effectively for 1:1
            SUM(claim_amount) as total_claim,
            SUM(allowed_amount) as total_allowed
        FROM medical_oltp.billing
        WHERE
            claim_status = 'Paid'
        GROUP BY
            encounter_id
    ) b ON e.encounter_id = b.encounter_id
    JOIN dim_patient d_pat ON e.patient_id = d_pat.patient_id
    JOIN dim_provider d_prov ON e.provider_id = d_prov.provider_id
    JOIN medical_oltp.providers p_source ON e.provider_id = p_source.provider_id
    JOIN dim_specialty d_spec ON p_source.specialty_id = d_spec.specialty_id
    JOIN dim_department d_dept ON e.department_id = d_dept.department_id
    JOIN dim_encounter_type d_type ON e.encounter_type = d_type.encounter_type_name;

-- 4. Populate Bridge Tables
-- -------------------------
TRUNCATE TABLE bridge_encounter_diagnoses;

INSERT INTO
    bridge_encounter_diagnoses (
        encounter_key,
        diagnosis_key,
        diagnosis_sequence
    )
SELECT fe.encounter_key, dd.diagnosis_key, ed.diagnosis_sequence
FROM
    medical_oltp.encounter_diagnoses ed
    JOIN fact_encounters fe ON ed.encounter_id = fe.encounter_id
    JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id;

TRUNCATE TABLE bridge_encounter_procedures;

INSERT INTO
    bridge_encounter_procedures (
        encounter_key,
        procedure_key,
        procedure_date_key
    )
SELECT fe.encounter_key, dp.procedure_key, CAST(
        DATE_FORMAT(ep.procedure_date, '%Y%m%d') AS UNSIGNED
    )
FROM
    medical_oltp.encounter_procedures ep
    JOIN fact_encounters fe ON ep.encounter_id = fe.encounter_id
    JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id;

-- Done!
SET FOREIGN_KEY_CHECKS = 1;

SELECT 'ETL Completed Successfully' as Status;