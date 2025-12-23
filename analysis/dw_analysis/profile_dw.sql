-- Performance Profiling for Star Schema Queries
-- Usage: SOURCE profile_dw.sql;

SET PROFILING = 1;

-- Q1 (Star)
SELECT "Running Q1 (Star)..." AS status;

SELECT
    d.year,
    d.month,
    p.specialty_name,
    et.encounter_type_name,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM
    fact_encounters f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_provider p ON f.provider_key = p.provider_key
    JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY
    1,
    2,
    3,
    4;

-- Q2 (Star)
SELECT "Running Q2 (Star)..." AS status;

SELECT diag.icd10_code, proc.cpt_code, COUNT(*) AS encounter_count
FROM
    fact_encounters f
    JOIN bridge_encounter_diagnoses bd ON f.fact_encounter_id = bd.fact_encounter_id
    JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
    JOIN bridge_encounter_procedures bp ON f.fact_encounter_id = bp.fact_encounter_id
    JOIN dim_procedure proc ON bp.procedure_key = proc.procedure_key
GROUP BY
    1,
    2
ORDER BY 3 DESC
LIMIT 10;

-- Q3 (Star)
SELECT "Running Q3 (Star)..." AS status;

SELECT
    p.specialty_name,
    COUNT(
        DISTINCT CASE
            WHEN et.encounter_type_name = 'Inpatient' THEN f.encounter_id
        END
    ) AS total_inpatient_discharges,
    SUM(f.is_readmission) AS readmissions,
    SUM(f.is_readmission) / COUNT(
        DISTINCT CASE
            WHEN et.encounter_type_name = 'Inpatient' THEN f.encounter_id
        END
    ) * 100 as rate
FROM
    fact_encounters f
    JOIN dim_provider p ON f.provider_key = p.provider_key
    JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY
    1;

-- Q4 (Star)
SELECT "Running Q4 (Star)..." AS status;

SELECT d.year, d.month, p.specialty_name, SUM(f.total_allowed_amount) AS total_revenue
FROM
    fact_encounters f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_provider p ON f.provider_key = p.provider_key
GROUP BY
    1,
    2,
    3;

-- Show Results
SHOW PROFILES;