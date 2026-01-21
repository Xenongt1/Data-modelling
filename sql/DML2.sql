USE medical_dw;

-- ==============================================================================
-- STAR SCHEMA OPTIMIZED QUERIES (DML2.sql)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- QUESTION 1: Monthly Encounters by Specialty
-- Strategy: Use denormalized 'encounter_date' to avoid joining dim_date
-- ------------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT
    DATE_FORMAT(f.encounter_date, '%M') as month_name,
    s.specialty_name,
    t.encounter_type_name,
    COUNT(f.encounter_key) as total_encounters,
    COUNT(DISTINCT f.patient_key) as unique_patients
FROM
    fact_encounters f
    JOIN dim_specialty s ON f.specialty_key = s.specialty_key
    JOIN dim_encounter_type t ON f.encounter_type_key = t.encounter_type_key
GROUP BY
    DATE_FORMAT(f.encounter_date, '%Y'),
    DATE_FORMAT(f.encounter_date, '%m'),
    month_name,
    s.specialty_name,
    t.encounter_type_name
ORDER BY DATE_FORMAT(f.encounter_date, '%Y'), DATE_FORMAT(f.encounter_date, '%m');

-- ------------------------------------------------------------------------------
-- QUESTION 2: Top Diagnosis-Procedure Overlaps
-- Strategy: Join Bridge tables directly on encounter_key
-- ------------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT dd.icd10_code, dp.cpt_code, COUNT(bd.encounter_key) as overlap_count
FROM
    bridge_encounter_diagnoses bd
    JOIN bridge_encounter_procedures bp ON bd.encounter_key = bp.encounter_key
    JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
    JOIN dim_procedure dp ON bp.procedure_key = dp.procedure_key
GROUP BY
    1,
    2
ORDER BY 3 DESC;

-- ------------------------------------------------------------------------------
-- QUESTION 3: 30-Day Readmission Rate
-- Strategy: Smart Keys (encounter_date on Fact) avoids Date Dimension self-join
-- ------------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT
    s.specialty_name,
    COUNT(DISTINCT ini.encounter_key) as index_admissions,
    COUNT(
        DISTINCT readmit.encounter_key
    ) as readmissions,
    (
        COUNT(
            DISTINCT readmit.encounter_key
        ) / COUNT(DISTINCT ini.encounter_key)
    ) * 100 as readmission_rate
FROM
    fact_encounters ini
    JOIN dim_specialty s ON ini.specialty_key = s.specialty_key
    JOIN dim_encounter_type t ON ini.encounter_type_key = t.encounter_type_key
    LEFT JOIN fact_encounters readmit ON ini.patient_key = readmit.patient_key
    AND readmit.encounter_date > ini.discharge_date
    AND readmit.encounter_date <= DATE_ADD(
        ini.discharge_date,
        INTERVAL 30 DAY
    )
WHERE
    t.encounter_type_name = 'Inpatient'
GROUP BY
    1
ORDER BY 4 DESC;

-- ------------------------------------------------------------------------------
-- QUESTION 4: Revenue by Specialty & Month (Cash Basis)
-- Strategy: Use 'claim_date' (Smart Key) + Pre-aggregated 'total_allowed_amount'
-- Filter: claim_date IS NOT NULL (implies 'Paid' status from ETL)
-- ------------------------------------------------------------------------------

EXPLAIN ANALYZE 
SELECT DATE_FORMAT(f.claim_date, '%M') as month_name, s.specialty_name, SUM(f.total_allowed_amount) as total_revenue
FROM
    fact_encounters f
    JOIN dim_specialty s ON f.specialty_key = s.specialty_key
WHERE
    f.claim_date IS NOT NULL
GROUP BY
    DATE_FORMAT(f.claim_date, '%Y'),
    DATE_FORMAT(f.claim_date, '%m'),
    month_name,
    s.specialty_name
ORDER BY DATE_FORMAT(f.claim_date, '%Y'), DATE_FORMAT(f.claim_date, '%m'), total_revenue DESC;

SELECT encounter_id, claim_date, is_inpatient_flag FROM fact_encounters
WHERE encounter_id = 4;

SELECT * FROM dim_date;

