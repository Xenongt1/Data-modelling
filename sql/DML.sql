USE medical_oltp;

EXPLAIN ANALYZE
SELECT
    DATE_FORMAT(encounter_date, '%Y-%m') as month,
    s.specialty_name,
    e.encounter_type,
    COUNT(*) as total_encounters,
    COUNT(DISTINCT e.patient_id) as unique_patients
FROM
    encounters e
    JOIN providers p ON e.provider_id = p.provider_id
    JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY
    1,
    2,
    3
ORDER BY 1, 2 DESC;

SELECT * FROM encounter_diagnoses;

SELECT * FROM encounter_procedures;

SELECT * FROM diagnoses;

SELECT * FROM procedures;

EXPLAIN ANALYZE
SELECT
    d.icd10_code,
    p.cpt_code,
    COUNT(e.encounter_id) as Number_of_visits_with_this_pair
FROM
    encounters e
    JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
    JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
    JOIN encounter_procedures ep ON e.encounter_id = ep.encounter_id
    JOIN procedures p ON ep.procedure_id = p.procedure_id
GROUP BY
    1,
    2
ORDER BY 3 DESC

EXPLAIN ANALYZE
SELECT
    s.specialty_name,
    COUNT(DISTINCT ini.encounter_id) as index_admissions,
    COUNT(DISTINCT readmit.encounter_id) as readmissions,
    (
        COUNT(DISTINCT readmit.encounter_id) / COUNT(DISTINCT ini.encounter_id)
    ) * 100 as readmission_rate
FROM
    encounters ini
    JOIN providers p ON ini.provider_id = p.provider_id
    JOIN specialties s ON p.specialty_id = s.specialty_id
    LEFT JOIN encounters readmit ON ini.patient_id = readmit.patient_id
    AND readmit.encounter_date > ini.discharge_date
    AND readmit.encounter_date <= DATE_ADD(
        ini.discharge_date,
        INTERVAL 30 DAY
    )
WHERE
    ini.encounter_type = 'Inpatient'
GROUP BY
    1
ORDER BY 4 DESC

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

SELECT * FROM billing;