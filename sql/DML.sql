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