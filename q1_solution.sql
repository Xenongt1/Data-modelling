-- Question 1: Monthly Encounters by Specialty
-- Goal: For each month and specialty, show total encounters and unique patients by encounter type.

SELECT
    -- 1. Derive Month from encounter_date
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,

-- 2. Get Specialty Name from specialties table
s.specialty_name,

-- 3. Get Encounter Type from encounters table
e.encounter_type,

-- 4. Calculate Metrics
COUNT(*) AS total_encounters,
COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
    -- Join to Providers to link Encounter -> Provider
    JOIN providers p ON e.provider_id = p.provider_id

-- Join to Specialties to link Provider -> Specialty
JOIN specialties s ON p.specialty_id = s.specialty_id

-- Group by the non-aggregated columns
GROUP BY 1, 2, 3 ORDER BY month, s.specialty_name;