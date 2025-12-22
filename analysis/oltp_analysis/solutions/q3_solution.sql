-- Question 3: 30-Day Readmission Rate
-- Goal: Find which specialty has the highest rate of patients returning within 30 days of an inpatient discharge.

SELECT s.specialty_name,

-- 1. Count original discharges (Denominator)
COUNT(DISTINCT e1.encounter_id) AS total_inpatient_discharges,

-- 2. Count readmissions (Numerator)
COUNT(DISTINCT e2.encounter_id) AS readmissions_within_30_days,

-- 3. Calculate Rate (Readmissions / Discharges * 100)
COUNT(DISTINCT e2.encounter_id) / COUNT(DISTINCT e1.encounter_id) * 100 AS readmission_rate
FROM encounters e1 -- The "Initial" Visit

-- Standard Joins to get Specialty
JOIN providers p ON e1.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id

-- The Self-Join to find the "Return" Visit
LEFT JOIN encounters e2 ON e1.patient_id = e2.patient_id -- Same patient
AND e2.encounter_date > e1.discharge_date -- Visit happens AFTER discharge
AND e2.encounter_date <= DATE_ADD(
    e1.discharge_date,
    INTERVAL 30 DAY
) -- But within 30 days
WHERE
    e1.encounter_type = 'Inpatient' -- Only care about Inpatient discharges
GROUP BY
    s.specialty_name
ORDER BY readmission_rate DESC;