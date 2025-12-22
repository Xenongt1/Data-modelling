-- Question 2: Top Diagnosis-Procedure Pairs
-- Goal: Find the most common combinations of a Diagnosis and a Procedure happening in the same encounter.

SELECT
    -- 1. Diagnosis Info
    d.icd10_code, d.icd10_description,

-- 2. Procedure Info
p.cpt_code, p.cpt_description,

-- 3. Count how often they appear together
COUNT(*) AS pair_count FROM encounters e

-- Join chain to get Diagnoses for the encounter
JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id

-- Join chain to get Procedures for the encounter
JOIN encounter_procedures ep ON e.encounter_id = ep.encounter_id
JOIN procedures p ON ep.procedure_id = p.procedure_id

-- Group by the unique pair
GROUP BY
    d.icd10_code,
    d.icd10_description,
    p.cpt_code,
    p.cpt_description

-- Show most frequent first
ORDER BY pair_count DESC LIMIT 10;