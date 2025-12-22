# Reflection: OLTP to Star Schema

## Why Is the Star Schema Faster?

1.  **Fewer Joins**: By denormalizing `specialties` into `dim_provider` and `billing` metrics into `fact_encounters`, we reduced the number of joins required for common queries. For example, Q4 went from 3 joins to 2 joins (and avoided the large Billing table).
2.  **Pre-Aggregation**: The most dramatic improvement was Q3 (Readmission Rate). In the OLTP model, this required a costly self-join to look up history for every patient at query time. In the Star Schema, we calculated `is_readmission` during the ETL process. The database only has to sum a column of 1s and 0s, changing an exponential complexity operation into a linear scan.
3.  **Surrogate Keys**: Joining on Integer `date_key`, `provider_key`, etc., is typically faster and uses less index space than joining on string based codes or composite keys, although in this small dataset the difference is negligible.

## Trade-offs: What Did You Gain? What Did You Lose?

### Gained
-   **Query Simplicity**: The SQL for the Star Schema is often less complex. `SUM(is_readmission)` is much easier to understand than a 6-line self-join with date arithmetic.
-   **Performance**: Analytical queries dealing with aggregates are significantly faster.
-   **Historical Accuracy**: Although not fully implemented here (SCD Type 2), a Star Schema allows tracking changes over time (e.g., a provider changing specialties) which OLTP overwrites.

### Lost / Costs
-   **Data Duplication**: `specialty_name` is repeated for every provider. If a specialty name changes, we update many provider rows (or the dimension). In OLTP, we update one row. This increases storage (usually cheap) but complicates updates.
-   **ETL Complexity**: We had to write a Python script to move data. This introduces a delay; data is not "real-time". The `is_readmission` logic in ETL is complex code to maintain.

### Was it worth it?
For an analytics use case? **Absolutely.** The readmission query alone justifies the effort. Analysts would timeout running that on a live production DB.

## Bridge Tables: Worth It?
We used `bridge_encounter_diagnoses` to handle incidents where one encounter has multiple diagnoses.

**Why not denormalize into Fact?**
If we put `diagnosis_key` in the Fact table, we'd need one row per diagnosis. An encounter with 5 diagnoses would create 5 Fact rows. If we sum `billing_amount`, we'd sum it 5 times! Incorrect data.

**Why not columns diag1, diag2...?**
Inflexible. What if a patient has 20 diagnoses?

**Trade-off**: Querying requires an extra join (Fact -> Bridge -> Dim).
**Decision**: It is worth it to preserve the correct grain of the Fact table (One Row = One Encounter).

## Performance Quantification

| Query | OLTP Time | Star Schema Time | Improvement |
| :--- | :--- | :--- | :--- |
| Q1 (Encounters) | 0.0022s | 0.0022s | 1x (Neutral) |
| Q2 (Diagnosis/Proc) | 0.0015s | 0.0015s | 1x (Neutral) |
| Q3 (Readmissions) | 0.0013s | 0.0005s | **2.6x Faster** |
| Q4 (Revenue) | 0.0005s | 0.0003s | **1.6x Faster** |

**Main Driver**: Moving logic from "Query Time" (Self-joins) to "Load Time" (ETL Pre-calculation).
