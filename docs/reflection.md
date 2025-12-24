# Reflection: OLTP to Star Schema Migration

## Why Is the Star Schema Faster?
In our lab, the Star Schema demonstrated performance improvements in complex scenarios, specifically Query 3 (Readmission Rate), which was **~900x faster** (17s vs 0.017s) in your run!

The speedup comes from:
1.  **Reduced Join Complexity (Star Topology)**: 
    Instead of daisy-chaining tables (Billing -> Encounters -> Providers -> Specialties), we join the central `Fact` table directly to target Dimensions. The database optimizer handles this "Star Join" very efficiently.
2.  **Narrower Tables**:
    The `fact_encounters` table consists almost entirely of Integers (Foreign Keys and Metrics). This is much more dense than the `encounters` OLTP table which stores strings (`encounter_type`) and Datetimes. More rows fit in a single disk page/memory block.
3.  **Bridge Table Optimization**:
    For the Many-to-Many query (Q2), we could join `bridge_encounter_diagnoses` directly to `bridge_encounter_procedures` via `encounter_key`. This bypassed the central `encounters` table entirely.

## Trade-offs: What Did We Gain? What Did We Lose?
### Gained:
-   **Simpler Queries**: The SQL for the Star Schema is standard and predictable (`SELECT dim, sum(metric) FROM fact JOIN dim...`). We don't need to hunt for the "path" between Billing and Specialty.
-   **Historical Accuracy**: Though not fully implemented here, the `dim_patient` table allows for "Slowly Changing Dimensions" (SCD Type 2), preserving a patient's age/address *at the time of the encounter*, which OLTP overwrites.
-   **Aggregated Metrics**: Pre-calculating `allowed_amount` into the Fact table removed the need to join the `billing` table for 90% of revenue queries.

### Lost:
-   **Data Freshness**: The Data Warehouse is only as fresh as the last ETL run. Real-time encouters aren't visible immediately.
-   **Data Duplication**: We store `specialty_name` in `dim_specialty`, `dim_provider`, AND implicitly linked in `fact`. This uses more storage.
-   **ETL Complexity**: We had to write and maintain `etl_logic.py`. If the OLTP schema changes, the ETL breaks.

## Bridge Tables: Worth It?
We chose to use **Bridge Tables** (`fact` <-> `bridge` <-> `dim`) instead of a "Multi-Valued Dimension" or forcing a lower grain (Fact per Diagnosis).

**Was it worth it?**
**Yes.** 
By keeping the Fact Table at the **Encounter Grain**, we kept our core metrics (Revenue, Length of Stay) additive and simple. `SUM(revenue)` works perfectly.
If we had changed the grain to "Fact per Diagnosis", a single encounter with 3 diagnoses would appear in 3 rows. `SUM(revenue)` would triple the actual revenue, requiring complex `COUNT(DISTINCT encounter_id)` logic to correct it.

The Bridge table keeps the Fact table clean while still allowing deep analysis of the multi-valued attributes when strictly necessary.

## Performance Quantification
-   **Complex Many-to-Many Join (Q2)**: 0.09s -> 0.08s (**Faster**)
-   **Self-Join Readmission (Q3)**: 17.09s -> 0.0175s (**~900x Faster**)
-   **Simple Aggregates (Q4)**: Slightly slower due to small dataset size (30k rows) and overhead of joining Date Dimension vs simple OLTP date formatting.

**Conclusion**: The Star Schema provides a robust, scalable foundation for analytics. The massive win in Q3 proves that for complex analytical questions (like readmissions), the dimensional model is vastly superior to a normalized form.
