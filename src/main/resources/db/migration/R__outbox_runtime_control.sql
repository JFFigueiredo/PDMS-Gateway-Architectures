-- Historical repeatable retained so existing Flyway history remains resolvable.
-- Runtime permission now lives in each PDMS process; no control/audit tables are created.
-- Existing historical tables are deliberately left untouched.
SELECT 1;
