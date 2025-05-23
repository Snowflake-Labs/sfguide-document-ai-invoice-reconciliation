-- Set the context to the correct database and schema (optional if already set in your session)
USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_ITEM_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
  current_run_timestamp TIMESTAMP_NTZ; -- Use consistent timestamp for the run
BEGIN
    current_run_timestamp := CURRENT_TIMESTAMP();
MERGE INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS AS target
USING(
    WITH db_items AS (
    SELECT
        invoice_id,
        product_name,
        quantity,
        unit_price,
        total_price,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_id, product_name
            ORDER BY quantity, unit_price, total_price
        ) as rn_occurrence
        FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS a
    ),
    docai_items AS (
    SELECT
        invoice_id,
        product_name,
        quantity,
        unit_price,
        total_price,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_id, product_name
            ORDER BY quantity, unit_price, total_price 
        ) as rn_occurrence
    FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS b
    ),

    join_table AS (SELECT
    COALESCE(a.invoice_id, b.invoice_id) as Reconciled_invoice_id,
    COALESCE(a.product_name, b.product_name) as Reconciled_product_name,
    COALESCE(a.rn_occurrence, b.rn_occurrence) as Product_Occurrence_Num,

    -- Data from Table A
    a.quantity AS Quantity_A,
    a.unit_price AS UnitPrice_A,
    a.total_price AS TotalPrice_A,

    -- Data from Table B
    b.quantity AS Quantity_B,
    b.unit_price AS UnitPrice_B,
    b.total_price AS TotalPrice_B,

    -- Reconciliation Status and Discrepancies
    CASE
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NOT NULL THEN 'Matched Line Item Occurrence'
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NULL THEN 'In Table A Only'
        WHEN a.invoice_id IS NULL AND b.invoice_id IS NOT NULL THEN 'In Table B Only'
    END AS Reconciliation_Status,

    CASE
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NOT NULL THEN
            TRIM(
                COALESCE(IFF(a.quantity <> b.quantity, 'Qty_Diff(' || a.quantity::VARCHAR || ' vs ' || b.quantity::VARCHAR || '); ', ''), '') ||
                COALESCE(IFF(a.unit_price <> b.unit_price, 'Unit_Price_Diff(' || a.unit_price::VARCHAR || ' vs ' || b.unit_price::VARCHAR || '); ', ''), '') ||
                COALESCE(IFF(a.total_price <> b.total_price, 'Total_Price_Diff(' || a.total_price::VARCHAR || ' vs ' || b.total_price::VARCHAR || '); ', ''), '')
                -- Add more detailed discrepancy checks as needed
            )
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NULL THEN 'In Table A Only'
        WHEN a.invoice_id IS NULL AND b.invoice_id IS NOT NULL THEN 'In Table B Only'
        ELSE NULL
    END AS Discrepancies
FROM db_items a
FULL OUTER JOIN docai_items b
    ON a.invoice_id = b.invoice_id
    AND a.product_name = b.product_name
    AND a.rn_occurrence = b.rn_occurrence
    ),

ReconciliationSource AS (
SELECT
    Reconciled_invoice_id AS invoice_id,
    LISTAGG(
        DISTINCT CASE
            WHEN discrepancies IS NOT NULL AND discrepancies <> '' THEN Reconciled_product_name || ': ' || discrepancies
            ELSE NULL
        END,
        '; '
    ) WITHIN GROUP (ORDER BY -- Corrected ORDER BY clause
                        CASE
                            WHEN discrepancies IS NOT NULL AND discrepancies <> '' THEN Reconciled_product_name || ': ' || discrepancies
                            ELSE NULL
                        END
                   ) AS item_mismatch_details,
    CASE
        WHEN item_mismatch_details = '' THEN 'Auto-reconciled'
        ELSE 'Pending Review'
    END AS review_status,
    :current_run_timestamp AS last_reconciled_timestamp,
    NULL AS reviewed_by,
    NULL AS reviewed_timestamp,
    NULL as notes
    
FROM
    join_table 
GROUP BY
    Reconciled_invoice_id
ORDER BY
    Reconciled_invoice_id
)
    -- Select final source for merge
    SELECT * FROM ReconciliationSource
  ) AS source
  ON target.invoice_id = source.invoice_id

  -- Action when a record for this item instance already exists
  WHEN MATCHED THEN UPDATE SET
    target.item_mismatch_details = source.item_mismatch_details, -- Update details if discrepancy changes or becomes null if matched
    -- Set review status based on whether it's a discrepancy or auto-reconciled
    target.review_status = CASE
                              WHEN source.review_status = 'Auto-reconciled' THEN source.review_status
                              WHEN target.review_status = 'Reviewed' THEN target.review_status
                              ELSE 'Pending Review' -- Reset to Pending Review if it becomes/remains a discrepancy
                           END,
    target.last_reconciled_timestamp = :current_run_timestamp, -- Use variable for consistency
    -- Reset review/correction fields only if it's now a discrepancy, keep them if auto-reconciled (though likely null anyway)
    target.reviewed_by = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_by 
                            WHEN source.review_status != 'Auto-reconciled' THEN NULL 
                            ELSE target.reviewed_by END,
    target.reviewed_timestamp = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_timestamp 
                                WHEN source.review_status != 'Auto-reconciled' THEN NULL
                                ELSE target.reviewed_timestamp END,
    target.notes =  CASE WHEN target.review_status = 'Reviewed' THEN target.notes
                    WHEN source.review_status != 'Auto-reconciled' THEN NULL 
                    ELSE target.notes END


  -- Action when a new discrepancy or auto-reconciled item is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    item_mismatch_details, -- Will be NULL for auto-reconciled
    review_status,         -- 'Pending Review' or 'Auto-Reconciled'
    last_reconciled_timestamp
  ) VALUES (
    source.invoice_id,
    source.item_mismatch_details,
    source.review_status,
    :current_run_timestamp  -- Use variable for consistency
  );

SELECT * FROM TRANSACT_ITEMS;

CREATE OR REPLACE TEMPORARY TABLE ReadyForGold AS(
    SELECT 
    *,
    'Auto-reconciled' AS reviewed_by,
    :current_run_timestamp AS reviewed_timestamp
    FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS
    WHERE INVOICE_ID IN (
        SELECT INVOICE_ID
        FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS
        WHERE REVIEW_STATUS = 'Auto-reconciled'
    )
);
    
DELETE FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS
WHERE invoice_id IN (SELECT DISTINCT invoice_id FROM ReadyForGold);

-- Step 2: Insert all the new line items from incoming_data.
INSERT INTO doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS (invoice_id, product_name, quantity, unit_price, total_price, reviewed_by, reviewed_timestamp)
SELECT invoice_id, product_name, quantity, unit_price, total_price, reviewed_by, reviewed_timestamp
FROM ReadyForGold;

  status_message := 'Item reconciliation executed. Discrepancies and auto-reconciled items merged into RECONCILE_RESULTS_ITEMS. Fully auto-reconciled invoices merged into GOLD_INVOICE_ITEMS.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during item reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;


CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_TOTALS_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
  current_run_timestamp TIMESTAMP_NTZ; -- Use consistent timestamp for the run
BEGIN
    current_run_timestamp := CURRENT_TIMESTAMP();
MERGE INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS AS target
USING(
    WITH db_totals AS (
    SELECT
        invoice_id,
        invoice_date,
        subtotal,
        tax,
        total
        FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS a
    ),
    docai_totals AS (
    SELECT
        invoice_id,
        invoice_date,
        subtotal,
        tax,
        total
    FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS b
    ),

    join_table AS (SELECT
    COALESCE(a.invoice_id, b.invoice_id) as Reconciled_invoice_id,
    
    -- Data from Table A
    a.invoice_date AS invoiceDate_A,
    a.subtotal AS subtotal_A,
    a.tax AS UnitPrice_A,
    a.total AS TotalPrice_A,

    -- Data from Table B
    b.invoice_date AS invoiceDate_B,
    b.subtotal AS subtotal_B,
    b.tax AS UnitPrice_B,
    b.total AS TotalPrice_B,

    -- Reconciliation Status and Discrepancies
    CASE
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NOT NULL THEN 'Matched Line Item Occurrence'
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NULL THEN 'In Table A Only'
        WHEN a.invoice_id IS NULL AND b.invoice_id IS NOT NULL THEN 'In Table B Only'
    END AS Reconciliation_Status,

    CASE
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NOT NULL THEN
            TRIM(
                COALESCE(IFF(a.invoice_date <> b.invoice_date, 'date_Diff(' || a.invoice_date::VARCHAR || ' vs ' || b.invoice_date::VARCHAR || '); ', ''), '') ||
                COALESCE(IFF(a.subtotal <> b.subtotal, 'subtotal_Diff(' || a.subtotal::VARCHAR || ' vs ' || b.subtotal::VARCHAR || '); ', ''), '') ||
                COALESCE(IFF(a.tax <> b.tax, 'tax_Diff(' || a.tax::VARCHAR || ' vs ' || b.tax::VARCHAR || '); ', ''), '') ||
                COALESCE(IFF(a.total <> b.total, 'total_Diff(' || a.total::VARCHAR || ' vs ' || b.total::VARCHAR || '); ', ''), '')
                -- Add more detailed discrepancy checks as needed
            )
        WHEN a.invoice_id IS NOT NULL AND b.invoice_id IS NULL THEN 'In Table A Only'
        WHEN a.invoice_id IS NULL AND b.invoice_id IS NOT NULL THEN 'In Table B Only'
        ELSE NULL
    END AS Discrepancies
FROM db_totals a
FULL OUTER JOIN docai_totals b
    ON a.invoice_id = b.invoice_id
    ),

ReconciliationSource AS (
SELECT
    Reconciled_invoice_id AS invoice_id,
    LISTAGG(
        DISTINCT CASE
            WHEN discrepancies IS NOT NULL AND discrepancies <> '' THEN Reconciled_invoice_id || ': ' || discrepancies
            ELSE NULL
        END,
        '; '
    ) WITHIN GROUP (ORDER BY
                        CASE
                            WHEN discrepancies IS NOT NULL AND discrepancies <> '' THEN Reconciled_invoice_id || ': ' || discrepancies
                            ELSE NULL
                        END
                   ) AS item_mismatch_details,
    CASE
        WHEN item_mismatch_details = '' THEN 'Auto-reconciled'
        ELSE 'Pending Review'
    END AS review_status,
    :current_run_timestamp AS last_reconciled_timestamp,
    NULL AS reviewed_by,
    NULL AS reviewed_timestamp,
    NULL as notes
    
FROM
    join_table 
GROUP BY
    Reconciled_invoice_id
ORDER BY
    Reconciled_invoice_id
)
    -- Select final source for merge
    SELECT * FROM ReconciliationSource
  ) AS source
  ON target.invoice_id = source.invoice_id

  -- Action when a record for this item instance already exists
  WHEN MATCHED THEN UPDATE SET
    target.item_mismatch_details = source.item_mismatch_details, -- Update details if discrepancy changes or becomes null if matched
    -- Set review status based on whether it's a discrepancy or auto-reconciled
    target.review_status = CASE
                              WHEN source.review_status = 'Auto-reconciled' THEN source.review_status
                              WHEN target.review_status = 'Reviewed' THEN target.review_status
                              ELSE 'Pending Review' -- Reset to Pending Review if it becomes/remains a discrepancy
                           END,
    target.last_reconciled_timestamp = :current_run_timestamp, -- Use variable for consistency
    -- Reset review/correction fields only if it's now a discrepancy, keep them if auto-reconciled (though likely null anyway)
    target.reviewed_by = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_by 
                            WHEN source.review_status != 'Auto-reconciled' THEN NULL 
                            ELSE target.reviewed_by END,
    target.reviewed_timestamp = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_timestamp 
                                WHEN source.review_status != 'Auto-reconciled' THEN NULL
                                ELSE target.reviewed_timestamp END,
    target.notes =  CASE WHEN target.review_status = 'Reviewed' THEN target.notes
                    WHEN source.review_status != 'Auto-reconciled' THEN NULL 
                    ELSE target.notes END


  -- Action when a new discrepancy or auto-reconciled item is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    item_mismatch_details, -- Will be NULL for auto-reconciled
    review_status,         -- 'Pending Review' or 'Auto-Reconciled'
    last_reconciled_timestamp
  ) VALUES (
    source.invoice_id,
    source.item_mismatch_details,
    source.review_status,
    :current_run_timestamp  -- Use variable for consistency
  );

SELECT * FROM TRANSACT_TOTALS;

CREATE OR REPLACE TEMPORARY TABLE ReadyForGold AS(
    SELECT 
    *,
    'Auto-reconciled' AS reviewed_by,
    :current_run_timestamp AS reviewed_timestamp
    FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS
    WHERE INVOICE_ID IN (
        SELECT INVOICE_ID
        FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS
        WHERE REVIEW_STATUS = 'Auto-reconciled'
    )
);
    
DELETE FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS
WHERE invoice_id IN (SELECT DISTINCT invoice_id FROM ReadyForGold);

-- Step 2: Insert all the new line TOTALS from incoming_data.
INSERT INTO doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS (invoice_id, invoice_date, subtotal, tax, total, reviewed_by, reviewed_timestamp)
SELECT invoice_id, invoice_date, subtotal, tax, total, reviewed_by, reviewed_timestamp
FROM ReadyForGold;

-- Clearing our streams of data
CREATE TEMPORARY TABLE table1 AS SELECT * FROM doc_ai_qs_db.doc_ai_schema.BRONZE_DB_STREAM WHERE 0 = 1;
CREATE TEMPORARY TABLE table2 AS SELECT * FROM doc_ai_qs_db.doc_ai_schema.BRONZE_DOCAI_STREAM WHERE 0 = 1;

  status_message := 'Item reconciliation executed. Discrepancies and auto-reconciled items merged into RECONCILE_RESULTS_TOTALS. Fully auto-reconciled invoices merged into GOLD_INVOICE_TOTALS.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during item reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;

-- Redefine the target table to capture specific column discrepancies
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS (
    invoice_id VARCHAR,
    item_mismatch_details VARCHAR,
    review_status VARCHAR,
    last_reconciled_timestamp TIMESTAMP_NTZ,
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    notes VARCHAR
);

-- Redefine the target table to capture specific column discrepancies
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS (
    invoice_id VARCHAR,
    item_mismatch_details VARCHAR,
    review_status VARCHAR,
    last_reconciled_timestamp TIMESTAMP_NTZ,
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    notes VARCHAR
    
);

 -- Gold table for corrected transaction items
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS (
    invoice_id VARCHAR,
    product_name VARCHAR,
    quantity NUMBER, -- Assuming numeric type
    unit_price DECIMAL(10,2), -- Assuming decimal for currency
    total_price DECIMAL(10,2), -- Assuming decimal for currency
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    notes VARCHAR

);

-- Gold table for corrected transaction totals
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS (
    invoice_id VARCHAR,
    invoice_date DATE, -- Assuming DATE type
    subtotal DECIMAL(10,2), -- Assuming decimal for currency
    tax DECIMAL(10,2), -- Assuming decimal for currency
    total DECIMAL(10,2), -- Assuming decimal for currency
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    notes VARCHAR

);

-- CREATE A STREAM TO MONITOR THE Bronze db table for new items to pass to our reconciliation task
CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DB_STREAM 
ON TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS;

CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DOCAI_STREAM 
ON TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS;

-- CREATE A TASK TO RUN WHEN THE STREAM DETECTS NEW INFO IN OUR MAIN DB TABLE OR DOCAI TABLE
create or replace task doc_ai_qs_db.doc_ai_schema.RECONCILE
	warehouse=doc_ai_qs_wh
	schedule='3 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('BRONZE_DB_STREAM') OR SYSTEM$STREAM_HAS_DATA('BRONZE_DOCAI_STREAM')
	as BEGIN
        CALL SP_RUN_ITEM_RECONCILIATION();
        CALL SP_RUN_TOTALS_RECONCILIATION();
    END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.RECONCILE RESUME;
    


--Kick off our streams + tasks with data entering the original bronze db tables.
-- Example INSERT statement for the first few rows
INSERT INTO doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (invoice_id, product_name, quantity, unit_price, total_price) VALUES
  ('2010', 'Onions (kg)', 4, 4.51, 18.04), 
  ('2010', 'Yogurt (cup)', 5, 1.29, 6.45), 
  ('2010', 'Eggs (dozen)', 5, 1.79, 8.95), 
  ('2010', 'Bread (loaf)', 5, 11.89, 59.45), 
  ('2010', 'Onions (kg)', 2, 4.78, 9.54), -- Intentionally modify unit price 4.62 -> 4.78 and total price 9.24 -> 9.54
  ('2010', 'Eggs (dozen)', 4, 2.72, 10.88), 
  ('2009', 'Onions (kg)', 3, 5.81, 17.43), 
  ('2009', 'Cheese (block)', 5, 16.57, 82.85), 
  ('2009', 'Chicken (kg)', 5, 16.62, 83.10), 
  ('2009', 'Eggs (dozen)', 5, 2.83, 14.15), 
  ('2009', 'Apples (kg)', 3, 5.02, 15.06), 
  ('2009', 'Eggs (dozen)', 4, 2.35, 9.40), 
  ('2009', 'Eggs (dozen)', 4, 2.69, 10.76), 
  ('2008', 'Tomatoes (kg)', 3, 6.21, 18.63), 
  ('2008', 'Rice (kg)', 2, 19.26, 38.52), 
  ('2008', 'Chicken (kg)', 3, 17.05, 51.15), 
  ('2008', 'Rice (kg)', 1, 20.62, 20.62), 
  ('2008', 'Tomatoes (kg)', 1, 6.74, 6.74), 
  ('2007', 'Butter (pack)', 2, 6.90, 13.80), 
  ('2007', 'Bread (loaf)', 4, 11.41, 45.64), 
  ('2007', 'Yogurt (cup)', 5, 1.66, 3.32), -- Intentionally modify quantity 2 -> 5
  ('2007', 'Bananas (kg)', 1, 3.09, 3.09), 
  ('2007', 'Bread (loaf)', 2, 10.18, 20.36), 
  ('2007', 'Chicken (kg)', 3, 17.72, 53.16), 
  ('2007', 'Bread (loaf)', 4, 13.00, 52.00), 
  ('2006', 'Bread (loaf)', 5, 13.41, 67.05), 
  ('2006', 'Chicken (kg)', 3, 17.45, 52.35), 
  ('2006', 'Bread (loaf)', 4, 10.42, 41.68), 
  ('2006', 'Cheese (block)', 3, 16.01, 48.03), 
  ('2006', 'Rice (kg)', 4, 12.96, 51.84), 
  ('2006', 'Bananas (kg)', 1, 4.26, 4.26), 
  ('2005', 'Bread (loaf)', 3, 10.65, 31.95), 
  ('2005', 'Butter (pack)', 3, 6.20, 23.40), -- Intentionally modify unit price 7.80 -> 6.20
  ('2005', 'Tomatoes (kg)', 3, 8.51, 25.53), 
  ('2005', 'Bananas (kg)', 3, 4.18, 12.54), 
  ('2005', 'Rice (kg)', 2, 12.20, 24.40), 
  ('2004', 'Yogurt (cup)', 5, 2.38, 11.90), 
  ('2004', 'Butter (pack)', 3, 7.10, 21.30), 
  ('2004', 'Onions (kg)', 4, 4.34, 17.36), 
  ('2004', 'Bananas (kg)', 2, 3.53, 7.06), 
  ('2004', 'Tomatoes (kg)', 4, 7.24, 28.96), 
  ('2004', 'Bread (loaf)', 3, 9.66, 28.98), 
  ('2004', 'Milk (ltr)', 5, 15.02, 75.10), 
  ('2003', 'Eggs (dozen)', 5, 1.95, 9.75), 
  ('2003', 'Eggs (dozen)', 5, 2.88, 14.40), 
  ('2003', 'Milk (ltr)', 5, 16.84, 84.20), 
  ('2003', 'Milk (ltr)', 2, 10.77, 21.54), 
  ('2003', 'Eggs (dozen)', 5, 2.84, 14.20), 
  ('2002', 'Apples (kg)', 3, 4.86, 14.58), 
  ('2002', 'Cheese (block)', 4, 8.35, 33.40), 
  ('2002', 'Eggs (dozen)', 1, 2.51, 2.51), 
  ('2002', 'Milk (ltr)', 2, 17.83, 35.66), 
  ('2002', 'Onions (kg)', 3, 4.22, 12.66), 
  ('2002', 'Yogurt (cup)', 4, 1.05, 4.20), 
  ('2001', 'Milk (ltr)', 4, 14.18, 56.72), 
  ('2001', 'Rice (kg)', 3, 16.84, 50.52), 
  ('2001', 'Apples (kg)', 4, 5.05, 20.20), 
  ('2001', 'Yogurt (cup)', 2, 1.64, 3.28), 
  ('2001', 'Onions (kg)', 4, 4.69, 18.76), 
  ('2001', 'Tomatoes (kg)', 2, 6.81, 13.62), 
  ('2001', 'Onions (kg)', 3, 3.86, 11.58);
    

-- Example INSERT statement for the first few rows
INSERT INTO doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS (invoice_id, invoice_date, subtotal, tax, total) VALUES
  ('2010', '2025-04-25', 113.01, 11.30, 124.31), 
  ('2009', '2025-04-24', 232.75, 23.28, 256.03), 
  ('2008', '2025-04-23', 135.66, 13.57, 149.23), 
  ('2007', '2025-04-22', 191.37, 19.14, 210.51), 
  ('2006', '2025-04-21', 265.21, 26.52, 291.73), 
  ('2005', '2025-04-20', 117.82, 11.78, 129.60), 
  ('2004', '2025-04-19', 190.66, 99.99, 309.73), -- Intentionally modify tax from 19.07 -> 99.99 total from 209.73 -> 309.73
  ('2003', '2025-04-18', 144.09, 14.41, 158.50), 
  ('2002', '2025-04-17', 103.01, 23.10, 113.31), -- Intentionally modify tax from 10.30 -> 23.10
  ('2001', '2025-04-16', 174.68, 17.47, 192.15); 