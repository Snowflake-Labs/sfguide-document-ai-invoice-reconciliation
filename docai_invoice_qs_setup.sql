USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE doc_ai_qs_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_qs_role;
GRANT EXECUTE TASK on account to role doc_ai_qs_role;

GRANT ROLE doc_ai_qs_role TO USER AMELATTI;

-- CREATE A WAREHOUSE TO BE USED
CREATE WAREHOUSE doc_ai_qs_wh;

-- GIVE THE doc_ai_qs_role ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE doc_ai_qs_wh TO ROLE doc_ai_qs_role;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE doc_ai_qs_role ACCESS
CREATE DATABASE doc_ai_qs_db;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE doc_ai_qs_db TO ROLE doc_ai_qs_role;

-- CHANGE TO THE QUICKSTART ROLE
USE ROLE doc_ai_qs_role;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE SCHEMA doc_ai_qs_db.doc_ai_schema;
-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE table ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE snowflake.ml.document_intelligence on schema doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE MODEL ON SCHEMA doc_ai_qs_db.doc_ai_schema TO ROLE doc_ai_qs_role;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE OR REPLACE STAGE doc_ai_qs_db.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

USE SCHEMA doc_ai_qs_db.doc_ai_schema;

-- CREATE A STREAM TO MONITOR THE STAGE FOR NEW FILE UPLOADS
CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM 
ON DIRECTORY(@DOC_AI_STAGE);

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS (
    invoice_id VARCHAR(255),
    product_name VARCHAR(255),
    quantity NUMBER(12, 2),
    unit_price NUMBER(12, 2),
    total_price NUMBER(12, 2),
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255)
);

 CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS (
    invoice_id VARCHAR(255),
    invoice_date DATE,
    subtotal NUMBER(12, 2),
    tax NUMBER(12, 2),
    total NUMBER(12, 2),
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255)
);

-- CREATE A TASK TO RUN WHEN THE STREAM DETECTS NEW FILE UPLOADS IN OUR STAGE
create or replace task doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT
	warehouse=doc_ai_qs_wh
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('INVOICE_STREAM')
	as BEGIN
        CREATE OR REPLACE TEMPORARY TABLE doc_ai_qs_db.doc_ai_schema.docai_parsed AS (
        SELECT
            Relative_path as file_name,
            size as file_size,
            last_modified,
            file_url as snowflake_file_url,
            PARSE_JSON(DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_QS_INVOICES!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1)) AS json_data
        FROM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM
        WHERE METADATA$ACTION = 'INSERT' OR METADATA$ACTION = 'UPDATE'
        );

        CREATE OR REPLACE TEMPORARY TABLE extracted_item_data AS (
        SELECT
            -- Remove '$' and ',' characters, then attempt to cast to NUMBER for appropriate values
            REPLACE(p.json_data:"total_info|Invoice ID"[0].value::STRING, '#', '') AS invoice_id,
            ni.value:"value"::STRING AS product_name,
            TRY_CAST(qi.value:"value"::STRING AS NUMBER(12, 2)) AS quantity,
            TRY_CAST(REPLACE(REPLACE(pri.value:"value"::STRING, '$', ''), ',', '') AS NUMBER(12, 2)) AS unit_price,
            TRY_CAST(REPLACE(REPLACE(ti.value:"value"::STRING, '$', ''), ',', '') AS NUMBER(12, 2)) AS total_price,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url
        FROM
            doc_ai_qs_db.doc_ai_schema.docai_parsed p,
            LATERAL FLATTEN(input => p.json_data:"order_info|Item") ni,
            LATERAL FLATTEN(input => p.json_data:"order_info|Quantity") qi,
            LATERAL FLATTEN(input => p.json_data:"order_info|Price") pri,
            LATERAL FLATTEN(input => p.json_data:"order_info|Total") ti
        WHERE
            ni.index = pri.index AND
            ni.index = qi.index AND
            ni.index = ti.index
        );
        
        DELETE FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS
        WHERE invoice_id IN (SELECT DISTINCT invoice_id FROM extracted_item_data);
        
        -- Step 2: Insert all the new line items from incoming_data.
        INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS (invoice_id, product_name, quantity, unit_price, total_price, file_name, file_size, last_modified, snowflake_file_url)
        SELECT invoice_id, product_name, quantity, unit_price, total_price, file_name, file_size, last_modified, snowflake_file_url
        FROM extracted_item_data;

        CREATE OR REPLACE TEMPORARY TABLE extracted_total_data AS(
            SELECT
                -- Remove '$' and ',' characters, then attempt to cast to NUMBER for appropriate values
                REPLACE(p.json_data:"total_info|Invoice ID"[0].value::STRING, '#', '') AS invoice_id,
                TRY_CAST(p.json_data:"total_info|Date"[0].value::STRING AS DATE) AS invoice_date,
                TRY_CAST(REPLACE(REPLACE(p.json_data:"total_info|Subtotal"[0].value::STRING, '$', ''), ',', '') AS NUMBER(12, 2)) AS subtotal,
                TRY_CAST(REPLACE(REPLACE(p.json_data:"total_info|Tax"[0].value::STRING, '$', ''), ',', '') AS NUMBER(12, 2)) AS tax,
                TRY_CAST(REPLACE(REPLACE(p.json_data:"total_info|Grand Total"[0].value::STRING, '$', ''), ',', '') AS NUMBER(12, 2)) AS total,
                file_name,
                file_size,
                last_modified,
                snowflake_file_url
            FROM
                doc_ai_qs_db.doc_ai_schema.docai_parsed p
            );

        DELETE FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS
        WHERE invoice_id IN (SELECT DISTINCT invoice_id FROM extracted_total_data);
        
        -- Step 2: Insert all the new totals from incoming_data.
        INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS (invoice_id,
          invoice_date,
          subtotal,
          tax,
          total,
          file_name,
          file_size,
          last_modified,
          snowflake_file_url)
        SELECT
          invoice_id,
          invoice_date,
          subtotal,
          tax,
          total,
          file_name,
          file_size,
          last_modified,
          snowflake_file_url
        FROM extracted_total_data;
    END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT RESUME;

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (
    invoice_id VARCHAR(255),
    product_name VARCHAR(255),
    quantity NUMBER(10, 2),
    unit_price NUMBER(10, 2),
    total_price NUMBER(12, 2)
);


 CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS (
    invoice_id VARCHAR(255),
    invoice_date DATE,
    subtotal NUMBER(10, 2),
    tax NUMBER(10, 2),
    total NUMBER(12, 2)
);
