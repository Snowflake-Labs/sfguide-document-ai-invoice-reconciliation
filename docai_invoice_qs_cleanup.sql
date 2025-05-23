USE ROLE ACCOUNTADMIN;

DROP TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT;
DROP TASK doc_ai_qs_db.doc_ai_schema.RECONCILE;

DROP STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DB_STREAM;
DROP STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DOCAI_STREAM;
DROP STREAM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM;

DROP PROCEDURE IF EXISTS doc_ai_qs_db.doc_ai_schema.SP_RUN_ITEM_RECONCILIATION();
DROP PROCEDURE IF EXISTS doc_ai_qs_db.doc_ai_schema.SP_RUN_TOTALS_RECONCILIATION();

DROP STAGE IF EXISTS doc_ai_qs_db.doc_ai_schema.DOC_AI_STAGE;

DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.CO_INVOICES_ITEMS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.DOCAI_PARSED;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS;
DROP TABLE IF EXISTS doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS;

DROP SCHEMA DOC_AI_SCHEMA;

DROP DATABASE DOC_AI_QS_DB;

DROP WAREHOUSE doc_ai_qs_wh;

DROP ROLE DOC_AI_QS_ROLE;