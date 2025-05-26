# Document AI Integration with Snowflake  

## Overview  

Organizations handle a vast number of documents from various vendors, departments, and external sources, integrating them into internal systems. These documents often contain critical data required by downstream systems. Many businesses rely on manual or semi-automated processes that are inefficient and require significant human intervention.  

This solution **fully automates the end-to-end invoice data extraction and reconciliation process** using **Document AI table extraction model**, integrating with **Snowflake** for structured storage, invoice reconciliation, and visualization. 

## Solution Components  

The architecture consists of multiple components working together to efficiently process, extract, and validate data from PDFs:  

- **📄 Document AI Model** – Table extraction model to extract structured information from invoices.  
- **🗂 Metadata Management** – Centralized tables to manage extracted values.  
- **✅ Invoice Reconciliation** – Streams + Tasks to handle automatic invoice reconciliation based on the values extracted.  
- **🖥 Streamlit UI** – A web-based interface for viewing and performing invoice reconciliations on documents with discrepancies.  

This automated pipeline enhances efficiency, reduces manual workload, and ensures accurate and scalable document processing within **Snowflake**. 🚀  

## Step-By-Step Guide

For prerequisites, environment setup, step-by-step guide and instructions, please refer to the [QuickStart Guide](https://quickstarts.snowflake.com/guide/doc-ai-invoice-reconciliation/index.html?index=..%2F..index#0).
