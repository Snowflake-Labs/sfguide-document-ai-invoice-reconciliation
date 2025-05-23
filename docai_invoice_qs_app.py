import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp, sql_expr
import snowflake.snowpark as snowpark # Required for types like DataFrame
import pandas as pd
from datetime import datetime
import pypdfium2 as pdfium # Import pypdfium2
import io

st.set_page_config(layout="wide") # Use wider layout for tables

# --- Configuration ---
DB_NAME = "DOC_AI_QS_DB"
SCHEMA_NAME = "DOC_AI_SCHEMA"
STAGE_NAME = "DOC_AI_STAGE"

RECONCILE_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_ITEMS"
RECONCILE_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS"
BRONZE_TRANSACT_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS"
BRONZE_TRANSACT_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS"
BRONZE_DOCAI_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_ITEMS"
BRONZE_DOCAI_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_TOTALS"

GOLD_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_ITEMS"
GOLD_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_TOTALS"

if 'processed_invoice_id' not in st.session_state:
    st.session_state.processed_invoice_id = None
if 'cached_mismatch_summary' not in st.session_state:
    st.session_state.cached_mismatch_summary = None

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Snowflake session established!")
    CURRENT_USER = session.get_current_role().replace("\"", "")
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop() # Stop execution if session cannot be established
    
# ─────────────────── Upload ───────────────────
with st.sidebar:
    st.header("📂 Upload Document")
    uploaded = st.file_uploader("PDF / Image", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True)
    st.divider()
    placeholder = st.empty()
if uploaded:
    for uploaded_file in uploaded:
        placeholder.empty()
        file_bytes = uploaded_file.read()
        file_name  = uploaded_file.name
        
        # Stage file & presigned URL
        session.file.put_stream(io.BytesIO(file_bytes), f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}", overwrite=True, auto_compress=False)
        presigned_url = session.sql(
            f"SELECT GET_PRESIGNED_URL('@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}', '{file_name}', 360) AS URL"
        ).to_pandas().at[0, "URL"]
        session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
        with st.sidebar:
            placeholder.write("Upload successful!")
    
    
# --- PDF Display Functions ---

def display_pdf_page():
    """Renders and displays the current PDF page."""
    if 'pdf_doc' not in st.session_state or st.session_state['pdf_doc'] is None:
        st.warning("No PDF document loaded.")
        return
    if 'pdf_page' not in st.session_state:
         st.session_state['pdf_page'] = 0 # Initialize if missing

    pdf = st.session_state['pdf_doc']
    page_index = st.session_state['pdf_page']
    num_pages = len(pdf)

    if not 0 <= page_index < num_pages:
        st.error(f"Invalid page index: {page_index}. Must be between 0 and {num_pages-1}.")
        st.session_state['pdf_page'] = 0 # Reset to first page
        page_index = 0

    page = pdf[page_index]

    try:
        # Rendering parameters - adjust scale for performance/quality trade-off
        bitmap = page.render(
                        scale = 2, # Reduced scale from 8 for potentially better performance
                        rotation = 0,
                )
        pil_image = bitmap.to_pil()
        st.image(pil_image, use_container_width='always') # Display the rendered page image
    except Exception as e:
        st.error(f"Error rendering PDF page {page_index + 1}: {e}")

def previous_pdf_page():
    """Navigates to the previous PDF page."""
    if 'pdf_page' in st.session_state and st.session_state['pdf_page'] > 0:
        st.session_state['pdf_page'] -= 1

def next_pdf_page():
    """Navigates to the next PDF page."""
    if ('pdf_page' in st.session_state and
        'pdf_doc' in st.session_state and
        st.session_state['pdf_doc'] is not None and
        st.session_state['pdf_page'] < len(st.session_state['pdf_doc']) - 1):
        st.session_state['pdf_page'] += 1

def summarize_mismatch_details(active_session, reconcile_items_details_df, reconcile_totals_details_df, selected_invoice_id):
    """
    Summarizes item mismatch details from two DataFrames for a selected invoice ID
    using Snowflake Cortex.
    """
    try:
        session = active_session # Get active Snowpark session

        # Filter DataFrames for the selected invoice ID
        try:
            item_mismatch_details = reconcile_items_details_df["ITEM_MISMATCH_DETAILS"][reconcile_items_details_df["INVOICE_ID"] == selected_invoice_id].iloc[0]
        except: 
            item_mismatch_details = ""
        try:
            total_mismatch_details = reconcile_totals_details_df["ITEM_MISMATCH_DETAILS"][reconcile_totals_details_df["INVOICE_ID"] == selected_invoice_id].iloc[0]
        except: 
            total_mismatch_details = ""

        all_mismatch_details = item_mismatch_details + total_mismatch_details

        if not all_mismatch_details:
            return f"No item mismatch details found for Invoice ID: {selected_invoice_id}."


        # Construct the prompt for Snowflake Cortex
        prompt = f"""
        Based on the following item mismatch details for invoice {selected_invoice_id}, please provide a concise summary of the differences.
        Focus on the types of mismatches and affected items or amounts, do not use the words expected or actual.

        Mismatch Details:
        ---
        {all_mismatch_details}
        ---
        """

        # Call Cortex complete function
        # You might need to adjust the model name based on availability and your needs.
        try:
            response_df = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{prompt}') as summary").collect()
            summary = response_df[0]["SUMMARY"] if response_df else "Could not retrieve summary."
        except Exception as e:
            return f"Error calling Snowflake Cortex: {e}"


        return summary

    except Exception as e:
        st.error(f"An error occurred: {e}")
        return f"Failed to generate summary due to an error: {str(e)}"
        
def get_invoice_reconciliation_metrics(session: session) -> dict | None:

    # Define the database and schema for clarity
    db_name = "doc_ai_qs_db"
    schema_name = "doc_ai_schema"
    transact_table = f"{db_name}.{schema_name}.TRANSACT_TOTALS"
    gold_totals_table = f"{db_name}.{schema_name}.GOLD_INVOICE_TOTALS"
    gold_items_table = f"{db_name}.{schema_name}.GOLD_INVOICE_ITEMS"

    # Construct the single SQL query using Common Table Expressions (CTEs) for clarity
    # and conditional aggregation.
    sql_query = f"""
    SELECT
        -- 1) Extract unique invoice count and total sum from TRANSACT_TOTALS
        COUNT(DISTINCT tt.invoice_id) AS total_invoice_count,
        SUM(tt.total) AS grand_total_amount,

        -- 2 & 3) Determine presence in both GOLD tables and aggregate conditionally
        COUNT(DISTINCT CASE
                        WHEN EXISTS (SELECT 1 FROM {gold_totals_table} git WHERE git.invoice_id = tt.invoice_id)
                         AND EXISTS (SELECT 1 FROM {gold_items_table} gii WHERE gii.invoice_id = tt.invoice_id)
                        THEN tt.invoice_id -- Count distinct reconciled invoice IDs
                        ELSE NULL
                    END) AS reconciled_invoice_count,
        --  Determine presence in both GOLD tables and aggregate conditionally for AUTO-RECONCILED
        COUNT(DISTINCT CASE
                        WHEN EXISTS (SELECT 1 FROM {gold_totals_table} git WHERE git.invoice_id = tt.invoice_id AND git.reviewed_by = 'Auto-reconciled')
                         AND EXISTS (SELECT 1 FROM {gold_items_table} gii WHERE gii.invoice_id = tt.invoice_id AND gii.reviewed_by = 'Auto-reconciled')
                        THEN tt.invoice_id -- Count distinct auto-reconciled invoice IDs
                        ELSE NULL
                    END) AS auto_reconciled_invoice_count,
        SUM(CASE
                WHEN EXISTS (SELECT 1 FROM {gold_totals_table} git WHERE git.invoice_id = tt.invoice_id)
                 AND EXISTS (SELECT 1 FROM {gold_items_table} gii WHERE gii.invoice_id = tt.invoice_id)
                THEN tt.total -- Sum total only if reconciled
                ELSE 0 -- Contribute 0 to the sum if not reconciled
            END) AS total_reconciled_amount,
        
    FROM
        {transact_table} AS tt;
    """

    try:
        st.write("Executing Reconciliation Query:")
        # st.code(sql_query, language='sql')

        # Execute the query using the provided session object
        # .collect() fetches all results (in this case, just one row)
        result = session.sql(sql_query).collect()

        if not result:
            st.warning(f"No data found in the source table: {transact_table}")
            return None

        # result is a list containing one Snowpark Row object
        row = result[0]

        # Extract values using the aliases defined in the SQL query
        total_invoices = row['TOTAL_INVOICE_COUNT']
        grand_total = row['GRAND_TOTAL_AMOUNT']
        reconciled_invoices = row['RECONCILED_INVOICE_COUNT']
        total_reconciled = row['TOTAL_RECONCILED_AMOUNT']
        count_auto_reconciled = row['AUTO_RECONCILED_INVOICE_COUNT']

        # Handle potential None values if SUM results in NULL (e.g., empty table)
        # Although COUNT should return 0, SUM might return NULL.
        grand_total = grand_total or 0.0
        total_reconciled = total_reconciled or 0.0
        total_invoices = total_invoices or 0
        reconciled_invoices = reconciled_invoices or 0
        count_auto_reconciled = count_auto_reconciled or 0


        # 4) Calculate ratios, handling potential division by zero
        reconciled_invoice_ratio = (float(reconciled_invoices) / float(total_invoices)) if total_invoices > 0 else 0.0
        reconciled_amount_ratio = (float(total_reconciled) / float(grand_total)) if grand_total != 0 else 0.0 # Use != 0 for float comparison robustness

        metrics = {
            'total_invoice_count': int(total_invoices), # Ensure integer count
            'grand_total_amount': float(grand_total),
            'reconciled_invoice_count': int(reconciled_invoices), # Ensure integer count
            'total_reconciled_amount': float(total_reconciled),
            'reconciled_invoice_ratio': float(reconciled_invoice_ratio),
            'reconciled_amount_ratio': float(reconciled_amount_ratio),
            'count_auto_reconciled': int(count_auto_reconciled)
        }
        return metrics

    except Exception as e:
        st.error(f"Snowflake SQL Error during reconciliation: {e}")
        return None



# --- Helper Functions ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_reconcile_data(status_filter='Pending Review'):
    """Loads data from reconciliation tables, optionally filtering by review_status."""
    try:
        # Use UNION ALL to get all invoice IDs needing review from both tables
        # Select distinct invoice IDs to avoid duplicates if an invoice has issues in both items and totals
        query = f"""
        SELECT DISTINCT invoice_id, review_status, last_reconciled_timestamp
        FROM {RECONCILE_ITEMS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        UNION
        SELECT DISTINCT invoice_id, review_status, last_reconciled_timestamp
        FROM {RECONCILE_TOTALS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        ORDER BY last_reconciled_timestamp DESC
        """
        reconcile_df = session.sql(query).to_pandas()

        # Also load full details for display later if needed (optional here, load on demand below)
        reconcile_items_full_df = session.table(RECONCILE_ITEMS_TABLE).filter(col("review_status") == status_filter).to_pandas() if status_filter != 'All' else session.table(RECONCILE_ITEMS_TABLE).to_pandas()
        reconcile_totals_full_df = session.table(RECONCILE_TOTALS_TABLE).filter(col("review_status") == status_filter).to_pandas() if status_filter != 'All' else session.table(RECONCILE_TOTALS_TABLE).to_pandas()

        return reconcile_df, reconcile_items_full_df, reconcile_totals_full_df
    except Exception as e:
        st.error(f"Error loading reconcile data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # Return empty DataFrames on error

def load_bronze_data(invoice_id):
    """Loads data from all relevant bronze tables for a specific invoice_id."""
    bronze_data = {}
    try:
        if invoice_id:
            # Use .filter().to_pandas() for smaller datasets typical for one invoice
            bronze_data['transact_items'] = session.table(BRONZE_TRANSACT_ITEMS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['transact_totals'] = session.table(BRONZE_TRANSACT_TOTALS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['docai_items'] = session.table(BRONZE_DOCAI_ITEMS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['docai_totals'] = session.table(BRONZE_DOCAI_TOTALS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
        return bronze_data
    except Exception as e:
        st.error(f"Error loading Bronze data for invoice {invoice_id}: {e}")
        return {} # Return empty dict on error

# --- Streamlit App UI ---
st.title("🛒 Invoice Reconciliation")
st.markdown(f"Connected as role: **{CURRENT_USER}**")

# --- Section 0: Display Totals
#run_reconciliation(session)
with st.spinner("Loading reconciliation metrics..."):
    reconciliation_data = get_invoice_reconciliation_metrics(session)

# --- Display results ---
if reconciliation_data:
    st.success("Metrics displayed below (updated automatically).")
    st.success(f"{reconciliation_data['count_auto_reconciled']} Invoices out of {reconciliation_data['total_invoice_count']} were fully Auto-Reconciled with DocAI")
    st.subheader("Reconciliation Ratios")
    col1, col2 = st.columns(2)
    col1.metric(
        label="Reconciled Invoices (Count Ratio)",
        value=f"{reconciliation_data['reconciled_invoice_ratio']:.2%}",
        help=f"Percentage of unique invoices from TRANSACT_TOTALS found in both GOLD tables. ({reconciliation_data['reconciled_invoice_count']}/{reconciliation_data['total_invoice_count']})"
    )
    col2.metric(
        label="Reconciled Amount (Value Ratio)",
        value=f"{reconciliation_data['reconciled_amount_ratio']:.2%}",
        help=f"Percentage of total amount from TRANSACT_TOTALS that corresponds to reconciled invoices. (${reconciliation_data['total_reconciled_amount']:,.2f} / ${reconciliation_data['grand_total_amount']:,.2f})"
    )

    st.subheader("Detailed Numbers")
    df_metrics = pd.DataFrame([
         {"Metric": "Total Unique Invoices", "Value": reconciliation_data['total_invoice_count']},
         {"Metric": "Fully Reconciled Invoices", "Value": reconciliation_data['reconciled_invoice_count']},
         {"Metric": "Grand Total Amount ($)", "Value": f"{reconciliation_data['grand_total_amount']:,.2f}"},
         {"Metric": "Total Reconciled Amount ($)", "Value": f"{reconciliation_data['total_reconciled_amount']:,.2f}"},
    ]).set_index("Metric")
    st.dataframe(df_metrics)

else:
    # Error messages are now mostly handled within the cached function call
    st.warning("Could not retrieve or calculate reconciliation metrics. Check logs above if any.")

# --- Section 1: Display reconcile Tables & Select Invoice ---
st.header("1. Invoices Awaiting Review")

review_status_options = ['Pending Review', 'Reviewed', 'Auto-reconciled']
selected_status = st.selectbox("Filter by Review Status:", review_status_options, index=0) # Default to 'Pending Review'

# Load distinct invoice IDs based on filter
invoices_to_review_df, reconcile_items_details_df, reconcile_totals_details_df = load_reconcile_data(selected_status)

if not invoices_to_review_df.empty:
    st.write(f"Found {len(invoices_to_review_df['INVOICE_ID'].unique())} unique invoices with totals or items status '{selected_status}'.")

    # Allow user to select an invoice
    invoice_list = [""] + invoices_to_review_df['INVOICE_ID'].unique().tolist() # Add blank option
    selected_invoice_id = st.selectbox(
        "Select Invoice ID to Review/Correct:",
        invoice_list,
        index=0, # Default to blank
        key="invoice_selector"
    )

    # Display details from reconcile tables for context (optional)
    with st.expander("Show Reconciliation Details for All Filtered Invoices"):
         st.subheader("Item Reconciliation Details")
         st.dataframe(reconcile_items_details_df, use_container_width=True)
         st.subheader("Total Reconciliation Details")
         st.dataframe(reconcile_totals_details_df, use_container_width=True)

else:
    st.info(f"No invoices found with status '{selected_status}'.")
    selected_invoice_id = None # Ensure no invoice is selected if list is empty

# --- Section 2: Display Bronze Data for Selected Invoice ---
st.header("2. Review and Correct Invoice Data")

if selected_invoice_id:
    st.subheader(f"Displaying Data for Invoice: `{selected_invoice_id}`")
    if selected_invoice_id != st.session_state.processed_invoice_id:
        mismatch_summary = summarize_mismatch_details(session, reconcile_items_details_df, reconcile_totals_details_df, selected_invoice_id)
        st.session_state.cached_mismatch_summary = mismatch_summary
        st.session_state.processed_invoice_id = selected_invoice_id
    if st.session_state.cached_mismatch_summary is not None:
        st.subheader(f"{st.session_state.cached_mismatch_summary}")
    # Load data from Bronze layer
    bronze_data_dict = load_bronze_data(selected_invoice_id)

    if bronze_data_dict:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Original Data (Source: TRANSACT_*)**")
            st.markdown(":rainbow[Editable Fields] - These can be edited directly and then accepted in section 3.")

            # --- Editable Transact Items ---
            st.write("**Items (Original DB):**")
            if not bronze_data_dict['transact_items'].empty:
                 # Convert relevant columns to numeric for proper editing if they aren't already
                 for col_name in ['QUANTITY', 'UNIT_PRICE', 'TOTAL_PRICE']:
                    if col_name in bronze_data_dict['transact_items'].columns:
                        bronze_data_dict['transact_items'][col_name] = pd.to_numeric(bronze_data_dict['transact_items'][col_name], errors='coerce')

                 edited_transact_items_df = st.data_editor(
                    bronze_data_dict['transact_items'],
                    key="editor_transact_items",
                    num_rows="dynamic", # Allow adding/deleting rows
                    use_container_width=True,
                    column_config={ # Optional: Add specific configs for better editing
                        "UNIT_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                        "TOTAL_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                    }
                )
                 st.session_state.edited_transact_items = edited_transact_items_df # Store edited data
            else:
                 st.info("No data found in TRANSACT_ITEMS for this invoice.")

            # --- Editable Transact Totals ---
            st.write("**Totals (Original DB):**")
            if not bronze_data_dict['transact_totals'].empty:
                # Ensure only one row and make it editable
                transact_totals_edit_df = bronze_data_dict['transact_totals'].head(1).copy() # Take first row if multiple exist

                # Convert relevant columns to numeric/date
                for col_name in ['SUBTOTAL', 'TAX', 'TOTAL']:
                     if col_name in transact_totals_edit_df.columns:
                        transact_totals_edit_df[col_name] = pd.to_numeric(transact_totals_edit_df[col_name], errors='coerce')
                if 'INVOICE_DATE' in transact_totals_edit_df.columns:
                    transact_totals_edit_df['INVOICE_DATE'] = pd.to_datetime(transact_totals_edit_df['INVOICE_DATE'], errors='coerce').dt.date


                edited_transact_totals_df = st.data_editor(
                    transact_totals_edit_df,
                    key="editor_transact_totals",
                    hide_index=True,
                    use_container_width=True,
                    disabled=["invoice_id"], # Don't allow editing invoice ID here
                     column_config={ # Optional: Add specific configs for better editing
                        "INVOICE_DATE": st.column_config.DateColumn(format="YYYY-MM-DD"),
                        "SUBTOTAL": st.column_config.NumberColumn(format="$%.2f"),
                        "TAX": st.column_config.NumberColumn(format="$%.2f"),
                        "TOTAL": st.column_config.NumberColumn(format="$%.2f"),
                    }
                )
                st.session_state.edited_transact_totals = edited_transact_totals_df # Store edited data
            else:
                st.info("No data found in TRANSACT_TOTALS for this invoice.")

        with col2:
            st.markdown("**Reference Data (Source: DOCAI_*)**")
            st.caption("Read-only reference extracted by Document AI.")

            # --- Display DocAI Items ---
            st.write("**Items (DocAI):**")
            if not bronze_data_dict['docai_items'].empty:
                st.dataframe(bronze_data_dict['docai_items'], use_container_width=True)

                st.session_state.docai_items = bronze_data_dict['docai_items'] # Store docai data for docai reconcile button
            else:
                st.info("No data found in DOCAI_INVOICE_ITEMS for this invoice.")

            # --- Display DocAI Totals ---
            st.write("**Totals (DocAI):**")
            if not bronze_data_dict['docai_totals'].empty:
                st.dataframe(bronze_data_dict['docai_totals'], use_container_width=True)
                pdf_file_name = bronze_data_dict['docai_totals']['FILE_NAME'][0]

                st.session_state.docai_totals = bronze_data_dict['docai_totals'] # Store docai data for docai reconcile button
            else:
                st.info("No data found in DOCAI_INVOICE_TOTALS for this invoice.")
                pdf_file_name = ""

        # Initialize session state keys for PDF viewer if they don't exist
        if 'pdf_page' not in st.session_state:
            st.session_state['pdf_page'] = 0
        if 'pdf_doc' not in st.session_state:
            st.session_state['pdf_doc'] = None
        if 'pdf_url' not in st.session_state:
            st.session_state['pdf_url'] = None
        with st.expander("Show Selected Invoice"):

            # --- PDF Display Logic ---
            if selected_invoice_id:
                    
                stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{pdf_file_name}"
    
                # Check if we need to load a new PDF
                if (st.session_state['pdf_url'] != stage_path):
                    st.write(f"Loading PDF from: {stage_path}")
                    try:
                        pdf_stream = session.file.get_stream(stage_path, decompress=False)
                        pdf_bytes = pdf_stream.read() # Read into memory first
                        pdf_stream.close()
                        st.session_state['pdf_doc'] = pdfium.PdfDocument(pdf_bytes) # Load from bytes
                        st.session_state['pdf_url'] = stage_path
                        st.session_state['pdf_page'] = 0 # Reset to first page
                        st.success(f"Loaded '{pdf_file_name}'")
                    except Exception as e:
                        if pdf_file_name == "":
                            st.warning(f"The file you're looking for may not have been uploaded yet!")
                        else:
                            st.warning(f"Failed to load or read PDF from '{stage_path}': {e}")
                            st.session_state['pdf_doc'] = None # Ensure pdf_doc is None on failure
                            st.session_state['pdf_url'] = None
    
                # Display navigation and page if PDF is loaded
                if st.session_state.get('pdf_doc') is not None:
                    nav_col1, nav_col2, nav_col3 = st.columns([1,2,1]) # Adjusted column ratios
                    with nav_col1:
                        st.button("⏮️ Previous", on_click=previous_pdf_page, use_container_width=True)
                    with nav_col2:
                        st.write(f"<div style='text-align: center;'>Page {st.session_state['pdf_page'] + 1} of {len(st.session_state['pdf_doc'])}</div>", unsafe_allow_html=True)
                    with nav_col3:
                        st.button("Next ⏭️", on_click=next_pdf_page, use_container_width=True)
    
                    display_pdf_page() # Call the function to render the page
                else:
                     st.info("Could not load document preview.")
    
            else:
                st.warning("No Invoice ID found for the selected row to retrieve the document.")
                # Clear PDF state if nothing is selected
                st.session_state['pdf_doc'] = None
                st.session_state['pdf_url'] = None
                st.session_state['pdf_page'] = 0


        
        # --- Section 3: Submit Corrections ---
        st.header("3. Submit Review and Corrections")
        
        submit_docai_button = st.button("❄️ ✅ Accept DocAI Extracted Values for Reconciliation")
        
        st.write("Or...")
        
        # Add fields for notes and corrected invoice number (if applicable)
        review_notes = st.text_area("Manual Review Notes / Comments:", key="review_notes")

        submit_button = st.button("✍️ ✔️ Accept Manual Edits above for Reconciliation")

        if submit_button | submit_docai_button:
            # --- Data Validation (Basic Example) ---
            valid = True
            if submit_docai_button:
                if 'docai_items' not in st.session_state or st.session_state.docai_items.empty:
                    st.warning("No item data to submit.")
                    valid = False
                elif 'docai_totals' not in st.session_state or st.session_state.docai_totals.empty:
                    st.warning("No total data to submit.")
                    valid = False
                else:
                    gold_items_df = st.session_state.docai_items[["INVOICE_ID", "PRODUCT_NAME", "QUANTITY", "UNIT_PRICE", "TOTAL_PRICE"]].copy()
                    gold_totals_df = st.session_state.docai_totals[["INVOICE_ID", "INVOICE_DATE", "SUBTOTAL", "TAX", "TOTAL"]].copy()
                
            else:
                if 'edited_transact_items' not in st.session_state or st.session_state.edited_transact_items.empty:
                    st.warning("No item data to submit.")
                    valid = False
                elif 'edited_transact_totals' not in st.session_state or st.session_state.edited_transact_totals.empty:
                    st.warning("No total data to submit.")
                    valid = False
                else:
                    gold_items_df = st.session_state.edited_transact_items.copy()
                    gold_totals_df = st.session_state.edited_transact_totals.copy()

            if valid:
                try:
                    st.write("Submitting...")
                    current_ts = datetime.now() # Use current time for reviewed_timestamp

                # --- Prepare Data for Gold Layer ---
                # Items
                    # Ensure invoice_id is consistent if rows were added/modified
                    gold_items_df['INVOICE_ID'] = selected_invoice_id
                    gold_items_df['REVIEWED_BY'] = CURRENT_USER
                    gold_items_df['REVIEWED_TIMESTAMP'] = current_ts
                    gold_items_df['NOTES'] = review_notes # Add notes

                     # Convert Pandas DataFrame to Snowpark DataFrame for writing
                    snowpark_gold_items = session.create_dataframe(gold_items_df)
                    # Convert column names to uppercase to match Snowflake default behavior if needed
                    for col_name in snowpark_gold_items.columns:
                         snowpark_gold_items = snowpark_gold_items.with_column_renamed(col_name, col_name.upper())

                    # Write to Gold Items Table (Append Mode)
                    st.write(f"Writing {len(snowpark_gold_items.collect())} rows to {GOLD_ITEMS_TABLE}...") 
                    # Example: Delete existing before insert
                    session.sql(f"DELETE FROM {GOLD_ITEMS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'").collect()
                    snowpark_gold_items.write.mode("append").save_as_table(GOLD_ITEMS_TABLE)
                    st.success(f"Successfully saved corrected items to {GOLD_ITEMS_TABLE}")

                # Totals
                    gold_totals_df = st.session_state.edited_transact_totals.copy()
                     # Ensure invoice_id is consistent
                    gold_totals_df['INVOICE_ID'] = selected_invoice_id
                    gold_totals_df['REVIEWED_BY'] = CURRENT_USER
                    gold_totals_df['REVIEWED_TIMESTAMP'] = current_ts
                    gold_totals_df['NOTES'] = review_notes

                    # Convert Pandas DataFrame to Snowpark DataFrame for writing
                    snowpark_gold_totals = session.create_dataframe(gold_totals_df)
                    # Convert column names to uppercase
                    for col_name in snowpark_gold_totals.columns:
                         snowpark_gold_totals = snowpark_gold_totals.with_column_renamed(col_name, col_name.upper())

                    # Write to Gold Totals Table (Append Mode - or Merge/Delete+Insert)
                    st.write(f"Writing corrected totals to {GOLD_TOTALS_TABLE}...")
                    # Example: Delete existing before insert (use with caution)
                    session.sql(f"DELETE FROM {GOLD_TOTALS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'").collect()
                    snowpark_gold_totals.write.mode("append").save_as_table(GOLD_TOTALS_TABLE)
                    st.success(f"Successfully saved corrected totals to {GOLD_TOTALS_TABLE}")

                    # --- Update reconcile Tables Review Status ---
                    st.write("Updating review status in reconcile tables...")
                    update_query = f"""
                    UPDATE {RECONCILE_ITEMS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', -- Format for TIMESTAMP_NTZ
                        NOTES = '{review_notes.replace("'", "''")}' -- Escape single quotes
                    WHERE INVOICE_ID = '{selected_invoice_id}';
                    """
                    session.sql(update_query).collect() # Use collect() to execute the update

                    update_query = f"""
                    UPDATE {RECONCILE_TOTALS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}',
                        NOTES = '{review_notes.replace("'", "''")}'
                    WHERE INVOICE_ID = '{selected_invoice_id}';
                    """
                    session.sql(update_query).collect() # Use collect() to execute the update

                    st.success(f"Successfully updated review status for invoice {selected_invoice_id} in reconcile tables.")

                    # --- Clear Cache and Rerun ---
                    st.cache_data.clear() # Clear the cache to reflect updated reconcile status

                    st.info("Refreshing application...")
                    st.rerun() # Rerun the app to refresh the invoice list

                except Exception as e:
                    st.error(f"An error occurred during submission: {e}")

    else:
        st.warning("Please select an invoice ID from the dropdown above.")

else:
    # Only show this message if the list wasn't empty but nothing was selected
    if not invoices_to_review_df.empty:
        st.info("Select an Invoice ID from the dropdown in section 1 to proceed.")
