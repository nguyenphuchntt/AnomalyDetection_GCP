# cd streamlit
# pip install -r requirements.txt
# streamlit run app.py

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os  # <--- THÊM VÀO
from dotenv import load_dotenv
load_dotenv()

# Page config
st.set_page_config(
    page_title="Fraud Detection Admin",
    layout="wide"
)

# Configuration - UPDATE THESE
PROJECT_ID = os.getenv("PROJECT_ID")  
DATASET_ID = os.getenv("DATASET_ID")  
PREDICTION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.prediction_data"
INPUT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.data_input_test"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw-data"
HISTORY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.history_db"

@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client"""

    # Try local file first (for local development)
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if credentials_path and os.path.exists(credentials_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            st.sidebar.success("Auth: Local Service Account")
            return client
        except Exception as e:
            st.sidebar.warning(f"Local auth failed: {e}")
    
    # Try environment variable JSON (for AWS deployment)
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json:
        try:
            import json
            service_account_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            st.sidebar.success("Auth: AWS Environment Variable")
            return client
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON in GOOGLE_APPLICATION_CREDENTIALS_JSON: {e}")
            st.stop()
        except Exception as e:
            st.error(f"Error loading credentials: {e}")
            st.stop()
    
    # No credentials found
    st.error("No credentials found!")
    st.info("""
    Please set one of:
    - Local: GOOGLE_APPLICATION_CREDENTIALS (path to JSON file)
    - AWS: GOOGLE_APPLICATION_CREDENTIALS_JSON (JSON content)
    """)
    st.stop()


client = get_bigquery_client()



# ==================== HELPER FUNCTIONS ====================
def get_pending_frauds():
    """Get all fraud predictions pending verification (result=1 AND checked=False)"""
    query = f"""
    SELECT 
        p.transaction_id,
        p.prediction_result,
        p.checked,
        i.* EXCEPT(transaction_id)
    FROM `{PREDICTION_TABLE}` p
    LEFT JOIN `{INPUT_TABLE}` i
    ON p.transaction_id = i.transaction_id
    WHERE p.prediction_result = 1 AND p.checked = False
    ORDER BY p.transaction_id DESC
    """
    return client.query(query).to_dataframe()


def update_prediction_and_history(transaction_id, new_result, set_checked=True):
    """Update prediction_result and checked status"""

    query = f"""
    UPDATE `{PREDICTION_TABLE}`
    SET prediction_result = @new_result,
        checked = @checked
    WHERE transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id)),
            bigquery.ScalarQueryParameter("new_result", "INT64", new_result),
            bigquery.ScalarQueryParameter("checked", "BOOL", set_checked)
        ]
    )
    client.query(query, job_config=job_config).result()

    query = f"""
    UPDATE `{HISTORY_TABLE}`
    SET actual_result = @new_result
    WHERE transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id)),
            bigquery.ScalarQueryParameter("new_result", "INT64", new_result)
        ]
    )
    client.query(query, job_config=job_config).result()



def update_raw_and_history(transaction_id, new_result):
    """Update class in raw_data + actual_result in history_db"""
    
    # Update raw_data
    query = f"""
    UPDATE `{RAW_TABLE}`
    SET Class = @new_result
    WHERE transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id)),
            bigquery.ScalarQueryParameter("new_result", "INT64", new_result)
        ]
    )
    client.query(query, job_config=job_config).result()
    
    # Update history_db
    query = f"""
    UPDATE `{HISTORY_TABLE}`
    SET actual_result = @new_result
    WHERE transaction_id = @transaction_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id)),
            bigquery.ScalarQueryParameter("new_result", "INT64", new_result)
        ]
    )
    client.query(query, job_config=job_config).result()



def search_transaction_in_prediction(transaction_id):
    """Search in prediction + input tables (active transactions)"""
    query = f"""
    SELECT 
        p.transaction_id,
        p.prediction_result,
        p.checked,
        i.* EXCEPT(transaction_id)
    FROM `{PREDICTION_TABLE}` p
    LEFT JOIN `{INPUT_TABLE}` i
    ON p.transaction_id = i.transaction_id
    WHERE p.transaction_id = @transaction_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id))
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df

def search_transaction_in_raw(transaction_id):
    """Search in raw_data (finalized transactions)"""
    query = f"""
    SELECT *
    FROM `{RAW_TABLE}`
    WHERE transaction_id = @transaction_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", str(transaction_id))
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df

# ==================== MAIN APP ====================

st.title("Fraud Detection Admin Interface")

tab1, tab2, tab3 = st.tabs(["Fraud Alert & Review", "Transaction Search", "Dashboard"])

# ==================== TAB 1: FRAUD ALERT ====================
with tab1:
    st.header("Pending Fraud Verification")

    # Refresh button
    if st.button("Refresh Data", key="refresh_tab1"):
        st.cache_data.clear()
        st.rerun()
    
    try:
        df_frauds = get_pending_frauds()
        
        if df_frauds.empty:
            st.success("No pending fraud cases to review!")
        else:
            st.warning(f"**{len(df_frauds)} fraud cases** pending verification")
            
            # Display table
            st.subheader("Select a transaction to review:")
            
            # Select important columns for display
            display_cols = ['transaction_id', 'prediction_result']
            # Add other feature columns if they exist
            other_cols = [col for col in df_frauds.columns if col not in display_cols and col != 'checked']
            display_df = df_frauds[display_cols + other_cols]
            
            selected_event = st.dataframe(
                display_df,
                width='stretch',
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun"
            )
            
            # Show details if selected
            if selected_event and len(selected_event.selection.rows) > 0:
                selected_idx = selected_event.selection.rows[0]
                selected_row = df_frauds.iloc[selected_idx]
                transaction_id = selected_row['transaction_id']
                
                st.divider()
                st.subheader(f"Transaction Details: `{transaction_id}`")
                
                # Display transaction info in 2 columns
                col_left, col_right = st.columns(2)
                
                with col_left:
                    st.markdown("### Prediction Info")
                    st.metric("Prediction Result", "FRAUD (1)" if selected_row['prediction_result'] == 1 else "NOT FRAUD (0)")
                    st.metric("Checked", selected_row['checked'])
                
                with col_right:
                    st.markdown("### Transaction Features")
                    # Show all features as a transposed dataframe
                    feature_cols = [col for col in selected_row.index if col not in ['transaction_id', 'prediction_result', 'checked']]
                    feature_df = pd.DataFrame({
                        'Feature': feature_cols,
                        'Value': [selected_row[col] for col in feature_cols]
                    })
                    st.dataframe(feature_df, width='stretch', hide_index=True)
                
                # Verification actions
                st.divider()
                st.markdown("### Verify this transaction:")
                
                col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
                
                with col_btn1:
                    if st.button("Confirm FRAUD", type="primary", width='stretch'):
                        try:
                            update_prediction_and_history(transaction_id, 1, True)  # Keep result=1, set checked=True
                            st.success(f"Transaction `{transaction_id}` confirmed as FRAUD")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating: {e}")
                
                with col_btn2:
                    if st.button("NOT Fraud", type="secondary", width='stretch'):
                        try:
                            update_prediction_and_history(transaction_id, 0, True)  # Change result=0, set checked=True
                            st.success(f"Transaction `{transaction_id}` marked as NOT FRAUD")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating: {e}")
    
    except Exception as e:
        st.error(f"Error loading fraud data: {e}")

# ==================== TAB 2: TRANSACTION SEARCH ====================
with tab2:
    st.header("Transaction Search & Status")
    st.markdown("Search for any transaction and view its current status")
    
    # Search input
    search_id = st.text_input("Enter Transaction ID:", key="search_input")
    
    if st.button("Search", type="primary", key="search_btn") or search_id:
        if search_id:
            try:
                # First, search in prediction + input (active transactions)
                df_active = search_transaction_in_prediction(search_id)
                
                if not df_active.empty:
                    # Found in active tables
                    st.success(f"Transaction `{search_id}` found in **active queue**")
                    
                    row = df_active.iloc[0]
                    
                    # Display status
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        result_label = "FRAUD (1)" if row['prediction_result'] == 1 else "NOT FRAUD (0)"
                        st.metric("Prediction Result", result_label)
                    with col2:
                        checked_label = "Verified" if row['checked'] else "Pending"
                        st.metric("Status", checked_label)
                    with col3:
                        st.metric("Location", "Active Queue")
                    
                    st.divider()
                    
                    # Show transaction details
                    st.markdown("### Transaction Data")
                    feature_cols = [col for col in df_active.columns if col not in ['prediction_result', 'checked', 'Class']]
                    st.dataframe(df_active[feature_cols], width='stretch', hide_index=True)
                    
                    # Allow update
                    st.divider()
                    st.markdown("### Update Prediction")
                    
                    
                    if row['prediction_result'] == 0:
                        if st.button("Mark as FRAUD", type="primary", width='stretch', key="mark_fraud"):
                            try:
                                update_prediction_and_history(search_id, 1, True)
                                st.success(f"Updated `{search_id}` to FRAUD")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                    
                    else:
                        st.markdown("""
                            <style>
                            div.stButton > button[kind="secondary"] {
                                background-color: #0ea5e9 !important;
                                color: white !important;
                                border: none !important;
                            }
                            div.stButton > button[kind="secondary"]:hover {
                                background-color: #0284c7 !important;
                            }
                            </style>
                        """, unsafe_allow_html=True)
                        if st.button("Mark as NOT FRAUD", type="secondary", width='stretch', key="mark_not_fraud",):
                            try:
                                update_prediction_and_history(search_id, 0, True)
                                st.success(f"Updated `{search_id}` to NOT FRAUD")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                
                else:
                    # Not in active queue, search in raw_data
                    df_raw = search_transaction_in_raw(search_id)
                    
                    if not df_raw.empty:
                        st.info(f"Transaction `{search_id}` found in **finalized data** (raw_data)")
                        
                        row = df_raw.iloc[0]
                        
                        # Display status
                        col1, col2 = st.columns(2)
                        with col1:
                            class_label = "FRAUD (1)" if row.get('Class', 0) == 1 else "NOT FRAUD (0)"
                            st.metric("Final Class", class_label)
                        with col2:
                            st.metric("Location", "Raw Data (Finalized)")
                        
                        st.divider()
                        
                        # Show transaction details
                        st.markdown("### Transaction Data")
                        feature_cols = [col for col in df_raw.columns if col not in ['prediction_result', 'checked', 'Class']]
                        st.dataframe(df_raw[feature_cols], width='stretch', hide_index=True)
                        # st.dataframe(df_raw, width='stretch')
                        
                        # st.info("This transaction has been finalized and cannot be modified")

                         # Allow update in raw_data
                        st.divider()
                        st.markdown("### Update Finalized Data")
                        st.warning("This will update the finalized data in raw_data and history_db")
                        
                        if row.get('Class', 0) == 0:
                            if st.button("Mark as FRAUD", type="primary", width='stretch', key="mark_fraud_raw"):
                                try:
                                    update_raw_and_history(search_id, 1)
                                    st.success(f"Updated `{search_id}` to FRAUD in raw_data")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        else:
                            st.markdown("""
                                <style>
                                div.stButton > button[kind="secondary"] {
                                    background-color: #0ea5e9 !important;
                                    color: white !important;
                                    border: none !important;
                                }
                                div.stButton > button[kind="secondary"]:hover {
                                    background-color: #0284c7 !important;
                                }
                                </style>
                            """, unsafe_allow_html=True)
                            if st.button("Mark as NOT FRAUD", type="secondary", width='stretch', key="mark_not_fraud_raw"):
                                try:
                                    update_raw_and_history(search_id, 0)
                                    st.success(f"Updated `{search_id}` to NOT FRAUD in raw_data")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                    
                    else:
                        st.warning(f"Transaction `{search_id}` not found in any table")
            
            except Exception as e:
                st.error(f"Error searching: {e}")
        else:
            st.info("👆 Enter a transaction ID to search")

with tab3:
    
    looker_url = "https://lookerstudio.google.com/embed/reporting/3633feef-9528-42d1-b87c-c39a976ec509/page/4vFfF"
    
    st.components.v1.iframe(
        src=looker_url,
        height=800,
        scrolling=True
    )