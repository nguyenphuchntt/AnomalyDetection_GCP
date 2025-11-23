# cd streamlit
# pip install -r requirements.txt
# streamlit run app.py

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from dotenv import load_dotenv
import hashlib
import hmac

load_dotenv()

# Page config
st.set_page_config(
    page_title="Fraud Detection Admin",
    layout="wide"
)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID")  
DATASET_ID = os.getenv("DATASET_ID")  
PREDICTION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.prediction_data"
INPUT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.data_input_test"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw-data"
HISTORY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.history_db"
USERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.users"

# ==================== AUTHENTICATION FUNCTIONS ====================

def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed_password):
    """Verify password against hash"""
    return hash_password(password) == hashed_password

@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client"""

    # Try local file first (for local development)
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if credentials_path and os.path.exists(credentials_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            return client
        except Exception as e:
            st.sidebar.warning(f"Local auth failed: {e}")
    
    # Try environment variable JSON (for AWS deployment)
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json:
        try:
            service_account_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            return client
        except Exception as e:
            st.error(f"Error loading credentials: {e}")
            st.stop()
    
    st.error("No credentials found!")
    st.stop()

client = get_bigquery_client()

def create_user(username, name, password, role="user"):
    """Create new user in BigQuery"""
    password_hash = hash_password(password)
    query = f"""
    INSERT INTO `{USERS_TABLE}` (username, name, password_hash, role)
    VALUES (@username, @name, @password_hash, @role)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("name", "STRING", name),
            bigquery.ScalarQueryParameter("password_hash", "STRING", password_hash),
            bigquery.ScalarQueryParameter("role", "STRING", role)
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Error creating user: {e}")
        return False

def check_user_exists(username):
    """Check if username already exists"""
    query = f"""
    SELECT COUNT(*) as count
    FROM `{USERS_TABLE}`
    WHERE username = @username
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username)
        ]
    )
    result = client.query(query, job_config=job_config).to_dataframe()
    return result['count'].iloc[0] > 0

def authenticate_user(username, password):
    """Authenticate user credentials"""
    query = f"""
    SELECT username, name, password_hash, role
    FROM `{USERS_TABLE}`
    WHERE username = @username
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username)
        ]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return None
        
        user = df.iloc[0]
        if verify_password(password, user['password_hash']):
            return {
                'username': user['username'],
                'name': user['name'],
                'role': user['role']
            }
        return None
    except Exception as e:
        st.error(f"Authentication error: {e}")
        return None

def login_form():
    """Display login form"""
    # Add custom CSS for centered login box
    st.markdown("""
        <style>
        .login-container {
            max-width: 450px;
            margin: 100px auto;
            padding: 40px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .login-header {
            text-align: center;
            margin-bottom: 30px;
        }
        .login-title {
            font-size: 28px;
            font-weight: 600;
            color: #1f2937;
            margin-bottom: 8px;
        }
        .login-subtitle {
            font-size: 16px;
            color: #6b7280;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Center the form using columns
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("<div class='login-header'>", unsafe_allow_html=True)
        st.markdown("<div class='login-title'>Fraud Detection System</div>", unsafe_allow_html=True)
        st.markdown("<div class='login-subtitle'>Login to Continue</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            submit = st.form_submit_button("Login", type="primary", use_container_width=True)
            
            if submit:
                if not username or not password:
                    st.error("Please enter both username and password")
                    return
                
                user = authenticate_user(username, password)
                if user:
                    st.session_state['authenticated'] = True
                    st.session_state['user'] = user
                    st.success(f"Welcome back, {user['name']}!")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
        
        st.divider()
        
        col_text, col_btn = st.columns([2, 1])
        with col_text:
            st.markdown("<p style='margin-top: 8px;'>Don't have an account?</p>", unsafe_allow_html=True)
        with col_btn:
            if st.button("Register", use_container_width=True):
                st.session_state['show_register'] = True
                st.rerun()

def register_form():
    """Display registration form"""
    st.title("Create New Account")
    
    with st.form("register_form"):
        name = st.text_input("Full Name")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        password_confirm = st.text_input("Confirm Password", type="password")
        
        col1, col2 = st.columns(2)
        with col1:
            submit = st.form_submit_button("Register", type="primary", use_container_width=True)
        with col2:
            cancel = st.form_submit_button("Back to Login", use_container_width=True)
        
        if cancel:
            st.session_state['show_register'] = False
            st.rerun()
        
        if submit:
            if not all([name, username, password, password_confirm]):
                st.error("Please fill in all fields")
                return
            
            if password != password_confirm:
                st.error("Passwords do not match")
                return
            
            if len(password) < 6:
                st.error("Password must be at least 6 characters")
                return
            
            if check_user_exists(username):
                st.error("Username already exists")
                return
            
            if create_user(username, name, password):
                st.success("Account created successfully! Please login.")
                st.session_state['show_register'] = False
                st.rerun()

# ==================== HELPER FUNCTIONS (Same as before) ====================

def get_pending_frauds():
    """Get all fraud predictions pending verification"""
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
    """Search in prediction + input tables"""
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
    """Search in raw_data"""
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

# Initialize session state
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'show_register' not in st.session_state:
    st.session_state['show_register'] = False

# Check authentication
if not st.session_state['authenticated']:
    if st.session_state.get('show_register', False):
        register_form()
    else:
        login_form()
    st.stop()

# Authenticated UI
user = st.session_state['user']

# Sidebar with user info
with st.sidebar:
    st.markdown(f"### Welcome, {user['name']}!")
    st.markdown(f"**Role:** {user['role']}")
    st.markdown(f"**Username:** {user['username']}")
    st.divider()
    if st.button("Logout", type="primary", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user'] = None
        st.rerun()

# Main content
st.title("Fraud Detection Admin Interface")

tab1, tab2, tab3 = st.tabs(["Fraud Alert & Review", "Transaction Search", "Dashboard"])

# ==================== TAB 1: FRAUD ALERT ====================
with tab1:
    st.header("Pending Fraud Verification")

    if st.button("Refresh Data", key="refresh_tab1"):
        st.cache_data.clear()
        st.rerun()
    
    try:
        df_frauds = get_pending_frauds()
        
        if df_frauds.empty:
            st.success("No pending fraud cases to review!")
        else:
            st.warning(f"**{len(df_frauds)} fraud cases** pending verification")
            
            display_cols = ['transaction_id', 'prediction_result']
            other_cols = [col for col in df_frauds.columns if col not in display_cols and col != 'checked']
            display_df = df_frauds[display_cols + other_cols]
            
            selected_event = st.dataframe(
                display_df,
                width='stretch',
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun"
            )
            
            if selected_event and len(selected_event.selection.rows) > 0:
                selected_idx = selected_event.selection.rows[0]
                selected_row = df_frauds.iloc[selected_idx]
                transaction_id = selected_row['transaction_id']
                
                st.divider()
                st.subheader(f"Transaction Details: `{transaction_id}`")
                
                col_left, col_right = st.columns(2)
                
                with col_left:
                    st.markdown("### Prediction Info")
                    st.metric("Prediction Result", "FRAUD (1)" if selected_row['prediction_result'] == 1 else "NOT FRAUD (0)")
                    st.metric("Checked", selected_row['checked'])
                
                with col_right:
                    st.markdown("### Transaction Features")
                    feature_cols = [col for col in selected_row.index if col not in ['transaction_id', 'prediction_result', 'checked']]
                    feature_df = pd.DataFrame({
                        'Feature': feature_cols,
                        'Value': [selected_row[col] for col in feature_cols]
                    })
                    st.dataframe(feature_df, width='stretch', hide_index=True)
                
                st.divider()
                st.markdown("### Verify this transaction:")
                
                col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
                
                with col_btn1:
                    if st.button("Confirm FRAUD", type="primary", width='stretch'):
                        try:
                            update_prediction_and_history(transaction_id, 1, True)
                            st.success(f"Transaction `{transaction_id}` confirmed as FRAUD")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating: {e}")
                
                with col_btn2:
                    if st.button("NOT Fraud", type="secondary", use_container_width=True):
                        try:
                            update_prediction_and_history(transaction_id, 0, True)
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
    
    search_id = st.text_input("Enter Transaction ID:", key="search_input")
    
    if st.button("Search", type="primary", key="search_btn") or search_id:
        if search_id:
            try:
                df_active = search_transaction_in_prediction(search_id)
                
                if not df_active.empty:
                    st.success(f"Transaction `{search_id}` found in **active queue**")
                    row = df_active.iloc[0]
                    
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
                    st.markdown("### Transaction Data")
                    feature_cols = [col for col in df_active.columns if col not in ['prediction_result', 'checked', 'Class']]
                    st.dataframe(df_active[feature_cols], width='stretch', hide_index=True)
                    
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
                        st.info(f"Transaction `{search_id}` found in **finalized data**")
                        row = df_raw.iloc[0]
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            class_label = "FRAUD (1)" if row.get('Class', 0) == 1 else "NOT FRAUD (0)"
                            st.metric("Final Class", class_label)
                        with col2:
                            st.metric("Location", "Raw Data (Finalized)")
                        
                        st.divider()
                        st.markdown("### Transaction Data")
                        feature_cols = [col for col in df_raw.columns if col not in ['prediction_result', 'checked', 'Class']]
                        st.dataframe(df_raw[feature_cols], width='stretch', hide_index=True)
                        
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

# ==================== TAB 3: DASHBOARD ====================
with tab3:
    looker_url = "https://lookerstudio.google.com/embed/reporting/3633feef-9528-42d1-b87c-c39a976ec509/page/4vFfF"
    
    st.components.v1.iframe(
        src=looker_url,
        height=800,
        scrolling=True
    )