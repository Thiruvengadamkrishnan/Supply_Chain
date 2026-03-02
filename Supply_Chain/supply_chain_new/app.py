import streamlit as st
import pandas as pd
from databricks import sql
import requests
import os
import re
import time

st.set_page_config(page_title="Silver Layer Analytics Assistant", layout="wide")

st.title("🚚 Supply Chain Silver Layer Assistant")
st.caption("Natural Language → Spark SQL → Databricks → Visualization")

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not MISTRAL_API_KEY:
    st.error("MISTRAL_API_KEY not found in environment.")
    st.stop()

if not DATABRICKS_TOKEN:
    st.error("DATABRICKS_TOKEN not found in environment.")
    st.stop()

try:
    conn = sql.connect(
        server_hostname="dbc-d104875c-6e43.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/63203cdd9ed56ffb",
        access_token=DATABRICKS_TOKEN
    )
except Exception as e:
    st.error(f"Databricks connection failed: {e}")
    st.stop()

SCHEMA_CONTEXT = """
Table: supply_chain.silver.car_orders_clean

Columns:
order_date, city, country, vehicle_brand, vehicle_model,
transport_mode, shipment_date, delay_days,
rating, delivery_duration_days, is_late_delivery

Rules:
- Generate Spark SQL only
- Must use supply_chain.silver.car_orders_clean
- Output must start with SELECT
- No explanations
"""

def generate_sql(question):

    prompt = f"""
You are a supply chain SQL assistant.

{SCHEMA_CONTEXT}

Question:
{question}

Return ONLY Spark SQL query.
"""

    payload = {
        "model": "mistral-large-latest",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 200
    }

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=30
    )

    result = response.json()
    return result["choices"][0]["message"]["content"]

def extract_sql(text):
    text = text.replace("```sql", "").replace("```", "")
    match = re.search(r"SELECT.*", text, re.DOTALL | re.IGNORECASE)
    return match.group(0).strip() if match else ""

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

query = st.chat_input("Ask a question about supply chain data...")

if query:

    st.session_state.messages.append({"role": "user", "content": query})

    with st.chat_message("assistant"):
        with st.spinner("Generating SQL and executing query..."):

            raw_sql = generate_sql(query)
            clean_sql = extract_sql(raw_sql)

            if not clean_sql:
                st.error("SQL generation failed.")
                st.stop()

            st.subheader("Generated SQL")
            st.code(clean_sql)

            cursor = conn.cursor()
            cursor.execute(clean_sql)

            data = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            df = pd.DataFrame(data, columns=columns)

            st.subheader("Query Result")
            st.dataframe(df, use_container_width=True)

            if len(df.columns) >= 2:
                st.subheader("Visualization")
                st.line_chart(df)
