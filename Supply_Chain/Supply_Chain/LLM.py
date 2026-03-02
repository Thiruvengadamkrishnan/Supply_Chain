# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests
import re

MISTRAL_API_KEY = "8tqGbDTeFmzZj49PkNH4kwuOHJCGkAdS"

URL = "https://api.mistral.ai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {MISTRAL_API_KEY}",
    "Content-Type": "application/json"
}

# COMMAND ----------

SCHEMA_CONTEXT = """
Table: supply_chain.silver.car_orders_clean

Columns:
order_date, city, country, vehicle_brand, vehicle_model,
transport_mode, shipment_date, delay_days,
rating, delivery_duration_days, is_late_delivery

Rules:
Generate Spark SQL only.
Output must start with SELECT.
Do not explain.
"""

# COMMAND ----------

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
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 150
    }

    response = requests.post(
        URL,
        headers=headers,
        json=payload
    )

    result = response.json()

    try:
        return result["choices"][0]["message"]["content"]

    except:
        return ""

# COMMAND ----------

def extract_sql(text):

    if text is None:
        return ""

    # Remove markdown SQL fences
    text = text.replace("```sql", "")
    text = text.replace("```", "")

    # Extract SELECT query only
    import re
    match = re.search(r"SELECT.*", text, re.DOTALL | re.IGNORECASE)

    if match:
        return match.group(0).strip()

    return ""

# COMMAND ----------

def supply_chain_chatbot(question):

    raw_sql = generate_sql(question)

    print("Raw LLM Output:\n", raw_sql)

    sql_query = extract_sql(raw_sql)

    print("Clean SQL:\n", sql_query)

    if sql_query == "":
        return "SQL generation failed"

    df = spark.sql(sql_query)

    return df

# COMMAND ----------

display(
    supply_chain_chatbot(
        "when did the highest delay date"
    )
)

# COMMAND ----------

display(
    supply_chain_chatbot(
        "give me monthwise average delay "
    )
)