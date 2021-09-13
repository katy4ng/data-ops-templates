# Databricks notebook source
import json

import requests

# COMMAND ----------

REGION = {region}
NAME_SPACE={name_space}
CLIENT={client}


BASE_URL = f"https://{REGION}-identity-history.camplexer.com/identity_history"

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": dbutils.secrets.get(scope="customer-api", key="x-api-key"),
}

# COMMAND ----------

#
# https://lexerdev.github.io/customer-api/redoc#operation/create_enrichment_job


def run_dataflow(enrichment_id):
    "Run a specific dataflow"
    payload = {"service": "Test", "enrichment_id": enrichment_id}
    response = requests.post(
        f"{BASE_URL}/jobs/enrichment",
        headers=HEADERS,
        json=payload,
    )
    assert response.ok, response.text
    return response.json()


def create_enrichment(name=str, description=str, bucket=str, namespace=str):
    "Create an enrichment."
    enrichment_config = {
        "name": name,
        "description": description,
        "bucket": bucket,
        "namespace": namespace,
    }
    response = requests.post(
        f"{BASE_URL}/enrichments",
        headers=HEADERS,
        json=enrichment_config,
    )
    assert response.ok, response.text
    return response.json()


def get_enrichment_config(enrichment_id: str):
    "Request config for a specific dataflow"
    response = requests.get(
        f"{BASE_URL}/enrichments/{enrichment_id}",
        headers=HEADERS,
    )
    assert response.ok, response.text
    return response.json()


def update_enrichment_config(enrichment_id: str, name=str, description=str, bucket=str, namespace=str):
    "Run a specific dataflow"
    enrichment_config = {
        "name": name,
        "description": description,
        "bucket": bucket,
        "namespace": namespace,
    }
    response = requests.put(
        f"{BASE_URL}/enrichments/{enrichment_id}",
        headers=HEADERS,
        json=enrichment_config,
    )
    assert response.ok, response.text
    return response.json()


def run_enrichment_job(service: str, enrichment_id: str, n_workers: str = 2, send_to_decorator: bool = True):
    "Run a specific enrichment"
    enrichment_job = {
        "service": service,
        "n_workers": n_workers,
        "enrichment_id": enrichment_id,
        "send_to_decorator": send_to_decorator,
    }
    response = requests.post(f"{BASE_URL}/jobs/enrichment", headers=HEADERS, json=enrichment_job)
    assert response.ok, response.text
    return response.json()


def get_enrichment_status(job_id):
    "Run a specific enrichment"
    response = requests.get(
        f"{BASE_URL}/jobs/enrichment/{job_id}",
        headers=HEADERS,
    )
    assert response.ok, response.text
    return response.json()

# COMMAND ----------

# MAGIC %md ## Spend decile

# COMMAND ----------

spend_decile_response=create_enrichment(name="Spend Decile", description="Spend Decile from a profile's total spend.", bucket=f"lexer-client-{CLIENT}", namespace=f"{NAME_SPACE}")
print(json.dumps(spend_decile_response,indent=4))

# COMMAND ----------

# MAGIC %md ## Inferred Gender

# COMMAND ----------

ig_response=create_enrichment(name="Inferred Gender", description="Inferred Gender from a profile's first name.", bucket=f"lexer-client-{CLIENT}", namespace=f"{NAME_SPACE}")
print(json.dumps(ig_response,indent=4)) 

# COMMAND ----------

# MAGIC %md ## LTV

# COMMAND ----------

ltv_response=create_enrichment(name="LTV Attributes", description="LTV attributes calculated from historic purchases and predicts Churn Risk, Predicted Orders, Predicted Spend, Predicted Order Value for the next 12 months.", bucket=f"lexer-client-{CLIENT}", namespace=f"{NAME_SPACE}")
print(json.dumps(ltv_response,indent=4)) 

# COMMAND ----------

# MAGIC %md ## Recommander

# COMMAND ----------

recommander=create_enrichment(name="Product Recommender Attributes", description="Product Recommender attributes calculated based on products purchased by similar users.", bucket=f"lexer-client-{CLIENT}", namespace=f"{NAME_SPACE}")
print(json.dumps(recommander,indent=4)) 

# COMMAND ----------


