# Databricks notebook source
# MAGIC %md
# MAGIC Enrichment that uses a dataset as a source
# MAGIC 

# COMMAND ----------

# Databricks Librarys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Column

# Python Librarys
import json

# COMMAND ----------

# CLIENT CONSTANTS
REGION = load_parameter("region")
BUCKET = load_parameter("bucket")

# ENRICHMENT DETAILS
ENRICHMENT_CONFIG = load_parameter("enrichment_config")
ENRICHMENT_ID = ENRICHMENT_CONFIG["id"]
ENRICHMENT_PATH = ENRICHMENT_CONFIG["path"]
DATASET_LIST = load_parameter("dataset_list")


# COMMAND ----------

# ToDo: Functions Description

# Dataset reading functions
def read_table_records(table: str,  dataset_ids: list, bucket=BUCKET, region=REGION) -> DataFrame:
    """
    Reads in specified table from a list of dataset_ids. 'table' variable can be either identity_history,
    entity_record or customer_record.
    """
    return (
        spark.read.format("delta")
        .load(f"s3://{bucket}/customerapi/{region}-identity-history/lake/datasets/delta/{table}/")
        .where(F.col("dataset_id").isin(dataset_ids))
    )

# COMMAND ----------

# Transform Functions

def source_to_persist(df: DataFrame) -> DataFrame:
    """
    This function should be used to apply any uniquing 
    or dedupling to the source table/s.
    e.g
    enrichment_persist = persist_to_fit(enrichment_source)
    """
    return df

def persist_to_fit(df: DataFrame) -> DataFrame:
    """
    This function should be used to apply any formatting 
    or logic to the persist table/s.
    e.g
    enrichment_fit = persist_to_fit(enrichment_persist)
    """
    return df

def fit_to_export(df: DataFrame) -> DataFrame:
    """
    This function should be used to group by unified link and 
    return the enrichment table/s.
    e.g
    enrichment_export = persist_to_fit(enrichment_fit)
    """
    return df

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Run
# MAGIC Below contains the transformations from source to the enrichment table

# COMMAND ----------

enrichment_persist = persist_to_fit(enrichment_source)

enrichment_fit = persist_to_fit(enrichment_persist)

enrichment_export = persist_to_fit(enrichment_fit)

# COMMAND ----------

enrichment_export.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ENRICHMENT_PATH)

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps(
        {
            "id": ENRICHMENT_ID,
            "bucket": BUCKET,
            "path": ENRICHMENT_PATH,
        }
    )
)
