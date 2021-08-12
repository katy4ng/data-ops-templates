# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **How to use this template:**   
# MAGIC Name the file with the "date-eda-task" format. EG: "20-06-24-eda-duplicate_skus"   
# MAGIC Clone the file to a folder in your workspace that follows the lexer-client-format Eg: "lexer-client-datarockstar"   
# MAGIC Follow the basic structure laid out in the notebook.

# COMMAND ----------

from data_toolkit.s3_helpers import S3Utils

# Databricks Librarys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Column

# Python Librarys
import json
import black

INTERACTIVE = True

# COMMAND ----------

# ToDo: Functions Description

# Dataset reading functions
def read_table_records( table: str,  dataset_ids: list,bucket=BUCKET,region=REGION) -> DataFrame:
    """
    Reads in specified table from a list of dataset_ids. 'table' variable can be either identity_history,
    entity_record or customer_record.
    """
    return (
        spark.read.format("delta")
        .load(f"s3://{bucket}/customerapi/{region}-identity-history/lake/datasets/delta/{table}/")
        .where(F.col("dataset_id").isin(dataset_ids))
    ).persist()

def explode_order_tables(df: DataFrame) -> DataFrame:
    '''
    parsse_dataset loops through the 4 different transaction tables
    and pulls out certain defined fields. These tables are unioned
    together and a df is returned.
    '''
    def parse_tables(df_table: DataFrame, table: str):
        '''
        parse_tables accepts a DataFrame and table type and grabs
        defined columns.
        '''
        return df_table.select(
            "updated_at",
            "action_at",
            "history_key",
            "type",
            "link_type",
            "link_value",
            F.explode(f"record.{table}.products")).select(
            "*",
        )

    table_types = [
        'ecommerce_purchase',
        'ecommerce_return',
        'instore_purchase',
        'instore_return'
    ]

    df_purchase = None
    df_return = None
    
    for table_type in table_types:
        if table_type in ["ecommerce_purchase","instore_purchase"]:
            if df_purchase == None:
                df_purchase = parse_tables(df,table_type)
            df_purchase = df_purchase.union(parse_tables(df,table_type))
        elif table_type in ["ecommerce_return","instore_return"]:
            if df_return == None:
                df_return = parse_tables(df,table_type)
            df_return = df_return.union(parse_tables(df,table_type))

    return df_purchase,df_return
