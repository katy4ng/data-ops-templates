# Databricks notebook source
# notebook for investigating duplications on history keys, and the effect of duplications in returns on total spend

# COMMAND ----------

import hashlib
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame, Column

# COMMAND ----------

def parse_dataset(df: DataFrame) -> DataFrame:
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
            "action_at",
            "history_key",
            "type",
            "link_type",
            "link_value",
            "col.quantity",
            "col.price_paid",
            "col.name",
            "col.entity_ref.id",
        )
    
    table_types = [
        'ecommerce_purchase',
        'ecommerce_return',
        'instore_purchase',
        'instore_return'
    ]
    
    schema = (
        T.StructType([
            T.StructField("action_at",T.TimestampType(),True),
            T.StructField("history_key",T.StringType(),True),
            T.StructField("type",T.StringType(),True),
            T.StructField("link_type",T.StringType(),True),
            T.StructField("link_value",T.StringType(),True),
            T.StructField("quantity",T.LongType(),True),
            T.StructField("price_paid",T.DoubleType(),True),
            T.StructField("name",T.StringType(),True),
            T.StructField("id",T.StringType(),True)
        ])
    )
    
    df_result = spark.createDataFrame([], schema)
    
    for table_type in table_types:
        df_result = df_result.union(parse_tables(df,table_type))
    
    return df_result

# COMMAND ----------

# DBTITLE 1,Client
REGION = ''
BUCKET = 'lexer-client-'
CLIENT = ''
DATASET_ID = ''

order_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider=shopify/account_id=*/year=*/month=*/day=*/orders_*.ndjson.gz"
)

orders_delta = (
    spark.read.format("delta")
    .load(f"s3://{BUCKET}/customerapi/{REGION}-identity-history/lake/datasets/delta/identity_history/")
    .where(F.col("dataset_id").isin(DATASET_ID))
)

# COMMAND ----------

selected_cols = parse_dataset(orders_delta)

# COMMAND ----------

# summary stats on all orders

print('returns with bad ids (duplicated returns), isolating the bad returns')
selected_cols.where(F.length(F.col("history_key")) > 7 ).agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

print('good ids for returns & purchases, what we are aiming for the hub to show')
selected_cols.where(F.length(F.col("history_key")) <= 7).agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

print('all history_keys, near to what the hub will display currently')
selected_cols.agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

# COMMAND ----------

# hub gives total spend as _

((54574722-1066425)/54574722)*100


# COMMAND ----------

# sort based on quantity < 0 for returns only

print('returns with bad ids (duplicated returns), isolating the bad returns')
selected_cols.where((F.length(F.col("history_key")) > 7) & (F.col("quantity") < 0)).agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

print('good ids for returns & purchases, what we are aiming for the hub to show')
selected_cols.where((F.length(F.col("history_key")) <= 7) & (F.col("quantity") < 0)).agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

print('all history_keys, near to what the hub will display currently')
selected_cols.where(F.col("quantity") < 0).agg(F.sum("price_paid"),F.sum("quantity"),F.countDistinct("history_key")).display()

# COMMAND ----------

# prep for QA:

# COMMAND ----------

selected_cols.where(F.col("link_value") == '{example_email}').display()
selected_cols.where(F.col("link_value") == '{example_email}').select("history_key","type","link_type","link_value").display()
order_source.where(F.col("email") == '{example_email}').select("order_number","cancel_reason","financial_status","refunds.id").distinct().display()

# COMMAND ----------

selected_cols.groupBy(F.length(F.col("history_key")) > 7 ).count().display()

# COMMAND ----------

selected_cols.groupBy("history_key").count().count()

# COMMAND ----------

orders_delta.select(F.col("record")).count()

# COMMAND ----------

# QA immediately post purge/confirm purge

orders_purged = (
    spark.read.format("delta")
    .load(f"s3://{BUCKET}/customerapi/{REGION}-identity-history/lake/datasets/delta/identity_history/")
    .where(F.col("dataset_id").isin(DATASET_ID))
)

orders_purged.display()

# COMMAND ----------

# QA after dataset reload

order_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider=shopify/account_id=*/year=*/month=*/day=*/orders_*.ndjson.gz"
)

orders_purged_reloaded = (
    spark.read.format("delta")
    .load(f"s3://{BUCKET}/customerapi/{REGION}-identity-history/lake/datasets/delta/identity_history/")
    .where(F.col("dataset_id").isin(DATASET_ID))
)

# COMMAND ----------

post_purge_df = parse_dataset(orders_purged_reloaded)

# COMMAND ----------

post_purge_df.where(F.col("link_value") == '{example_email}').display()
post_purge_df.where(F.col("link_value") == '{example_email}').select("history_key","type","link_type","link_value").display()
order_source.where(F.col("email") == '{example_email}').select("order_number","cancel_reason","financial_status","refunds.id").distinct().display()

# COMMAND ----------

post_purge_df.groupBy(F.length(F.col("history_key")) > 7 ).count().display()

# COMMAND ----------

post_purge_df.groupBy("history_key").count().count()

# COMMAND ----------

orders_purged_reloaded.select(F.col("record")).count()
