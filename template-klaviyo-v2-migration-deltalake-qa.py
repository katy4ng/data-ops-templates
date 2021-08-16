# Databricks notebook source
import hashlib
import json
import re
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame, Column
import pandas as pd
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
import plotly.express as px

pd.options.plotting.backend = "plotly"


# COMMAND ----------

BUCKET={bucket}
REGION={region}
v1_dataset_id={id1}
v2_dataset_id={id2}

# COMMAND ----------

identity_history_table = spark.read.format("delta").load(
    f"s3://{BUCKET}/customerapi/{REGION}-identity-history/lake/datasets/delta/identity_history"
)

identity_history_table_email=identity_history_table.where(F.col("dataset_id").isin([v1_dataset_id,v2_dataset_id]))

# COMMAND ----------

def gather_email_events(email_types):
    return (
        identity_history_table_email.where(F.col("type") == email_types)
        .select("dataset_id",
                "link_value",
                "history_key",
                "action_at",
                f"record.{email_types}.email_id",
                "type")
    )

def union_many(*dfs):
    return reduce(DataFrame.union, dfs)

# COMMAND ----------

email_types = [
    "email_send",
    "email_open",
  "email_click",
  "email_subscribe",
  "email_bounce"
]

email_events = (
    union_many(*(gather_email_events(e_type) for e_type in email_types))
  .withColumn('date',F.to_timestamp("action_at"))
)

# COMMAND ----------

email_events.groupby('dataset_id').count().display()

# COMMAND ----------

v1_events=email_events.where(F.col('dataset_id')==v1_dataset_id)
v2_events=email_events.where(F.col('dataset_id')==v2_dataset_id)

# COMMAND ----------

def email_count_by_type(dataframe,event_type,version):
  return dataframe.where(F.col('type')==event_type).groupby(F.date_trunc('week','date').alias('date')).agg(F.countDistinct('link_value').alias(f'{version}_{event_type}_identity'))

# COMMAND ----------

def compare_counts(v1_df,v2_df):
  joined_df=v1_df.join(v2_df,on='date',how='full_outer')
  return joined_df

# COMMAND ----------

send_compare=compare_counts(email_count_by_type(v1_events,'email_send','v1'),email_count_by_type(v2_events,'email_send','v2'))

# COMMAND ----------

send_compare.toPandas().set_index('date').plot()

# COMMAND ----------

open_compare=compare_counts(email_count_by_type(v1_events,'email_open','v1'),email_count_by_type(v2_events,'email_open','v2'))

# COMMAND ----------

  open_compare.toPandas().set_index('date').plot()

# COMMAND ----------

click_compare=compare_counts(email_count_by_type(v1_events,'email_click','v1'),email_count_by_type(v2_events,'email_click','v2'))

# COMMAND ----------

click_compare.toPandas().set_index('date').plot()

# COMMAND ----------

# MAGIC %md ### Overall v2 is very healthy. There were two weeks where v1 showed less values than v2. I think that's just because v1 integration failed on that day.

# COMMAND ----------

def campaign_count(df,version,event_type):
  return (df.where(F.col('type')==event_type)
   .select("link_value","email_id")
   .groupby('email_id')
   .agg(F.countDistinct('link_value').alias('link_count'))
    .orderBy(F.desc('link_count'))
    .limit(10)
    .withColumn('version',F.lit(version)))

# COMMAND ----------

# MAGIC %md ### Top 10 Campaign clicked

# COMMAND ----------

campaign_compare=campaign_count(v1_events,'v1','email_click').union(campaign_count(v2_events,'v2','email_click')).toPandas()
fig = px.bar(campaign_compare, x="email_id", y="link_count",
             color='version', barmode='group',
             height=400)
fig.show()

# COMMAND ----------

# MAGIC %md ### Top 10 Campaign send

# COMMAND ----------

campaign_compare=campaign_count(v1_events,'v1','email_send').union(campaign_count(v2_events,'v2','email_send')).toPandas()
fig = px.bar(campaign_compare, x="email_id", y="link_count",
             color='version', barmode='group',
             height=400)
fig.show()

# COMMAND ----------

# MAGIC %md ### Top 10 Campaign opend

# COMMAND ----------

campaign_compare=campaign_count(v1_events,'v1','email_send').union(campaign_count(v2_events,'v2','email_send')).toPandas()
fig = px.bar(campaign_compare, x="email_id", y="link_count",
             color='version', barmode='group',
             height=400)
fig.show()

# COMMAND ----------


