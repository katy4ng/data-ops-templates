# Databricks notebook source
# Databricks Librarys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Column
from pyspark.sql.window import Window

# Python Librarys
import json
from typing import List, Dict, Any,Optional
from operator import itemgetter

from data_toolkit.util import spark, dbutils
from data_toolkit.config import JobConfig

# COMMAND ----------

class MyJobConfig(JobConfig):
    region: str 
    bucket: str
    client: str
    dataset_ids: List[str]
    flag_config: List[Dict]
    enrichment_config: Optional[Dict]

# COMMAND ----------

Config=MyJobConfig.from_dbutils(dbutils) 

for config in Config:
    print(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flagging Enrichment
# MAGIC This enrichment is a generialise enrichment for flagging profiles and creating record/s based on conditions definied in the config. To flag profiles, all you need in a list of sources with a list of condition/s and key pairs and the rest is handled.
# MAGIC 1. Sources
# MAGIC   * 'customer_record' - pass through to flag using profiles PII or custom_fields for a specific client.
# MAGIC   * 'identity_history' - pass through to flag using transactions for a specific client.
# MAGIC 2. Attribute Records
# MAGIC For each source, a list of conditions + keys are needed to flag on
# MAGIC   * condition has 3 fields - method, column and value
# MAGIC     * method
# MAGIC       * 'isin' - flag profiles with values in a specific column ::list/str
# MAGIC       * 'equals' - flag profiles based on a column equalling a certain value ::str
# MAGIC     * column - this is the column that the method is getting applied to
# MAGIC     * value - the value which is being compared to the given column
# MAGIC   * key - this is what the record attribute key will be (name of column in enrichment export)

# COMMAND ----------

print(json.dumps(Config.flag_config, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def get_keys() -> List:
    res = []
    for source in Config.flag_config:
        tmp = list(map(itemgetter('key'), source['attribute_records']))
        res = res + tmp
    return res

def parse_condition(method,column,value):
    if method == 'isin':
        return (F.col(column).isin(value))
    if method == 'equals':
        return (F.col(column) == value)


def parse_source(flag_config: Dict) -> DataFrame:
    # check if dataset table
    if flag_config["source"] in ["customer_record","identity_history"]:
        path_to_table = f's3://{Config.bucket}/customerapi/{Config.region}-identity-history/lake/datasets/delta/{flag_config["source"]}/'
        table_record = (
            spark.read.format("delta")
            .load(path_to_table)
        )
    else:
        source_path = flag_config["source"]
        table_record = (
            spark.read.format("delta")
            .load(source_path)
        )
    
    column_keys = []
    for flag_record in flag_config["attribute_records"]:
        column_keys.append(flag_record["key"])
        table_record = (
            table_record
            .withColumn(flag_record["key"],
                            F.when(
                                parse_condition(flag_record["condition"]["method"],flag_record["condition"]["column"],flag_record["condition"]["value"]),
                                F.lit(True)
                            )
                       )
        )
        
    table_record = (
        table_record
        .select(
            F.col("link_type"),
            F.col("link_value"),
            F.col("id_type"),
            *[F.col(col) for col in column_keys]
        ).dropna(how="all",subset=column_keys).drop_duplicates()
    )
    
    return table_record

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrichment flagging logic

# COMMAND ----------

first_dataframe = True
for flag_config in Config.flag_config:
    tmp_fit = parse_source(flag_config)
    if first_dataframe:
        first_dataframe = False
        enrichment_fit = tmp_fit
    else:
        enrichment_fit = enrichment_fit.join(tmp_fit, on=["link_type","link_value","id_type"],how="full_outer")


#TODO apply unification HERE

enrichment_fit = (
    enrichment_fit.select(
        F.struct([
            F.col("link_type"),
            F.col("link_value"),
            F.col("id_type")
        ]).alias("unified_link"),
        *get_keys()
        ).drop_duplicates()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### QA

# COMMAND ----------

enrichment_fit.select(*[F.count(F.when(F.col(c).isNotNull(), c)).alias(c) for c in get_keys()],F.count(F.col("unified_link").alias('Total count'))).display()

# COMMAND ----------

enrichment_export.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(Config.enrichment_config['path'])

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps(
        {
            "id": Config.enrichment_config['id'],
            "bucket": Config.bucket,
            "path": Config.enrichment_config['path'],
        }
    )
)
