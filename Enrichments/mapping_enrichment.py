# Databricks notebook source
# Databricks Librarys
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Column
from pyspark.sql.window import Window

# Python Librarys
import json
from typing import List, Dict, Any,Optional

from data_toolkit.util import spark, dbutils
from data_toolkit.config import JobConfig

# COMMAND ----------

class MyJobConfig(JobConfig):
    region: str 
    bucket: str
    client: str
    dataset_ids: List[str]
    mapping_config: Dict
    attributes: List[Dict]
    enrichment_config: Dict

# COMMAND ----------

Config=MyJobConfig.from_dbutils(dbutils) 

for config in Config:
    print(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapping Enrichment
# MAGIC This enrichment can be used to join a mapping table to a source table in order to create new attributes. It takes in a mapping file, a source table, a column pair to map on and a list of attribute keys to create from the new mapped table.
# MAGIC 1. mapping file
# MAGIC   * mapping_file {
# MAGIC         "file_source": - path to file/table
# MAGIC         "file_type": - defined file type e.g file, delta
# MAGIC         "format": - format of file e.g csv, json
# MAGIC   }
# MAGIC 2. Source table
# MAGIC   * currently only customer_record or identitiy_history
# MAGIC 3. columns to map on
# MAGIC     "mapping_column": "first_name",
# MAGIC     "source_col": "record.customer_record.customer.first_name",
# MAGIC 4. attribute keys for column selection
# MAGIC   * {"column_available": "renamed column",
# MAGIC      "column_available": "renamed column"}

# COMMAND ----------

print(json.dumps(Config.mapping_config, indent=4))

# COMMAND ----------

table_types = ["customer_record","identity_history"]

if Config.mapping_config["source_table"] in table_types:
    path_to_table = f's3://{Config.bucket}/customerapi/{Config.region}-identity-history/lake/datasets/delta/{Config.mapping_config["source_table"]}/'
    source_table = (
            spark.read.format("delta")
            .load(path_to_table)
        )

# read in mapping file
mapping_table = (
        spark.read.format(Config.mapping_config["mapping_file"]["format"])
        .load(Config.mapping_config["mapping_file"]["file_source"],header=True)
    )

# COMMAND ----------

source_table = source_table.withColumn("place_holder", F.trim(F.upper(F.col(Config.mapping_config["source_col"]))))
mapping_table = mapping_table.withColumn(Config.mapping_config["mapping_column"], F.trim(F.upper(F.col(Config.mapping_config["mapping_column"]))))

mapped_fit = (
    source_table
    .join(
        mapping_table,
        source_table.place_holder==mapping_table[Config.mapping_config["mapping_column"]],
        how="inner")
)

#     .groupby(F.struct("link_type", "link_value").alias("unified_link")).agg(F.first("inferred_gender").alias("inferred_gender"))

# COMMAND ----------

mapped_fit.display()

# COMMAND ----------

enrichment_export = (
    mapped_fit.select(
        F.struct([
            F.col("link_type"),
            F.col("link_value"),
            F.col("id_type")
        ]).alias("unified_link"),
        *[F.col(key).alias(value) for key, value in Config.mapping_config["attribute_keys"].items()])
)
enrichment_export.display()

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
