# Databricks notebook source
import hashlib
import json
from datetime import datetime

from pyspark.ml.feature import QuantileDiscretizer

from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError

from typing import List, Dict, Any,Optional
from data_toolkit.config import JobConfig

from pydantic import BaseModel
from pydantic import root_validator

# COMMAND ----------

# This is a helper function to generate output path for export files
def output_path(bucket, client, named_source, record_type, dataset_id, job_run_id):
    prefix = f"s3://{bucket}/jobs/{client}/{named_source}__{record_type}/dataset_id_partition={dataset_id}/job_run_id={job_run_id}"
    print(prefix)
    return prefix


# COMMAND ----------

# MAGIC %md ## Transform Overview
# MAGIC The end goal of a transaction transform script is to munipulate the provided data into three relational tables as shown on the diagram below and export them to the client's s3 bucket.  
# MAGIC Note: The diagram only shows important or mandatory fields, for a complete documentation please see https://lexerdev.github.io/customer-api/redoc#operation/load_history_events
# MAGIC 
# MAGIC 1. Transaction export
# MAGIC   - We need to split it into four subtables for the CDE to ingest them (ecom pruchase/return, instore purchase return
# MAGIC   - Links column is the link key to the customer export and entity_ref column is the link key to the product export
# MAGIC   - History key is the pirmary key
# MAGIC   - The export will be at the order level and the line items will be nesting under the array column products
# MAGIC   - For full documentation please see: https://lexerdev.github.io/customer-api/redoc#operation/load_history_events
# MAGIC   
# MAGIC 2. Product export
# MAGIC   - Is a table which stores product information
# MAGIC   - For full documentation please see: https://lexerdev.github.io/customer-api/redoc#operation/load_entities
# MAGIC   - For any non-standard product information, please put them into the catgeories column. 
# MAGIC 
# MAGIC 3. Customer export
# MAGIC   - Is a table which stores customer information
# MAGIC   - For full documentation please see: https://lexerdev.github.io/customer-api/redoc#operation/load_customers
# MAGIC   - For any non-standard customer information, please put them into the custom_fields column. 
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC <a href="https://ibb.co/3SGQKg2"><img src="https://i.ibb.co/MPWQdFr/Screen-Shot-2021-09-20-at-9-44-03-am.png" alt="Screen-Shot-2021-09-20-at-9-44-03-am" border="0"></a><br /><a target='_blank' href='https://emoticoncentral.com/category/ded-gaem'>

# COMMAND ----------

# MAGIC %md ## 0. Load Parameters

# COMMAND ----------

# MAGIC %md We use the pydantic to declare, load and check parameters. A few things to note:  
# MAGIC - When you are declaring parameters, please define types (str, int, List, Dict etc.) for them
# MAGIC - There are two kinds of parameters: 1. Mandatory Parameters 2. Optional parameters
# MAGIC - Optional parameters are more relevant to integration transform (shopify, magento etc.) because the script is going to be used by multiple clients
# MAGIC - Region, bucket, client and dataset_id are the 4 mandatory parameters. Please don't pass a default value
# MAGIC - We can also add optional parameters. For these parameters you'll need to define a default value

# COMMAND ----------

class MyJobConfig(JobConfig):
    # These are mandatory parameters. Don't pass a default value to them
    region: str 
    bucket: str
    client: str
    dataset_id:str
    #### Define your optional parameters here and assign with default value
    # Example:
    # account_id: str = "*"
    # exclude_tax: bool = False # If we want to exclude gst from price calculation

# COMMAND ----------

# Load configs from dbutils and print them
Config=MyJobConfig.from_dbutils(dbutils) 

for config in Config:
  print(config)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Ingesting raw data
# MAGIC 
# MAGIC This is the part where we ingest raw from s3 to spark dataframes

# COMMAND ----------

# MAGIC %md ### 1.1 Define Data Streams    
# MAGIC       
# MAGIC - There can be different types of files from the data feed. We call these data streams
# MAGIC - Usually, for transaction data there will be 3 streams: customer, orders and products
# MAGIC - In some cases products and orders are in the same file. We will need to seperate them later on
# MAGIC - The names of the data streams needs to be exactly the same with the files we recievd 
# MAGIC - File names can change, for example, "orders" can be "transaction" sometimes
# MAGIC - Subject to client, sometimes there will be other fact tables (store information, product collection etc.)   
# MAGIC - We also define a data stream class which you can store multiple dataframes

# COMMAND ----------

# Subject to clients, you'll need to add, delete or rename some of the streams
DATA_STREAMS = [
    'customers',
    'orders',
    'products'
]


# COMMAND ----------

# MAGIC %md ### 1.2 Define Schemas for Data Streams

# COMMAND ----------

# MAGIC %md 
# MAGIC - When ingesting raw data, some of the rows may be rejected by the spark read method because they have bad format. We call it "corrupt record"
# MAGIC - It is our responsiblity to capture, analyze and explian "corrupt record". And find out a way to ingest them if they are needed.
# MAGIC - In order to do that, we need to define data schemas, add a "corrupt_record" column and use the mode "permissive" in the reading method. 
# MAGIC - For more details of permissive reading please see https://docs.databricks.com/_static/notebooks/read-csv-corrupt-record.html
# MAGIC - You can use schema helpers from data toolkits. https://github.com/lexerdev/data-toolkit/blob/main/data_toolkit/schema_helpers.py

# COMMAND ----------

CUSTOMER_SCHEMA="{}"

# COMMAND ----------

PRODUCT_SCHEMA="{}"

# COMMAND ----------

ORDER_SCHEMA="{}"

# COMMAND ----------

DATA_STREAM_SCHEMA = {
  "customer":CUSTOMER_SCHEMA,
  "product":PRODUCT_SCHEMA,
  "order":OORDER_SCHEMA
}

# COMMAND ----------

# MAGIC %md ### 1.3 Read and check dataframes
# MAGIC - Reading dataframes with schemas
# MAGIC - Split the dataframes up into "good" and "bad"
# MAGIC - "good" dataframes contains record that can be ingested by the defined schema
# MAGIC - "bad" dataframes contains corrupt record

# COMMAND ----------

# Define static path
# Usually for integrations the source path looks like this
SOURCE_DIR = f's3://lexer-client-{CONFIG.client}/{integrations}/provider={}/account_id={CONFIG.account_id}/year=*/month=*/day=*/'

# For sftp files the source path looks like this
SOURCE_DIR = f's3://lexer-client-{CONFIG.client}/imports/*/*/*/'

# COMMAND ----------

SOURCE_DIRECTORY=f"s3://{Config.bucket}/integrations/provider=salesforce_marketing/"
sources=S3Utils.list_keys(SOURCE_DIRECTORY).persist()
sources.display()

# COMMAND ----------

def read_source(stream:str, file_format:str,schema: str) -> Dict:
    df = spark.read.format(file_format).option('mode','PERMISSIVE').load(stream, schema=T.StructType.fromJson(json.loads(schema)))

    df.cache() # Can't do any filter/select on _corrupt_record without this

    bad = df.select("_corrupt_record").where(F.col("_corrupt_record").isNotNull())
    good = df.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    return {"good":good, "bad":bad}
  
def get_file_list(s3_source:DataFrame,file_name:str) ->List:
    s3_source=S3Utils.filter_keys(s3_source,file_name)
    return s3_source.select("path").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

SOURCES_DEBUG={}

for stream in DATA_STREAMS:
    print(f"Loading source {stream}")
    SOURCES_DEBUG[stream] = read_source(get_file_list(sources,stream), 'json', DATA_STREAM_SCHEMA[stream])
    if not SOURCES_DEBUG[stream]["bad"].rdd.isEmpty():
      print(stream, 'has corrupt record')
      #print bad records
      SOURCES_DEBUG[stream]["bad"].display()
    else:
      print(stream, 'is good')
      SOURCES_DEBUG[stream]["good"].display()

# COMMAND ----------

# MAGIC %md ## 1.4 Loading all good records to a nametuple class SOURCES

# COMMAND ----------

SOURCES={}
for stream in DATA_STREAMS:
    SOURCES[stream] = SOURCES_DEBUG[stream]["good"]

SOURCES = Streams(**SOURCES)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Source to persist
# MAGIC 
# MAGIC - Where required, dataframes are persisted to a dataframe with `_persist` suffix.
# MAGIC - Persist dataframes are all deuped by a certain column or a combination of columns
# MAGIC - They can be used to draw comparison with the export dataframes to validate your transform
# MAGIC 
# MAGIC Some notes on order files:
# MAGIC 1. Order file can come in a nested format (usually json) or a flat format (usually csv)
# MAGIC 2. Nested transaction files are at the order level. One row representes one order. The line items are nested in an array "line_item" column. Order id should be unique in these files.
# MAGIC 3. Flat transaction files are at the item level so one row represents one item. Multiple rows can shore one order id in this case. A combination of order_id and itme_id (can be sku or product_id) is a better PK.

# COMMAND ----------

# Low level helper to add a input data column.
def add_input_file_date(source: DataFrame, date_pattern:str, col_name: str = "file_date") -> DataFrame:
    return (
        source.withColumn(
            col_name,
            F.regexp_extract(
                F.input_file_name(),
                date_pattern,
                1,
            ),
        )
    )

# Please note filter_cols can be a list. So you can pass multiple columns to it
# The date_pattern can be either r"(year=\d{4}\/month=\d{2}\/day=\d{2})" or r"\/imports\/(\d{4}\/\d{2}\/\d{2})" subject to import path
def filter_most_recent(source: DataFrame, filter_cols: List[Column], order_by:Column="file_date", date_pattern:str, test=False) -> DataFrame:
    if order_by=='file_date':
      source=add_input_file_date(source, date_pattern)
      
    df=(
        source
        # Add a row number to each record based on the filter cols & file date
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy(filter_cols).orderBy(F.desc(order_by))
            ),
        )
        # Keep only the 1st record that appeared in each file
        .filter(F.col("row_number") == 1)
        .drop("file_date", "row_number")
    )
    
    if test:
      print('before:',source.count())
      print('after:', df.count())      
    return df

# COMMAND ----------


orders_persist=filter_most_recent(source= SOURCES.orders, 
                                  filter_cols= "entity_id",
                                  date_pattern = r"(year=\d{4}\/month=\d{2}\/day=\d{2})" 
                                  test=True).persist()

customer_persist=filter_most_recent(source= SOURCES.customers, 
                                    filter_cols="customer.id",
                                    date_pattern = r"(year=\d{4}\/month=\d{2}\/day=\d{2})" 
                                    test=True).persist()

# Sometimes we need to rely on a native column to dedupe instead of input path
# In this case, you need to specify a order_by parameter and can leave date_pattern blank
products_persist=filter_most_recent(source= SOURCES.products,
                                    filter_cols="sku",
                                    order_by="updated_at",
                                    test=True).persist()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Persist to Fit
# MAGIC 
# MAGIC This is the section where we do most of the heavy lifting jobs in transforming data into customer-api spec

# COMMAND ----------

# MAGIC %md ### 3.1 Transaction/Orders persist to fit
# MAGIC 
# MAGIC Transaction/order is the focal point of a "transaction transform".   
# MAGIC You can checkout required and optional fields in here: https://lexerdev.github.io/customer-api/redoc#operation/load_history_events  
# MAGIC 
# MAGIC 
# MAGIC Things to look after:
# MAGIC 1. Customer link key: 
# MAGIC     - We use email as prefered link. 
# MAGIC     - If the original transaction table doesn't have email you need to join it with the customer table
# MAGIC     - Do a "before and after join" count comparison to detect duplicated rows  
# MAGIC 2. "Products" column: 
# MAGIC     - It contains three types of fields: 1. Price fields (price_paid, discount and full_price) 2. Product fields (product_id, sku, name etc.) 3. Product link (entity_ref)
# MAGIC     - The export tables needs to be at the order level so all line items needs to be nesting under this column
# MAGIC     - Only price and quantity are mandatory but you are also required to fill the discount and full price fields for purchase exports (no need for returns)
# MAGIC     - Discounts field needs to be a positive float
# MAGIC     - All price fields needs to be row total (price * quantity) 
# MAGIC     - Please check the consistency between full price, price_paid and discounts (full_price=price_paid + discount) for all rows
# MAGIC     - If the original transaction table doesn't contain product information just leave these fields empty
# MAGIC     - When the raw file is flat (at the item level), you'll need to aggregate it to order level so the "products" column becomes an array. Use F.collect_set() to do it.
# MAGIC     - When the raw file is nested (at the order level), you have tow options:
# MAGIC       1. Write a column in column out function to transfrom original line itme column into products column (see shopify). This way you don't need to explode the table
# MAGIC       2. Explode the table, construct the products column and reaggregate it back to order level (see magento).
# MAGIC       Option 1 is more advanced and considered to be a better practice. Option 2 is easier to test. 
# MAGIC 3. Product link keys:
# MAGIC     - Entity_ref is the product link. It matches the "id" column in the product table
# MAGIC     - It's s nested field under the products column
# MAGIC     - The id type can be either product_id, sku or upc. As long as they have high coverage and unique to prduct
# MAGIC     - If you can't find any valid entify_ref for rows please leave the entire column as null or else it will break CDE.   
# MAGIC       Example: 1. entity_ref:null is good. 2. entity_ref: {id_type:sku,id:123456} is good.3. entity_ref: {id_type:sku,id:null} is bad.
# MAGIC     - It is also worth to check the propotion of transactions that can be linked to the product table
# MAGIC 4. Return vs Purchase:  
# MAGIC     - You'll need find the right column for spliting purchases and returns
# MAGIC     - A rule of thumb is: If there is a "return_flag" column, use it to identify returns. If not use (qty<0) as the filter 
# MAGIC 5. Retail vs Ecommerce:
# MAGIC     - Return/Purchases needs to be future splited into ecom and retail
# MAGIC     - Please note the store columns requries different "store_type" (online or retail) for ecom and retail. 
# MAGIC 6. Primary key:
# MAGIC     - Column "history_key" is the unique identifier for the order file
# MAGIC     - Usaully it is called "order_id"

# COMMAND ----------

# MAGIC %md Useful helper functions for gathering nested columns. Some of them may requrie some changes subject to customer's files. 

# COMMAND ----------

# We prefer email as primary customer link. If email is not avaliable we fall back on customer id 
def gather_link(email: Column, customer_id: Column) -> Column:
    return (
        F.when(
            email.isNotNull(),
            F.struct(F.lit("email").alias("link_type"), F.trim(email).alias("link_value")), # It's importand to trim for customer link values to prevent duplicated profiles in the hub
        )
        .when(
            customer_id.isNotNull(),
            F.struct(F.lit("customer_id").alias("link_type"),  F.trim(customer_id).cast("string").alias("link_value")),
        )
        .otherwise(F.lit(None))
        .alias("link")
    )

# In rare oncassion, you may need multiple entity_refs. See shopify transfrom.
def gather_entity_ref(ref_col:Column,id_type:str) -> Column:
    return (F.struct(
                F.col(ref_col).alias('id'),
                F.lit(id_type).alias('id_type'),
                F.lit(CONFIG.dataset_id).alias('dataset_id')
            )
         )

# This for magento. Please tweak this function for other transform
def gather_payment_types(name:Column, total:Column, payment_types) -> Column:
    payment_type=  (
                     F.struct(
                               F.filter(F.col(name),payment_types)[0].alias("name"),
                               F.col(total).alias("total")
                             )
                    ).alias("payment_types")
    payment_type=F.when(payment_type["name"].isNotNull(),payment_type).otherwise(F.lit(None))
    return payment_type.alias("payment_types")

def gather_store(store_name:Column , store_id:Column,store_type:str)-> Column:
    return  (
              F.struct(
                         F.cole(store_name).alias("name"),
                         F.col(store_id).alias("id"),
                         F.lit(store_type).alias("type")
                             )
            ).alias("store")


# COMMAND ----------

# MAGIC %md You are also encoraged to wirte higher order dataframe in dataframe out functions

# COMMAND ----------

# Just an example:

def transaction_persist_to_fit(transaction_persist:DataFrame, is_return:bool , channel:str ) -> DataFrame:
    
    RETURN_FILTER= ((F.col('item.qty_ordered') >= 1))
    # Chnage the filter for returns and purchases
    if is_return:
      TRANSACTION_FILTER = RETURN_FILTER
    else:
      TRANSACTION_FILTER = (~RETURN_FILTER)
    
    CHANNEL_FILTER = (F.col("channel") == channel)
       
    transaction_fit=(
        transaction_persist
        .where(TRANSACTION_FILTER & CHANNEL_FILTER) 
        .select(
            F.col('created_at').alias('action_at'),
            F.col("entity_id").alias('history_key'),
            F.struct(
                F.col('item.qty_ordered').alias('quantity'),
                F.col('item.row_total').alias('price_paid'),
                (-F.col('discount_amount')).alias('discount'),
                (F.col('item.row_total')- F.col('discount_amount')).alias('full_price'),
                F.col('item.sku').alias('sku'),
                F.trim('item.name').alias('name'),
                F.col('item.product_id').alias('product_id'),
                gather_entity_ref(F.col('item.sku'),'sku').alias('entity_ref')
        ).alias("products"),
          gather_link(F.col("customer_email"),F.col("customer_id")),
          gather_payment_types("payment.additional_information","payment.amount_paid",valid_payment_type),
          gather_store("store_name","store_id",channel)
        )
    )
    
return transaction_fit

# COMMAND ----------

# MAGIC %md Some clients are only on ecommerce or retail. It is okay to only have one of retail or ecommerce

# COMMAND ----------

ecommerce_purchase_fit=transaction_persist_to_fit(order_persist,is_return=False,channel='online')
ecommerce_return_fit=transaction_persist_to_fit(order_persist,is_return=True,channel='online')
instore_purchase_fit=transaction_persist_to_fit(order_persist,is_return=False,channel='retail')
instore_return_fit=transaction_persist_to_fit(order_persist,is_return=True,channel='retail')

# COMMAND ----------

# MAGIC %md ### 3.2 Product persist to fit
# MAGIC 
# MAGIC You can checkout required and optional fields in here: https://lexerdev.github.io/customer-api/redoc#operation/load_entities
# MAGIC 
# MAGIC Things to look after:
# MAGIC - The "product" column is only for standard product fileds
# MAGIC - The "categories" column is for non-standard columns. The format needs to be: [{"name":{category_name1},"value":{category_value1}}, {"name":{category_name2},"value":{category_value2}}]
# MAGIC - {"name":{category_name2},"value":null} is not acceptable for CDE. {null} is acceptable. 

# COMMAND ----------

# This a helper to gather a single category to a column. If your transform have multiple category you'll need to ammend this function (see shopify) or later union them into an array (but watchout the null values)
def prepare_categories(category_name: str,category_value) -> Column:
    return (
        F.when(
            category_value.isNotNull(),
            F.array(
                F.struct(F.lit(category_name).alias("name"), category_value.alias("value"))
            ))
          .when(
            category_value.isNull(),
            F.lit(None)
            )
    )

# COMMAND ----------

# This is just an example from magento. You could also rewrite it into a function

product_fit=product_persist.select(
    F.struct(
        F.col("sku").alias("id"),
        F.lit("sku").alias("id_type")
    ).alias('id'),
    F.struct(
        F.col("id").alias("product_id"),
        F.col("sku"),
        F.col('upc'),
        F.col("name"),
        F.col("description"),
        prepare_categories(category_name="Category",category_value=F.col("attribute_set_name")).alias('categories'),
        F.col("image")
    ).alias("product"))

# COMMAND ----------

# MAGIC %md ### 3.3 Customer persist to fit
# MAGIC 
# MAGIC You can access the full documentation in here: https://lexerdev.github.io/customer-api/redoc#operation/load_customers

# COMMAND ----------

# Example from shopify. You can also write it as function
customer_fit = customer_persist.select(
    F.split("tags", ", ").alias("tags"),
    F.struct(
        F.col("currency"),
        F.col("note"),
        F.col("orders_count"),
        F.col("total_spent"),
        F.col("verified_email"),
    ).alias("custom_fields"),
    gather_link(F.col("email"), F.col("id")),
    F.struct(
        F.col("email"),
        F.col("phone").alias("mobile"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("id").alias("customer_id"),
        F.col("accepts_marketing").alias("communication_opt_in"),
        F.col("default_address.country").alias("country"),
        F.col("default_address.province").alias("state"),
        F.col("default_address.city").alias("city"),
        F.col("default_address.zip").alias("zip" if Config.region == "us" else "postcode"),
        F.col("default_address.address1").alias("address_1"),
        F.col("default_address.address2").alias("address_2"),
    ).alias("customer"),
)

# COMMAND ----------

# MAGIC %md ## 4. Fit to Export
# MAGIC This the final stage of the transform. In here you can add the metadata, apply cuurency conversion rate or do a final dedupe.

# COMMAND ----------

# Helper to bring in the metadata
def apply_metadata(dataframe:DataFrame,dataset_id:str,record_type:str,version:str='1.0') -> DataFrame:
  return (
           dataframe.withColumn('dataset_id',F.lit(dataset_id))
                    .withColumn('version',F.lit(version))
                    .withColumn('type',F.lit(record_type))
  )

# COMMAND ----------

# Customer
customer_export=apply_metadata(customer_fit,dataset_id=CONFIG.dataset_id,record_type='customer_record')
# Transaction
ecommerce_purchase_export=apply_metadata(ecommerce_purchase_export,dataset_id=CONFIG.dataset_id,record_type='ecommerce_purchase')
ecommerce_return_export=apply_metadata(ecommerce_return_export,dataset_id=CONFIG.dataset_id,record_type='ecommerce_return')
instore_purchase_export=apply_metadata(instore_purchase_export,dataset_id=CONFIG.dataset_id,record_type='instore_purchase')
instore_return_export=apply_metadata(instore_return_export,dataset_id=CONFIG.dataset_id,record_type='instore_return')
# Product
product_export=apply_metadata(product_export,dataset_id=CONFIG.dataset_id,record_type='product')

# COMMAND ----------

# MAGIC %md ## 5. Preparation for load_dataset.py
# MAGIC 
# MAGIC In production, we use the load_dataset.py to run the glue job and push the data to the CDE.   
# MAGIC In order to do that we need to do a few things first.   
# MAGIC Normally you don't need to change anything in here apart from fill in the SERVICE_NAME. But it is good for you to know what's happening in here. 

# COMMAND ----------

# MAGIC %md ### 5.1 Define export path and job prefixes

# COMMAND ----------

# The convention is to use the name of the provider if it's coming from an integration. If not just use "transaction"
SERVICE_NAME={your_service_name}

customer_export_path = output_path(CONFIG.bucket, CONFIG.client, SERVICE_NAME, "customer_record", CONFIG.dataset_id, JOB_RUN_ID)
product_export_path = output_path(CONFIG.bucket, CONFIG.client, SERVICE_NAME, "product", CONFIG.dataset_id, JOB_RUN_ID)
ecommerce_purchase_export_path = output_path(
    CONFIG.bucket, CONFIG.client, SERVICE_NAME, "ecommerce_purchase", CONFIG.dataset_id, JOB_RUN_ID
)
ecommerce_return_export_path = output_path(
    CONFIG.bucket, CONFIG.client, SERVICE_NAME, "ecommerce_return", CONFIG.dataset_id, JOB_RUN_ID
)
instore_purchase_export_path = output_path(
    CONFIG.bucket, CONFIG.client, SERVICE_NAME, "instore_purchase", CONFIG.dataset_id, JOB_RUN_ID
)
instore_return_export_path = output_path(
    CONFIG.bucket, CONFIG.client, SERVICE_NAME, "instore_return", CONFIG.dataset_id, JOB_RUN_ID
)

# COMMAND ----------

# Define a disctionary. This is required by the glue job to run.
job_prefixes = [
    {
        "prefix": customer_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "customer_record",
    },
    {
        "prefix": product_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "product",
    },
    {
        "prefix": ecommerce_purchase_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "ecommerce_purchase",
    },
    {
        "prefix": ecommerce_return_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "ecommerce_return",
    },
    {
        "prefix": instore_purchase_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "instore_purchase",
    },
    {
        "prefix": instore_return_export_path.replace(f"s3://{Config.bucket}/", ""),
        "record_type": "instore_return",
    },
]

# COMMAND ----------

# MAGIC %md ### 5.2 Export dataframes to s3

# COMMAND ----------

customer_export.write.json(customer_export_path, mode="overwrite", compression="gzip")
product_export.write.json(product_export_path, mode="overwrite", compression="gzip")
ecommerce_purchase_export.write.json(ecommerce_purchase_export_path, mode="overwrite", compression="gzip")
ecommerce_return_export.write.json(ecommerce_return_export_path, mode="overwrite", compression="gzip")

instore_purchase_export.write.json(instore_purchase_export_path, mode="overwrite", compression="gzip")
instore_return_export.write.json(instore_return_export_path, mode="overwrite", compression="gzip")

# COMMAND ----------

# MAGIC %md ### 5.3 Notebook exit 
# MAGIC Passing the job_prefixes to the load_dataset.py. Don't forget to include this one-liner as it is essential in production

# COMMAND ----------

dbutils.notebook.exit(json.dumps(job_prefixes))

# COMMAND ----------

# MAGIC %md ## 6. Mannually push the data into CDE (EDA only. Don't include this in production.)
# MAGIC This is where we mannually push the data to CDE.   Because in product we have load_dataset.py so you don't need to include this in your PR.  
# MAGIC But it is very useful to run it in EDA as it will generate report for you to check if there is any data being rejected by the API.

# COMMAND ----------

RECORD_TABLES = {
    "customer_record": "customer_record",
    "engage_customer_record": "customer_record",
    "product": "entity_record",
    "ecommerce_purchase": "identity_history",
    "ecommerce_return": "identity_history",
    "instore_purchase": "identity_history",
    "instore_return": "identity_history",
    "clienteling_create_customer": "identity_history",
    "clienteling_amend_customer": "identity_history",
    "clienteling_check_in_customer": "identity_history",
    "email_send": "identity_history",
    "email_open": "identity_history",
    "email_click": "identity_history",
    "email_subscribe": "identity_history",
    "email_bounce": "identity_history",
    "engage_agent_message": "identity_history",
    "external_support_agent_message": "identity_history",
    "external_support_customer_message": "identity_history",
}

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": dbutils.secrets.get(scope="customer-api", key="x-api-key"),
}


def load_glue(bucket, action, table, dataset_id, prefixes, service, n_workers):
    "Use the Identity History API to bulk load records from S3."
    payload = {
        "bucket": bucket,
        "action": action,
        "table": table,
        "dataset_id": dataset_id,
        "prefixes": prefixes,
        "service": service,
        "n_workers": n_workers,
    }
    response = requests.post(
        f"https://{REGION}-identity-history.camplexer.com/identity_history/jobs/s3_load_glue",
        headers=HEADERS,
        json=payload,
    )
    assert response.ok, response.text
    return response.json()

  
def arrange_jobs(job_prefixes):
    "Arranges job_prefixes acording to the table they belong."
    table_jobs = {}

    for prefix in job_prefixes:
        table = RECORD_TABLES[prefix["record_type"]]
        if table in table_jobs.keys():
            table_jobs[table] += [prefix]
        else:
            table_jobs[table] = [prefix]

    return table_jobs

N_WORKERS=20

# COMMAND ----------

import requests


job_responses = []

REGION=CONFIG.region

for table, prefixes in arrange_jobs(job_prefixes).items():
    job_responses.append(
        load_glue(
            bucket=BUCKET,
            action="UPSERT",
            dataset_id=DATASET_ID,
            table=table,
            prefixes=prefixes,
            service=SERVICE_NAME,
            n_workers=N_WORKERS,
        )
    )
    
def get_glue_status(job_id):
    "Get the status for a given job_id"
    response = requests.get(
        f"https://{REGION}-identity-history.camplexer.com/identity_history/jobs/s3_load_glue/{job_id}", headers=HEADERS
    )
    return response.json()
  
import pandas as pd
pd.options.display.max_colwidth = 0

# COMMAND ----------

# MAGIC %md A pandas dataframe will pop up when all of the jobs are finished. Please pay attention to the "stats" field as it tells you how many records were rejected.

# COMMAND ----------

from time import sleep
status = pd.DataFrame([get_glue_status(job["id"]) for job in job_responses])

while any(status['status']!='FINISHED'):
    status = pd.DataFrame([get_glue_status(job["id"]) for job in job_responses])
    print(status['status'])
    sleep(100)
print("Done!")
status.T
