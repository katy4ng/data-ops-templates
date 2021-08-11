# Databricks notebook source
# MAGIC %md qa constants

# COMMAND ----------

CLIENT = "thememo"
KLAYVIO_ACCOUNT = "11481"
BUCKET = f"lexer-client-{CLIENT}"
# s3 Paths
# v1 path e.g s3://{BUCKET}/{path}/lexer_klaviyo_events.json
v1_path = "test"
# v2 path
v2_day = "01"
v2_month = "07"
v2_year = "2021"

# COMMAND ----------

# MAGIC %md
# MAGIC # V1 Read files in

# COMMAND ----------

# Databricks notebook source
import hashlib
import json
from datetime import datetime

from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError


def output_path(bucket, client, named_source, record_type, dataset_id, job_run_id):
    prefix = f"s3://{bucket}/jobs/{client}/{named_source}__{record_type}/dataset_id_partition={dataset_id}/job_run_id={job_run_id}"
    print(prefix)
    return prefix


# COMMAND ----------

def load_parameter(parameter_name):
    return json.loads(dbutils.widgets.get(parameter_name))



# JOB PARAMETERES
FROM_EMAIL = ""
DEFAULT_EMAIL_ID = ""
# list of possible values for subscribe events
try:
    SUBSCRIBE_VALUES = load_parameter("subscribe_values")
except Py4JJavaError:
    SUBSCRIBE_VALUES = ['Subscribed to List']

# JOB CONSTANTS
JOB_RUN_ID = "db_" + hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()


# These files are from a seperate 30 day pull
events_source_v1 = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_events_*.json"
)

exclusions_source_v1 = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_exclusions_*.json"
)
list_subs_source_v1 = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_list-subscriptions_*.json"
)
lists_source_v1 = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_lists_*.json"
)
people_source_v1 = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_people_*.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # V2 Read Files in

# COMMAND ----------

# Databricks notebook source
import hashlib
import json
import re
from datetime import datetime

from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row


pull_out_date = F.udf(
    lambda file_path_string: "/".join(
        re.search("year=(\d{4})\/month=(\d{2})\/day=(\d{2})", file_path_string).group(
            1, 2, 3
        )
    ),
    T.StringType(),
)

# JOB PARAMETERES
REGION = "us"
DATASET_ID = "1234"
FROM_EMAIL = ""
DEFAULT_EMAIL_ID = "tatsoul"
VERSION = "1.0"

PROVIDER = "klaviyo"
# JOB PARAMETERES

def output_path(
    record_type,
    named_source="klaviyo",
    dataset_id=DATASET_ID,
    job_run_id=JOB_RUN_ID,
    bucket=BUCKET,
    client=CLIENT,
):
    prefix = f"s3://{bucket}/jobs/{client}/{named_source}__{record_type}/dataset_id_partition={dataset_id}/job_run_id={job_run_id}"
    print(prefix)
    return prefix


def get_latest_rows(table, partition_by: str):
    "Selects the most recent records from data source from the file name, splitting on partition_by field"
    return (
        table.withColumn(
            "file_date",
            pull_out_date(F.input_file_name()),
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy(partition_by).orderBy(F.desc("file_date"))
            ),
        )
        .filter(F.col("row_number") == 1)
        .drop("file_date", "row_number")
    )

def hash_unix_timestamp():
    return "db_" + hashlib.sha256(str(datetime.utcnow().timestamp()).encode()).hexdigest()

JOB_RUN_ID = hash_unix_timestamp

people_base_scheama = """{
    "fields": [
        {
            "metadata": {},
            "name": "$address1",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$address2",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$city",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$country",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$first_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$last_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$organization",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$phone_number",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$region",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$timezone",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$title",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$zip",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "object",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "updated",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
people_json_schema = json.loads(people_base_scheama)
people_base_scheama = T.StructType.fromJson(people_json_schema)
PEOPLE_BASE_FIELDS = {col["name"] for col in people_json_schema["fields"]}

events_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "datetime",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "event_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "event_properties",
            "nullable": true,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "List",
                        "nullable": true,
                        "type": "string"
                    },
                    {
                        "metadata": {},
                        "name": "Campaign Name",
                        "nullable": true,
                        "type": "string"
                    },
                    {
                        "metadata": {},
                        "name": "Subject",
                        "nullable": true,
                        "type": "string"
                    }
                ],
                "type": "struct"
            }
        },
        {
            "metadata": {},
            "name": "person",
            "nullable": true,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "id",
                        "nullable": true,
                        "type": "string"
                    }
                ],
                "type": "struct"
            }
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "object",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "statistic_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "timestamp",
            "nullable": true,
            "type": "long"
        },
        {
            "metadata": {},
            "name": "uuid",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""

events_schema = T.StructType.fromJson(json.loads(events_schema))

list_exclusions_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "reason",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
list_exclusions_schema = T.StructType.fromJson(json.loads(list_exclusions_schema))

global_exclusions_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "timestamp",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "reason",
            "nullable": true,
            "type": "string"
        }

    ],
    "type": "struct"
}"""
global_exclusions_schema = T.StructType.fromJson(json.loads(global_exclusions_schema))

list_subs_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "phone_number",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
list_subs_schema = T.StructType.fromJson(json.loads(list_subs_schema))

lists_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "folder_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "updated",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
lists_schema = T.StructType.fromJson(json.loads(lists_schema))

lists_mapping_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "list_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
lists_mapping_schema = T.StructType.fromJson(json.loads(lists_mapping_schema))

segments_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "segment_id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
segments_schema = T.StructType.fromJson(json.loads(segments_schema))


# Schema for the above fields that we need for the customer record

# TODO: Investigate
spark.conf.set("spark.sql.caseSensitive", "true")
events_source_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/timeline_*.ndjson.gz",
    events_schema,
)
people_source_inferred_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/people_2*.ndjson.gz"
)

global_exclusions_source_v2 = spark.read.json(
  f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/people_excluded_*.ndjson.gz", global_exclusions_schema)

list_subs_source_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/group_all*.ndjson.gz",
    list_subs_schema,
)

list_subs_source_v2 = list_subs_source_v2.withColumn("list_id", F.split(F.input_file_name(), '_')[3])

lists_source_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/lists_mapping*.ndjson.gz",
    lists_mapping_schema,
)

people_source_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/people_2*.ndjson.gz",
    people_base_scheama,
)


# These files won't always exist
try:
    list_exclusions_source_v2 = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/list_exclusion_*.ndjson.gz",
    list_exclusions_schema,
    )
except AnalysisException:
    list_exclusions_source_v2 = sqlContext.createDataFrame(sc.emptyRDD(), list_exclusions_schema)
try:
    segment_id_to_email_v2 = spark.read.json(
        f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id={KLAYVIO_ACCOUNT}/year={v2_year}/month={v2_month}/day={v2_day}/segments_*.ndjson.gz",
        segments_schema,
    )
except AnalysisException:
    # Make an empty table
    fields = [
        T.StructField("email", T.StringType(), True),
        T.StructField("segment_id", T.StringType(), True),
    ]
    schema = T.StructType(fields)
    segment_id_to_email = sqlContext.createDataFrame(sc.emptyRDD(), schema)

# COMMAND ----------

# Find time window that intersects with both sources since they run at different times
events_v1_min = events_source_v1.select(F.min("timestamp")).collect()[0][0]
events_v1_max = events_source_v1.select(F.max("timestamp")).collect()[0][0]

events_v2_min = events_source_v2.select(F.min("timestamp")).collect()[0][0]
events_v2_max = events_source_v2.select(F.max("timestamp")).collect()[0][0]

if events_v1_min > events_v2_min:
    events_min_timestamp = events_v1_min
else:
    events_min_timestamp = events_v2_min

if events_v1_max < events_v2_max:
    events_max_timestamp = events_v1_max
else:
    events_max_timestamp =events_v2_max

# events_min_timestamp = events_source_v1.select(F.min("timestamp")).collect()[0][0]
# events_max_timestamp = events_source_v2.select(F.max("timestamp")).collect()[0][0]
events_source_v1 = events_source_v1.filter(F.col("timestamp") >= events_min_timestamp).filter(F.col("timestamp") <= events_max_timestamp)
events_source_v2 = events_source_v2.filter(F.col("timestamp") >= events_min_timestamp).filter(F.col("timestamp") <= events_max_timestamp)

# Exclusions are also effected by pull time but we pull them in full everytime so we only need to worry about the max date

v2_exclusions_max_timestamp = list_exclusions_source_v2.union(global_exclusions_source_v2).select(F.max(F.to_timestamp("created"))).collect()[0][0]
v1_exclusions_max_timestamp = exclusions_source_v1.select(F.max(F.to_timestamp("create_date"))).collect()[0][0]

if v2_exclusions_max_timestamp < v1_exclusions_max_timestamp:
    exclusions_max_timestamp = v2_exclusions_max_timestamp
else:
    exclusions_max_timestamp = v1_exclusions_max_timestamp

exclusions_source_v1 = exclusions_source_v1.filter(F.to_timestamp("create_date") <= exclusions_max_timestamp)
list_exclusions_source_v2 = list_exclusions_source_v2.filter(F.to_timestamp("created") <= exclusions_max_timestamp)
global_exclusions_source_v2 = global_exclusions_source_v2.filter(F.to_timestamp("timestamp") <= exclusions_max_timestamp)


events_source_v1_count = events_source_v1.count()
exclusions_source_v1_count = exclusions_source_v1.count()
list_subs_source_v1_count = list_subs_source_v1.count()
lists_source_v1_count = lists_source_v1.count()
people_source_v1_count = people_source_v1.count()



events_source_v2_count = events_source_v2.count()
exclusions_source_v2_count = list_exclusions_source_v2.count()
list_subs_source_v2_count = list_subs_source_v2.count()
lists_source_v2_count = lists_source_v2.count()
people_source_v2_count = people_source_v2.count()
global_exclusions_source_v2_count = global_exclusions_source_v2.count()




# COMMAND ----------

# MAGIC %md
# MAGIC Compare Raw Source File Counts

# COMMAND ----------

source_compare = sqlContext.createDataFrame(sc.parallelize([
  Row(file="events", count=events_source_v1_count, name="events-v1"),
  Row(file="events", count=events_source_v2_count, name="events-v2"),
  Row(file="exclusions", count=exclusions_source_v1_count, name="exclusions-v1"),
  Row(file="exclusions", count=exclusions_source_v2_count, name="exclusions-v2"),
  Row(file="list_subs", count=list_subs_source_v1_count, name="list_subs-v1"),
  Row(file="list_subs", count=list_subs_source_v2_count, name="list_subs-v2"),
  Row(file="lists", count=lists_source_v1_count, name="lists-v1"),
  Row(file="lists", count=lists_source_v2_count, name="lists-v2"),
  Row(file="people", count=people_source_v1_count, name="people-v1"),
  Row(file="people", count=people_source_v2_count, name="people-v2"),
  ]))
display(source_compare)

# COMMAND ----------

# MAGIC %md
# MAGIC exclusions v2 is the sum of list and global exclusions, v1 has it all in 1 file

# COMMAND ----------

assert (global_exclusions_source_v2_count + exclusions_source_v2_count) == exclusions_source_v1_count

# COMMAND ----------

# MAGIC %md 
# MAGIC V1 Transforms

# COMMAND ----------

# JOB CONSTANTS
JOB_RUN_ID = "db_" + hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()

# COMMAND ----------

events_source = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_events_*.json"
)

exclusions_source = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_exclusions_*.json"
)
list_subs_source = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_list-subscriptions_*.json"
)
lists_source = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_lists_*.json"
)
people_source = spark.read.json(
    f"s3://{BUCKET}/{v1_path}/lexer-klaviyo_{KLAYVIO_ACCOUNT}_people_*.json"
)
# COMMAND ----------
# Filter events source to line up with integration pulls having different times
events_source = events_source.filter(F.col("timestamp") >= events_min_timestamp).filter(F.col("timestamp") <= events_max_timestamp)
exclusions_source = exclusions_source.filter(F.to_timestamp("create_date") <= exclusions_max_timestamp)
# Create a complete, deduplicated view of data gathered from S3

# The events table won't change so dropping duplicates is fine
events_persist = events_source.drop_duplicates()

# Keep only the latest provided row for each list id
lists_persist = (
    lists_source.withColumn(
        "file_date",
        F.regexp_extract(F.input_file_name(), "\/imports\/(\d{4}\/\d{2}\/\d{2})", 1),
    )
    .withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("id").orderBy(F.desc("file_date"))),
    )
    .filter(F.col("row_number") == 1)
    .drop("file_date", "row_number")
)

# Keep only the latest provided row for each people id
people_persist = (
    people_source.withColumn(
        "file_date",
        F.regexp_extract(F.input_file_name(), "\/imports\/(\d{4}\/\d{2}\/\d{2})", 1),
    )
    .withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("id").orderBy(F.desc("file_date"))),
    )
    .filter(F.col("row_number") == 1)
    .drop("file_date", "row_number")
)

# COMMAND ----------

# Gather deliverable and undeliverable in lists
list_subs_fit =(
  list_subs_source.withColumn(
    "file_date",
    F.regexp_extract(F.input_file_name(), "\/imports\/(\d{4}\/\d{2}\/\d{2})", 1),
  ) .withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("person_id","list_id").orderBy(F.desc("file_date"))),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)


list_subs_fit = list_subs_fit.withColumn("status", F.lit("active_in_lists"))
list_subs_fit = list_subs_fit.join(
    people_persist.select("id","email_address"), on=(list_subs_fit.person_id == people_persist.id), how="left"
)

exclusions_fit =(
  exclusions_source.withColumn(
    "file_date",
    F.regexp_extract(F.input_file_name(), "\/imports\/(\d{4}\/\d{2}\/\d{2})", 1)
  ) 
  .withColumn(
        "row_number",
        F.row_number().over(Window.partitionBy("email_address").orderBy(F.desc("file_date"))),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

exclusions_fit = exclusions_fit.withColumn("status", F.lit("undeliverable_in_lists"))

# Exclude exlusion people from list subscription if and list id and email address matches.
list_subs_fit=list_subs_fit.withColumn("sub_file_date",F.to_timestamp("file_date","yyyy/MM/dd")).drop("file_date")

list_subs_fit=list_subs_fit.join(exclusions_fit.select('email_address',
                                         F.to_timestamp('file_date',"yyyy/MM/dd").alias('unsub_file_date'),
                                                      "list_id")
                   ,on=['email_address','list_id'],how='left')

list_subs_excluded=list_subs_fit.where(F.col("unsub_file_date").isNull())

# Edge cases. Unsubscribed and later subscribed again.
second_subscribe=list_subs_fit.where(F.col('sub_file_date')>F.col('unsub_file_date'))

#Bring them altogether
list_subs_clean=list_subs_excluded.union(second_subscribe)

list_status = list_subs_clean.select("email_address", "list_id", "status")
list_status = list_status.union(
    exclusions_fit.select("email_address", "list_id", "status")
)

#Bring in list name
list_status = list_status.join(
    lists_persist, on=(list_status.list_id == lists_persist.id), how="left"
)

list_status=list_status.withColumn('name',
                                   F.when((F.col('status')=='undeliverable_in_lists') & (F.col('name').isNull())
                                        ,F.lit('Undeliverable in unknown list')).otherwise(F.col('name'))
                                  )

list_status = list_status.groupby("email_address", "status").agg(
    F.collect_list("name").alias("list_names")
)


list_status = list_status.where(F.size("list_names") > 0)

list_status = (
    list_status.groupBy(["email_address"]).pivot("status").agg(F.first("list_names"))
)

# Lit string (none) if null. 
#If an identity didn't susbscribe to any list it'll have (none) in active in list and vice versa
list_status=list_status.withColumn(
  'active_in_lists',F.when(F.col('active_in_lists').isNull(),F.array(F.lit('Undeliverable')))
                   .otherwise(F.col('active_in_lists')
                             )
)

list_status=list_status.withColumn(
  'undeliverable_in_lists',F.when(F.col('undeliverable_in_lists').isNull(),F.array(F.lit('Deliverable')))
                           .otherwise(F.col('undeliverable_in_lists')
                                                                                                      )
)


# Copy the source customer dataframe `customer_source` for transformation as `customer_fit`
customer_fit = people_persist

# Set the DATASET_ID and record type
customer_fit = customer_fit.withColumn("dataset_id", F.lit(DATASET_ID))
customer_fit = customer_fit.withColumn("version", F.lit("1.0"))
customer_fit = customer_fit.withColumn("type", F.lit("customer_record"))


# We may add these later during schema validataion
# customer_fit = customer_fit.withColumn('metadata', lit("{}"))
# customer_fit = customer_fit.withColumn('custom_fields', lit("{}"))

# Set the record link as email
customer_fit = customer_fit.withColumn(
    "link",
    F.struct(
        F.lit("email").alias("link_type"),
        F.col("email_address").cast("string").alias("link_value"),
    ),
)

# Layout the customer struct from the fields Shopify make available
customer_fit = customer_fit.withColumn(
    "customer",
    F.struct(
        F.col("email_address").alias("email"),
        F.col("id").alias("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("address1").alias("address_1"),
        F.col("address2").alias("address_2"),
        F.col("city").alias("city"),
        F.col("region").alias("state"),
        F.col("zip"),
        F.col("country").alias("country"),
    ),
)

# Add "active_in_lists", "undeliverable_in_lists" from list_status to dataframe
customer_fit = customer_fit.join(list_status, "email_address", how="left")

# Match Inferred Gender
inf_gndr = spark.read.csv(
    f"s3a://{BUCKET}/datasets/reference_data/name_gender_lookup_table.csv",
    header=True,
)
customer_fit = customer_fit.withColumn("first_name", F.trim(F.upper(F.col("first_name"))))
customer_fit = customer_fit.join(inf_gndr, on="first_name", how="left")

# If no one is excluded from lists, "undeliverable_in_lists" is never created
if "undeliverable_in_lists" not in customer_fit.columns:
    customer_fit = customer_fit.withColumn("undeliverable_in_lists", F.lit(None))

# Set Custom Fields
customer_fit = customer_fit.withColumn(
    "custom_fields",
    F.struct(
        "inferred_gender",
        "active_in_lists",
        "undeliverable_in_lists",
    ),
)

# Subset the columns to those required in target schema
# customer_fit = customer_fit.select('dataset_id', 'type', 'tags', 'metadata', 'custom_fields', 'link', 'customer')
customer_export_v1 = customer_fit.select(
    "dataset_id", "version", "type", "link", "customer", "custom_fields"
)

# Test the transformation against the target schema
# customer_export = spark.createDataFrame(customer_export.rdd, customer_record_schema)

# COMMAND ----------

events_fit = events_source

# Set the DATASET_ID and record type
events_fit = events_fit.withColumn("dataset_id", F.lit(DATASET_ID))
events_fit = events_fit.withColumn("version", F.lit("1.0"))

# Set the record link as email
events_fit = events_fit.join(
    people_persist.select("id", "email_address"),
    events_fit.person_id == people_persist.id,
    how="left",
)

events_fit = events_fit.withColumn(
    "link",
    F.struct(
        F.lit("email").alias("link_type"),
        F.col("email_address").cast("string").alias("link_value"),
    ),
)

EVENT_MAP = {
    "Subscribed to List": "email_subscribe",
    "Unsubscribed from List": "email_subscribe",
    "Bounced Email": "email_bounce",
    "Received Email": "email_send",
    "Opened Email": "email_open",
    "Clicked Email": "email_click",
    "Unsubscribed": "email_subscribe",
}

# append custom subscribe values as email_subscribe events
for value in SUBSCRIBE_VALUES:
    EVENT_MAP[value] = "email_subscribe"

EMAIL_EVENTS = [
    "email_bounce",
    "email_click",
    "email_open",
    "email_send",
    "email_subscribe",
]

events_fit = events_fit.withColumn("type", F.col("name"))
events_fit = events_fit.replace(to_replace=EVENT_MAP, subset=["type"])
events_fit = events_fit.where(F.col("type").isin(EMAIL_EVENTS))


events_fit = events_fit.withColumn("action_at", F.to_timestamp(F.col("timestamp")))

events_fit = events_fit.withColumn(
    "history_key", F.concat("timestamp", "type", "person_id")
)
events_fit = events_fit.drop_duplicates(subset=["history_key"])

events_fit = events_fit.withColumn("to", F.struct(F.col("email_address").alias("email")))
events_fit = events_fit.withColumn("from", F.struct(F.lit(FROM_EMAIL).alias("email")))
events_fit = events_fit.withColumn("email_id", F.col("campaign_name"))
events_fit = events_fit.fillna(DEFAULT_EMAIL_ID, subset=["email_id"])
events_fit = events_fit.withColumn(
    "campaign", F.struct(F.col("campaign_name").alias("name"))
)

events_fit = events_fit.withColumn(
    "status",
    F.when(F.col("name").isin(SUBSCRIBE_VALUES), F.lit("subscribed"))
    .when(F.col("name").isin("Unsubscribed from List", "Unsubscribed"), "unsubscribed")
    .otherwise(None),
)


BASE_EMAIL_EXPORT_FIELDS = [
    "action_at",
    "dataset_id",
    "version",
    "link",
    "history_key",
    "type",
    "campaign",
    "from",
    "to",
    "subject",
    "email_id",
]

email_bounce_export_v1 = events_fit.where(events_fit.type == "email_bounce").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_click_export_v1 = events_fit.where(events_fit.type == "email_click").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_open_export_v1 = events_fit.where(events_fit.type == "email_open").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_send_export_v1 = events_fit.where(events_fit.type == "email_send").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_subscribe_export_v1 = events_fit.where(
    (events_fit.type == "email_subscribe") & (events_fit.status == "subscribed")
).select(BASE_EMAIL_EXPORT_FIELDS + ["status"])
email_unsubscribe_export_v1 = events_fit.where(
    (events_fit.type == "email_subscribe") & (events_fit.status == "unsubscribed")
).select(BASE_EMAIL_EXPORT_FIELDS + ["status"])

# COMMAND ----------

# MAGIC %md
# MAGIC V2 Transforms

# COMMAND ----------


def output_path(
    record_type,
    named_source="klaviyo",
    dataset_id=DATASET_ID,
    job_run_id=JOB_RUN_ID,
    bucket=BUCKET,
    client=CLIENT,
):
    prefix = f"s3://{bucket}/jobs/{client}/{named_source}__{record_type}/dataset_id_partition={dataset_id}/job_run_id={job_run_id}"
    print(prefix)
    return prefix


def get_latest_rows(table, partition_by: str):
    "Selects the most recent records from data source from the file name, splitting on partition_by field"
    return (
        table.withColumn(
            "file_date",
            pull_out_date(F.input_file_name()),
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy(partition_by).orderBy(F.desc("file_date"))
            ),
        )
        .filter(F.col("row_number") == 1)
        .drop("file_date", "row_number")
    )


def hash_unix_timestamp():
    return (
        "db_" + hashlib.sha256(str(datetime.utcnow().timestamp()).encode()).hexdigest()
    )


JOB_RUN_ID = hash_unix_timestamp

# COMMAND ----------

# SCHEMAS
# People base fields, these are fields we know will be in the people object
# Since identities can have different custom fields depending on which lists they are subscribed to we will read
# in these below fields that we need and infer the whole file
people_base_scheama = """{
    "fields": [
        {
            "metadata": {},
            "name": "$address1",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$address2",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$city",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$country",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$first_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$last_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$organization",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$phone_number",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$region",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$timezone",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$title",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "$zip",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "object",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "updated",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
people_json_schema = json.loads(people_base_scheama)
people_base_scheama = T.StructType.fromJson(people_json_schema)
PEOPLE_BASE_FIELDS = {col["name"] for col in people_json_schema["fields"]}

events_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "datetime",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "event_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "event_properties",
            "nullable": true,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "List",
                        "nullable": true,
                        "type": "string"
                    },
                    {
                        "metadata": {},
                        "name": "Campaign Name",
                        "nullable": true,
                        "type": "string"
                    },
                    {
                        "metadata": {},
                        "name": "Subject",
                        "nullable": true,
                        "type": "string"
                    }
                ],
                "type": "struct"
            }
        },
        {
            "metadata": {},
            "name": "person",
            "nullable": true,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "id",
                        "nullable": true,
                        "type": "string"
                    }
                ],
                "type": "struct"
            }
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "object",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "statistic_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "timestamp",
            "nullable": true,
            "type": "long"
        },
        {
            "metadata": {},
            "name": "uuid",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""

events_schema = T.StructType.fromJson(json.loads(events_schema))

list_exclusions_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "reason",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
list_exclusions_schema = T.StructType.fromJson(json.loads(list_exclusions_schema))

global_exclusions_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "timestamp",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "reason",
            "nullable": true,
            "type": "string"
        }

    ],
    "type": "struct"
}"""
global_exclusions_schema = T.StructType.fromJson(json.loads(global_exclusions_schema))

list_subs_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "phone_number",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
list_subs_schema = T.StructType.fromJson(json.loads(list_subs_schema))

lists_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "created",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "folder_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "updated",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
lists_schema = T.StructType.fromJson(json.loads(lists_schema))

lists_mapping_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "list_name",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "list_id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
lists_mapping_schema = T.StructType.fromJson(json.loads(lists_mapping_schema))

segments_schema = """{
    "fields": [
        {
            "metadata": {},
            "name": "email",
            "nullable": true,
            "type": "string"
        },
        {
            "metadata": {},
            "name": "segment_id",
            "nullable": true,
            "type": "string"
        }
    ],
    "type": "struct"
}"""
segments_schema = T.StructType.fromJson(json.loads(segments_schema))


# Schema for the above fields that we need for the customer record

spark.conf.set("spark.sql.caseSensitive", "true")
events_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/timeline_*.ndjson.gz",
    events_schema,
)
people_source_inferred = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/people_2*.ndjson.gz"
)

list_subs_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/group_all*.ndjson.gz",
    list_subs_schema,
)
# put list_ids from file name into the list_id column
list_subs_source = list_subs_source.withColumn(
    "list_id", F.split(F.input_file_name(), "_")[3]
)

lists_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/lists_mapping*.ndjson.gz",
    lists_mapping_schema,
)

people_source = spark.read.json(
    f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/people_2*.ndjson.gz",
    people_base_scheama,
)

# These files won't always exist
try:
    list_exclusions_source = spark.read.json(
        f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/list_exclusion_*.ndjson.gz",
        list_exclusions_schema,
    )
except AnalysisException:
    list_exclusions_source = sqlContext.createDataFrame(
        sc.emptyRDD(), list_exclusions_schema
    )
try:
    global_exclusions_source = spark.read.json(
        f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/people_excluded_*.ndjson.gz",
        global_exclusions_schema,
    )
except AnalysisException:
    global_exclusions_source = sqlContext.createDataFrame(
        sc.emptyRDD(), global_exclusions_schema
    )
try:
    segment_id_to_email = spark.read.json(
        f"s3://{BUCKET}/integrations/provider={PROVIDER}/account_id=*/year=*/month=*/day=*/segments_*.ndjson.gz",
        segments_schema,
    )
except AnalysisException:
    # Make an empty table
    fields = [
        T.StructField("email", T.StringType(), True),
        T.StructField("segment_id", T.StringType(), True),
    ]
    schema = T.StructType(fields)
    segment_id_to_email = sqlContext.createDataFrame(sc.emptyRDD(), schema)

list_exclusions_source = list_exclusions_source.filter(F.to_timestamp("created") <= exclusions_max_timestamp)
global_exclusions_source = global_exclusions_source.filter(F.to_timestamp("timestamp") <= exclusions_max_timestamp)
events_source = events_source.filter(F.col("timestamp") >= events_min_timestamp).filter(F.col("timestamp") <= events_max_timestamp)

# COMMAND ----------

# Create a complete, deduplicated view of data gathered from S3

# We pull down segment lists historically daily so we only want the most recent file
# Give it a date column
segment_id_to_email = segment_id_to_email.withColumn(
    "file_date",
    pull_out_date(F.input_file_name()),
)

# Get the max date as str constant
most_recent_segment_date = segment_id_to_email.select(F.max("file_date")).collect()[0][
    0
]

# Filter out all but most recent
segment_id_to_email = segment_id_to_email.filter(
    F.col("file_date") == most_recent_segment_date
)

grouped_segments = segment_id_to_email.groupby("email").agg(
    F.collect_list("segment_id").alias("segments")
)

# The events table won't change so dropping duplicates is fine
events_persist = events_source.drop_duplicates()

# The events table won't change so dropping duplicates is fine
events_persist = events_source.drop_duplicates()

# Keep only the latest provided row for each list id
lists_persist = get_latest_rows(lists_source, "list_id")

# Keep only the latest provided row for each people id
people_persist = get_latest_rows(people_source, "id")

people_persist_inferred = get_latest_rows(people_source_inferred, "id")
# Get the custom fields from the people inferred file
PEOPLE_INFERRED_COLUMNS = set(people_source_inferred.columns)

CUSTOM_COLUMNS = list(PEOPLE_INFERRED_COLUMNS - PEOPLE_BASE_FIELDS)
identities_custom_columns = people_persist_inferred.withColumn(
    # The `` helps deal with weirdly named custom columns like names with . in them
    "custom_columns",
    F.struct([F.col(f"`{c}`") for c in CUSTOM_COLUMNS]),
).select("$email", "custom_columns", "id")
# COMMAND ----------

# Add file date to the table and then add a status column
list_exclusions_fit = (
    list_exclusions_source.withColumn("file_date", pull_out_date(F.input_file_name()))
    .withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy([F.col("email"), F.col("list_id")]).orderBy(
                F.desc("file_date")
            )
        ),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

global_exclusions_fit = (
    global_exclusions_source.withColumn("file_date", pull_out_date(F.input_file_name()))
    .withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy([F.col("email"), F.col("list_id")]).orderBy(
                F.desc("file_date")
            )
        ),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
# Map exclusions "reason" column values
mapping = {
    "unsubscribed": "Unsubscribed",
    "invalid_email": "Invalid Email",
    "bounced": "Bounced",
    "reported_spam": "Reported Spam",
}
global_exclusions_fit = global_exclusions_fit.withColumnRenamed(
    "timestamp", "created"
).replace(to_replace=mapping, subset=["reason"])


exclusions_fit = global_exclusions_fit.union(list_exclusions_fit)
exclusions_fit = exclusions_fit.withColumn(
    "status", F.lit("undeliverable_in_lists")
).where(F.col("email").isNotNull())

exclusions_fit = exclusions_fit.withColumn("status", F.lit("undeliverable_in_lists"))

# COMMAND ----------

# Gather deliverable and undeliverable in lists
list_subs_fit = (
    list_subs_source.withColumn(
        "file_date",
        pull_out_date(F.input_file_name()),
    )
    .withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy([F.col("list_id"), F.col("email")]).orderBy(
                F.desc("file_date")
            )
        ),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# Give a status of active in lists
list_subs_fit = list_subs_fit.withColumn("status", F.lit("active_in_lists"))

list_subs_fit = list_subs_fit.join(
    people_persist.select("id", "$email"),
    on=(
        [
            list_subs_fit["id"] == people_persist["id"],
            list_subs_fit["email"] == people_persist["$email"],
        ]
    ),
    how="left",
).drop("$email")

# Exclude exlusion people from list subscription if list id and email address matches.
list_subs_fit = list_subs_fit.withColumn(
    "sub_file_date", F.to_timestamp("file_date", "yyyy/MM/dd")
).drop("file_date")

list_subs_fit = list_subs_fit.join(
    exclusions_fit.select(
        "email",
        F.to_timestamp("file_date", "yyyy/MM/dd").alias("unsub_file_date"),
        "list_id",
    ),
    on=["email", "list_id"],
    how="left",
)

# COMMAND ----------

list_subs_excluded = list_subs_fit.where(F.col("unsub_file_date").isNull())

# Edge cases. Unsubscribed and later subscribed again.
second_subscribe = list_subs_fit.where(
    F.col("sub_file_date") > F.col("unsub_file_date")
)

# Bring them altogether

list_subs_clean = list_subs_excluded.union(second_subscribe)

list_status = list_subs_clean.select("email", "list_id", "status")

list_status = list_status.union(exclusions_fit.select("email", "list_id", "status"))

# Bring in list name
list_status = list_status.join(
    lists_source, on=(list_status.list_id == lists_source.list_id), how="left"
).distinct()

list_status = list_status.groupby("email", "status").agg(
    F.collect_list("list_name").alias("list_names")
)

# Make sure that list status has at list one list name
list_status = list_status.where(F.size("list_names") > 0)
assert (
    list_status.where(F.size("list_names") > 0).count() > 0
), "This needs to be larger then zero"


# Collect all the email and pivot on status
list_status = list_status.groupBy(["email"]).pivot("status").agg(F.first("list_names"))

# Lit string (none) if null.
# If an identity didn't susbscribe to any list it'll have (none) in active in list and vice versa
list_status = list_status.withColumn(
    "active_in_lists",
    F.when(
        F.col("active_in_lists").isNull(), F.array(F.lit("Active in no list"))
    ).otherwise(F.col("active_in_lists")),
)

if "undeliverable_in_lists" not in list_status.columns:
    list_status = list_status.withColumn("undeliverable_in_lists", F.lit(None))

list_status = list_status.withColumn(
    "undeliverable_in_lists",
    F.when(
        F.col("undeliverable_in_lists").isNull(),
        F.array(F.lit("Undeliverable in no list")),
    ).otherwise(F.col("undeliverable_in_lists")),
)

# COMMAND ----------

# Copy the source customer dataframe `customer_source` for transformation as `customer_fit`
customer_fit = people_persist

# Set the DATASET_ID and record type
customer_fit = customer_fit.withColumn("dataset_id", F.lit(DATASET_ID))
customer_fit = customer_fit.withColumn("version", F.lit(VERSION))
customer_fit = customer_fit.withColumn("type", F.lit("customer_record"))


# We may add these later during schema validataion
# customer_fit = customer_fit.withColumn('metadata', F.lit("{}"))
# customer_fit = customer_fit.withColumn('custom_fields', F.lit("{}"))

# Set the record link as email
customer_fit = customer_fit.withColumn(
    "link",
    F.struct(
        F.lit("email").alias("link_type"),
        F.col("$email").cast("string").alias("link_value"),
    ),
)

# Layout the customer struct from the fields Shopify make available
customer_fit = customer_fit.withColumn(
    "customer",
    F.struct(
        F.col("$email").alias("email"),
        F.col("id").alias("customer_id"),
        F.col("$first_name").alias("first_name"),
        F.col("$last_name").alias("last_name"),
        F.col("$address1").alias("address_1"),
        F.col("$address2").alias("address_2"),
        F.col("$city").alias("city"),
        F.col("$region").alias("state"),
        F.col("$zip").alias("zip"),
        F.col("$country").alias("country"),
    ),
)

# Add "active_in_lists", "undeliverable_in_lists" from list_status to dataframe
customer_fit = customer_fit.join(
    list_status, on=(customer_fit["$email"] == list_status["email"]), how="left"
)
# Inferred Gender
inf_gndr = spark.read.csv(
    f"s3a://{BUCKET}/datasets/reference_data/name_gender_lookup_table.csv",
    header=True,
)
customer_fit = customer_fit.withColumn(
    "first_name", F.trim(F.upper(F.col("$first_name")))
)
customer_fit = customer_fit.join(inf_gndr, on="first_name", how="left")

# Add 'segments' col to customer dataframe
customer_fit = customer_fit.join(
    grouped_segments,
    on=(customer_fit["$email"] == grouped_segments["email"]),
    how="left",
)

# If no one is excluded from lists, "undeliverable_in_lists" is never created
if "undeliverable_in_lists" not in customer_fit.columns:
    customer_fit = customer_fit.withColumn("undeliverable_in_lists", F.lit(None))

# Join to the custom fields file
customer_fit = customer_fit.join(
    identities_custom_columns,
    on=(customer_fit.customer.customer_id == identities_custom_columns["id"]),
    how="left",
).drop("$email")

# Set Custom Fields
customer_fit = customer_fit.withColumn(
    "custom_fields",
    F.struct(
        "inferred_gender",
        F.when(F.col("active_in_lists").isNull(), F.array(F.lit("Active in no list")))
        .otherwise(F.col("active_in_lists"))
        .alias("active_in_lists"),
        F.when(
            F.col("undeliverable_in_lists").isNull(),
            F.array(F.lit("Undeliverable in no list")),
        )
        .otherwise(F.col("undeliverable_in_lists"))
        .alias("undeliverable_in_lists"),
        F.when(F.col("segments").isNull(), F.array(F.lit("No Segments")))
        .otherwise(F.col("segments"))
        .alias("segments"),
        F.col("custom_columns"),
    ),
)

# Subset the columns to those required in target schema
# customer_fit = customer_fit.select('dataset_id', 'type', 'tags', 'metadata', 'custom_fields', 'link', 'customer')
customer_export_v2 = customer_fit.select(
    "dataset_id", "version", "type", "link", "customer", "custom_fields"
)
# TODO: When hugh gives over the rdd's for CDE, Test the transformation against the target schema
# customer_export = spark.createDataFrame(customer_export.rdd, customer_record_schema)

# COMMAND ----------

events_fit = events_persist
# Set the DATASET_ID and record type
events_fit = events_fit.withColumn("dataset_id", F.lit(DATASET_ID))
events_fit = events_fit.withColumn("version", F.lit(VERSION))

people_renamed = people_persist.withColumnRenamed("id", "p_id")
people_renamed = people_renamed.filter(F.col("p_id").isNotNull())

# Set the record link as email
events_fit = events_fit.join(
    people_renamed.select(F.col("p_id"), "$email"),
    on=([events_fit.person["id"] == people_renamed.p_id]),
    how="left",
)

# Casting and alias for events table
events_fit = events_fit.withColumn(
    "link",
    F.struct(
        F.lit("email").alias("link_type"),
        F.col("$email").cast("string").alias("link_value"),
    ),
)

EVENT_MAP = {
    "Subscribed to List": "email_subscribe",
    "Unsubscribed from List": "email_subscribe",
    "Bounced Email": "email_bounce",
    "Received Email": "email_send",
    "Opened Email": "email_open",
    "Clicked Email": "email_click",
    "Unsubscribed": "email_subscribe",
}

EMAIL_EVENTS = [
    "email_bounce",
    "email_click",
    "email_open",
    "email_send",
    "email_subscribe",
]

# add a type column and replace whats in it with event map hash
events_fit = events_fit.withColumn("type", F.col("event_name"))
events_fit = events_fit.replace(to_replace=EVENT_MAP, subset=["type"])
events_fit = events_fit.where(F.col("type").isin(EMAIL_EVENTS))


events_fit = events_fit.withColumn("action_at", F.to_timestamp(F.col("timestamp")))

events_fit = events_fit.withColumn("history_key", F.concat("timestamp", "type", "p_id"))
events_fit = events_fit.drop_duplicates(subset=["history_key"])

events_fit = events_fit.withColumn("to", F.struct(F.col("$email").alias("email")))
events_fit = events_fit.withColumn("from", F.struct(F.lit(FROM_EMAIL).alias("email")))

events_fit = events_fit.withColumn("email_id", F.col("event_properties.Campaign Name"))
events_fit = events_fit.fillna(DEFAULT_EMAIL_ID, subset=["email_id"])

events_fit = events_fit.withColumn("subject", F.col("event_properties.Subject"))

# Similar as the above, I'm not all too sure about this
events_fit = events_fit.withColumn(
    "campaign", F.struct(F.col("event_properties.Campaign Name").alias("name"))
)

# As we brought deliverable and undeliverable together alias them to subscribed and unsubscribed
events_fit = events_fit.withColumn(
    "status",
    F.when(F.col("event_name") == "Subscribed to List", F.lit("subscribed"))
    .when(
        F.col("event_name").isin("Unsubscribed from List", "Unsubscribed"),
        "unsubscribed",
    )
    .otherwise(None),
)

BASE_EMAIL_EXPORT_FIELDS = [
    "action_at",
    "dataset_id",
    "version",
    "link",
    "history_key",
    "type",
    "campaign",
    "from",
    "to",
    "subject",
    "email_id",
]

email_bounce_export_v2 = events_fit.where(events_fit.type == "email_bounce").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_click_export_v2 = events_fit.where(events_fit.type == "email_click").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_open_export_v2 = events_fit.where(events_fit.type == "email_open").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_send_export_v2 = events_fit.where(events_fit.type == "email_send").select(
    BASE_EMAIL_EXPORT_FIELDS
)
email_subscribe_export_v2 = events_fit.where(
    (events_fit.type == "email_subscribe") & (events_fit.status == "subscribed")
).select(BASE_EMAIL_EXPORT_FIELDS + ["status"])

email_unsubscribe_export_v2 = events_fit.where(
    (events_fit.type == "email_subscribe") & (events_fit.status == "unsubscribed")
).select(BASE_EMAIL_EXPORT_FIELDS + ["status"])


# COMMAND ----------

# MAGIC %md
# MAGIC Make sure v1 and v2 transformed files have the same counts

# COMMAND ----------

email_unsubscribe_export_v1_count = email_unsubscribe_export_v1.count()
email_unsubscribe_export_v2_count = email_unsubscribe_export_v2.count()
email_subscribe_export_v1_count = email_subscribe_export_v1.count()
email_subscribe_export_v2_count = email_subscribe_export_v2.count()
email_send_export_v1_count = email_send_export_v1.count()
email_send_export_v2_count = email_send_export_v2.count()
email_open_export_v1_count = email_open_export_v1.count()
email_open_export_v2_count = email_open_export_v2.count()
email_click_export_v1_count = email_click_export_v1.count()
email_click_export_v2_count = email_click_export_v2.count()
email_bounce_export_v1_count = email_bounce_export_v1.count()
email_bounce_export_v2_count = email_bounce_export_v2.count()
customer_export_v1_count = customer_export_v1.count()
customer_export_v2_count = customer_export_v2.count()

# COMMAND ----------

export_compare = sqlContext.createDataFrame(sc.parallelize([
Row(file="unsub", count=email_unsubscribe_export_v1_count, name="unsub-v1"),
Row(file="unsub", count=email_unsubscribe_export_v2_count, name="unsub-v2"),
Row(file="sub", count=email_subscribe_export_v1_count, name="sub-v1"),
Row(file="sub", count=email_subscribe_export_v2_count, name="sub-v2"),
Row(file="send", count=email_send_export_v1_count, name="send-v1"),
Row(file="send", count=email_send_export_v2_count, name="send-v2"),
Row(file="open", count=email_open_export_v1_count, name="open-v1"),
Row(file="open", count=email_open_export_v2_count, name="open-v2"),
Row(file="click", count=email_click_export_v1_count, name="click-v1"),
Row(file="click", count=email_click_export_v2_count, name="click-v2"),
Row(file="bounce", count=email_bounce_export_v1_count, name="bounce-v1"),
Row(file="bounce", count=email_bounce_export_v2_count, name="bounce-v2"),
Row(file="customer", count=customer_export_v1_count, name="customer-v1"),
Row(file="customer", count=customer_export_v2_count, name="customer-v2")
  ]))
display(export_compare)

# COMMAND ----------

assert email_unsubscribe_export_v1.columns == email_unsubscribe_export_v2.columns
assert email_subscribe_export_v1.columns == email_subscribe_export_v2.columns
assert email_send_export_v1.columns == email_send_export_v2.columns
assert email_open_export_v1.columns == email_open_export_v2.columns
assert email_click_export_v1.columns == email_click_export_v2.columns
assert email_bounce_export_v1.columns == email_bounce_export_v2.columns
assert customer_export_v1.columns == customer_export_v2.columns 




# COMMAND ----------

def compare_df_counts(df1, df2, percentage, name):
    df1_count = df1.count()
    df2_count = df2.count()
    max_count = max(df1_count, df2_count)
    diff = max(df1_count, df2_count) - min(df1_count, df2_count)
    print(name, "df1_count", df1_count, "df2_count", df2_count, "diff", diff)
    if (diff / max_count) > percentage:
        raise Exception(f"V1 and V2 files counts percentage difference is greater than {percentage * 100.00}%")

# COMMAND ----------

# MAGIC %md #customer file count diffs
# MAGIC Due to us not being able to pull data at the same time for v1 and v2 integrations (rate limits) there will probably be small differences in the count of v1 vs v2 files, this is due to a few reasons
# MAGIC * While pulling events from the timeline endpoint we also pull down the people (customer) objects that come with these events to save time, this does mean that because the pulls are run at different times we COULD pull down different amounts of people from the events endpoint IF new events occured between the 2 pulls, which is fairly likely
# MAGIC * We also pull people records down as members of lists, so we pull all list_ids down and then pull down all the members of each list individually, these means that as people sub and unsub from lists between pulls we could pull a different number of people
# MAGIC 
# MAGIC The main thing to look at in the above graph is if the numbers look comparable, you can set the tolerance you want in the above compare_df_counts() function and assert that it is within range

# COMMAND ----------

compare_df_counts(email_unsubscribe_export_v1, email_unsubscribe_export_v2, 0.00001, "unsubs")
compare_df_counts(email_subscribe_export_v1, email_subscribe_export_v2, 0.00001, "subs")
compare_df_counts(email_send_export_v1, email_send_export_v2, 0.00001, "send")
compare_df_counts(email_open_export_v1, email_open_export_v2, 0.00001, "open")
compare_df_counts(email_click_export_v1, email_click_export_v2, 0.00001, "click")
compare_df_counts(email_bounce_export_v1, email_bounce_export_v2, 0.00001, "bounce")
compare_df_counts(customer_export_v1, customer_export_v2, 0.00001, "customer")

# COMMAND ----------


