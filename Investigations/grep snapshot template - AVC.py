# Databricks notebook source
# this notebook is to save some time finding which AVC snapshots an email address exists in, to avoid the snapshots re-loading profiles after a GDPR
# list the email/s to find in the array in cmd 3. re-run cmd 4 to update snapshot list.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.utils import AnalysisException
from data_toolkit.s3_helpers import S3Utils

# COMMAND ----------

grep_val = ["{email_address_to_find}"] # can list multiple emails in the array

# COMMAND ----------

# df = S3Utils.list_keys("s3://lexer-client-avc/deltas/avc/snapshots/current/")
# df.where(F.col("path").like("%%")).display()

# COMMAND ----------

click_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/click_snapshot.csv', sep='|', header = False)
contact_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/contact_snapshot.csv', sep='|', header = False)
customer_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/customer_snapshot.csv', sep='|', header = False)
form_submissions_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/form_submissions_snapshot.csv', sep='|', header = False)
function_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/function_snapshot.csv', sep='|', header = False)
loyalty_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/loyalty_snapshot.csv', sep='|', header = False)
open_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/open_snapshot.csv', sep='|', header = False)
redeemed_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/redeemed_snapshot.csv', sep='|', header = False)
reservation_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/reservation_snapshot.csv', sep='|', header = False)
send_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/send_snapshot.csv', sep='|', header = False)
ticket_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/ticket_snapshot.csv', sep='|', header = False)
transaction_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/transaction_snapshot.csv', sep='|', header = False)
venue_snapshot = spark.read.csv('s3://lexer-client-avc/deltas/avc/snapshots/current/transaction_snapshot.csv', sep='|', header = False)

# COMMAND ----------

venue_snapshot.show(truncate = False)

# COMMAND ----------

print("click_snapshot")
click_snapshot.where(F.col("_c0").isin(grep_val)).show(truncate = False)
print("contact_snapshot")
contact_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
print("customer_snapshot")
customer_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
print("form_submissions_snapshot")
form_submissions_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
print("function_snapshot")
function_snapshot.where(F.col("_c2").isin(grep_val)).show(truncate = False)
print("loyalty_snapshot")
loyalty_snapshot.where(F.col("_c0").isin(grep_val)).show(truncate = False)
print("open_snapshot")
open_snapshot.where(F.col("_c0").isin(grep_val)).show(truncate = False)
print("redeemed_snapshot")
redeemed_snapshot.where(F.col("_c0").isin(grep_val)).show(truncate = False)
print("reservation_snapshot")
reservation_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
print("send_snapshot")
send_snapshot.where(F.col("_c0").isin(grep_val)).show(truncate = False)
print("ticket_snapshot")
ticket_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
#transaction_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)
#venue_snapshot.where(F.col("_c1").isin(grep_val)).show(truncate = False)

# COMMAND ----------


