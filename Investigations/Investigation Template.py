# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **How to use this template:**   
# MAGIC Name the file with the "date-eda-task" format. EG: "20-06-24-eda-duplicate_skus"   
# MAGIC Clone the file to a folder in your workspace that follows the lexer-client-format Eg: "lexer-client-datarockstar"   
# MAGIC Follow the basic structure laid out in the notebook.

# COMMAND ----------

from data_toolkit.s3_helpers import S3Utils
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import pyspark.sql.types as T

INTERACTIVE = True

# COMMAND ----------

# MAGIC %sh pip show data_toolkit

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Wisdom of the Ancients](https://imgs.xkcd.com/comics/wisdom_of_the_ancients.png "Wisdom of the Ancients")
# MAGIC 
# MAGIC ## *DEAR USERS OF THE FUTURE: Here's what we've learned so far...*
# MAGIC Always add a description of the work to be completed as it will be kept as a record for future users.

# COMMAND ----------

##########################################################################
# ASANA TASK 
# {https://app.asana.com..Paste ASANA Task Here}
##########################################################################

# Background: {Paste Asana description here}
##########################################################################




# Solutions Commentary:
##########################################################################
# @Solutions Analyst says: Seems to boil down to something like:
# Give some insight into the aim



# Outcomes:
##########################################################################
# Pase in some outcomes once you're finished. What did you find?


# Update 1:
##########################################################################
# Revisiting? Did someone else do something different? what was the update?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Files
# MAGIC Look at what is in S3 that can help us with our issue.

# COMMAND ----------

# Which Client are you exploring?
CLIENT = 'datarockstar'

# Which Source directory?
# Example: s3://lexer-client-{CLIENT}/integrations/provider=magento/account_id=11292/year=2021/month=06
SOURCE_DIR = f's3://lexer-client-{CLIENT}/'

sources = S3Utils.list_keys(SOURCE_DIR)
if INTERACTIVE:
    print(sources.count())
    display(sources)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## STEP 2: Explore the data
# MAGIC Below you can manipulate the data, be sure to leave a brief explanation of what you're looking for or what you find in the dataframes.

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## STEP 3: Transform Data
# MAGIC Now that we have a few ideas to explore, transform the DataFrames for outputs useful for reporting or export.

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 4: Findings
# MAGIC Anything to follow up after EDA?
# MAGIC Append to top of page.

# COMMAND ----------


