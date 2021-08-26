# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **How to use this template:**   
# MAGIC Name the file with the "date-eda-task" format. EG: "20-06-24-eda-duplicate_skus"   
# MAGIC Clone the file to a folder in your workspace that follows the lexer-client-format Eg: "lexer-client-datarockstar"   
# MAGIC Follow the basic structure laid out in the notebook.

# COMMAND ----------

from data_toolkit.s3_helpers import S3Utils  # See repo here https://github.com/lexerdev/data-toolkit

# Databricks Librarys
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Python Librarys
import json
import black

INTERACTIVE = True


# COMMAND ----------

# ToDo: Functions Description

def compare_dataframe_counts(func):
    def inner(*a, **kw):
        if not isinstance(a[0], DataFrame):
            print('your dataframe must be the first positional argument to be count()ed')
            return func(*a, **kw)
        print('before:', a[0].count())
        df = func(*a, **kw)
        print('after:', df.count())
        return df
    return inner

def print_result(func):
    def inner(*args, **kwargs):
        result = func(*args, **kwargs)
        print(result)
        return result
    return inner

def display_result(func):
    def inner(*args, **kwargs):
        result = func(*args, **kwargs)
        result.display()
        return result
    return inner

# Some one-off helper functions to render the schemas and display them nicely in different forms

def pretty_print_schema_obj(schema_json):
    print(black.format_str(str(T.StructType.fromJson(json.loads(schema_json))), mode=black.FileMode(line_length=100)))

def print_unflattened_json(flat_json):
    print(json.dumps(json.loads(flat_json), indent=2))
    
def with_corrupt_record_col(json_schema: str) -> str:
    tmp = json.loads(json_schema)
    tmp["fields"].append(
        {
            "metadata": {},
            "name": "_corrupt_record",
            "nullable": True,
            "type": "string"
        }
    )
    return json.dumps(tmp,indent=2)

# COMMAND ----------

# MAGIC %sh pip show data_toolkit

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###List available sources

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions
# MAGIC 
# MAGIC This defines each schema statically as JSON!
# MAGIC 
# MAGIC 1. They're easier to transport, JSON is a well-supported data structure that any language can handle, not python-specific
# MAGIC 2. They require no editing from the form that you can print out
# MAGIC   - As opposed to the object/"StructType" schemas, that require manual edits like List -> [], True -> true etc.
# MAGIC 3. They can be converted easily from JSON -> object/"StructType" forms and back again
# MAGIC 
# MAGIC 
# MAGIC ```python
# MAGIC # Convert from JSON -> StructType
# MAGIC # This "fromJson" method should be called fromDict, as it accepts a dict rather than a JSON string
# MAGIC OBJ_SCHEMA = T.StructType.fromJson(json.loads(JSON_SCHEMA))
# MAGIC 
# MAGIC # Convert from StructType -> JSON
# MAGIC OBJ_SCHEMA.json()
# MAGIC 
# MAGIC # Convert from StructType -> pretty-printed/indented JSON
# MAGIC json.dumps(json.loads(OBJ_SCHEMA.json()), indent=2)
# MAGIC ```

# COMMAND ----------



# COMMAND ----------


