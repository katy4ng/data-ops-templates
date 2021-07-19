# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # AVC experian import setup
# MAGIC 
# MAGIC AVC uses a custom unification script `unify.py`. 
# MAGIC 
# MAGIC We want to import Experian data to enrich AVC profiles. We **also** want to import all unmatched Experian profiles (the 'experian universe') as standalone records in the hub.
# MAGIC 
# MAGIC - If we process Experian data through `unify.py`, that will cause profiles to unify based on Experian information. We don't want that.
# MAGIC 
# MAGIC So, we really want to match our pre-unified profiles up against Experian 'where possible' (without causing new unification) and then import the remaining profiles standalone.
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC As at July 2021 we had done this once already, using `email` as the join point for Experian data. 
# MAGIC 
# MAGIC - In that import, we used `pin_to_hin.csv` for email<->pin matching.
# MAGIC   - This `pin_to_hin.csv` file is the output of "enriching" the ConsumerView `PIN|HIN|email` columns with Experian Linkage `PIN|HIN|email` info. Producing that file is its own exercise.
# MAGIC   - Experian Linkage is generally a poorer quality dataset than CV, but we normally use it to increase match rate.
# MAGIC  
# MAGIC Even with the Linkage-enriched `pin_to_hin.csv`, the AVC Experian import yielded a low match rate for AVC (~12.8%). This was not good enough.
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC This script implements an approach to use both of the AVC `unify.py` assets: `lexer_email` and `lexer_mobile`, to join to ConsumerView, WITHOUT any help from Linkage data (aka `pin_to_hin.csv`). It does this in a safe and conservative way, that will not cause new unification but does improve the AVC-Experian CV match rate to about ~28.5% as at July 2021.
# MAGIC 
# MAGIC The output of this script is a new `pin_to_lexer_id.csv` file which can easily be used to join to Experian datasets in an import.
# MAGIC 
# MAGIC 
# MAGIC ## Before you import AVC Experian data
# MAGIC 
# MAGIC You will pretty much always need to purge:
# MAGIC - Every *profile* without a `.lexer_id` 
# MAGIC - Every experian (including grand index or consumer view) attribute value on the remaining profiles

# COMMAND ----------

# DBTITLE 1,Basic notebook setup / generic helpers
from pyspark.sql import functions as F
from pyspark.sql import functions as T


def db_headers(yml_str):
  """
  Prepares a list of column headers ready to use in a Spark schema.
  Where yml_str is a `columns` block, including the `columns` label, from an ETL db schema. 
  For example:
  
    columns:
      - { column: emailId,            sql_type: text,         type: string           }
      - { column: contactId,          sql_type: text,         type: string           }
      - { column: launch_date,        sql_type: timestamptz,  type: datetime_ymd     }
      - { column: delivery_status,    sql_type: text,         type: string           }
      - { column: bounce_status,      sql_type: text,         type: string           }
  
  """
  import yaml
  dct = yaml.safe_load(yml_str)
  cols = [d['column'] for d in dct['columns']]
  return cols



def read_ss(path, yml_cols):
  from pyspark.sql import types as T
  HEADERS = db_headers(yml_cols)
  df_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in HEADERS])
  return spark.read.csv(path, schema=df_schema, sep="|")
  

  
def write_single_csv(spark_df, out_path, header=True, sep="|"):
  # Remove the pre-existing single file if it exists
  try:
    print("Removing old version if one exists...")
    dbutils.fs.rm(out_path)
    print("Now removed. Proceeding...")
  except:
    print("No prior file found. Proceeding...")
    
  # Save, as a single "part"
  # NB: repartition(1) is expensive, and may run OOM with large datasets
  spark_df.repartition(1) \
    .write.format("csv") \
    .option("header", header) \
    .option("sep", sep) \
    .save(out_path)
  
  # Find the single file we care about
  out_files = dbutils.fs.ls(out_path)
  parts = [f for f in out_files if "part" in f.path]
  single_part = parts[0].path
  
  # Clean up
  dbutils.fs.mv(single_part, out_path+'tmp')
  dbutils.fs.rm(out_path, recurse=True)
  dbutils.fs.mv(out_path+'tmp', out_path)
  return True  

# COMMAND ----------

# DBTITLE 1,Stupid column headers
cv_cols = """
columns:
        - {column: PIN, sql_type: text, type: string}
        - {column: HIN, sql_type: text, type: string}
        - {column: TitleCode, sql_type: text, type: string}
        - {column: FirstName, sql_type: text, type: string}
        - {column: Middlename, sql_type: text, type: string}
        - {column: Surname, sql_type: text, type: string}
        - {column: Address1, sql_type: text, type: string}
        - {column: Address2, sql_type: text, type: string}
        - {column: Suburb, sql_type: text, type: string}
        - {column: State, sql_type: text, type: string}
        - {column: Postcode, sql_type: text, type: string}
        - {column: Metro, sql_type: text, type: string}
        - {column: Actual_Age, sql_type: text, type: string}
        - {column: Age_Code, sql_type: text, type: string}
        - {column: Age_Band, sql_type: text, type: string}
        - {column: DOB, sql_type: text, type: string}
        - {column: Gender, sql_type: text, type: string}
        - {column: Phone_Land, sql_type: text, type: string}
        - {column: Phone_Land_Suppress, sql_type: text, type: string}
        - {column: PNDPDR_Validated_Landline, sql_type: text, type: string}
        - {column: Phone_Mobile, sql_type: text, type: string}
        - {column: Phone_Mobile_Suppress, sql_type: text, type: string}
        - {column: PNDPDR_Validated_Mobile, sql_type: text, type: string}
        - {column: EmailAddress, sql_type: text, type: email}
        - {column: Email_Suppress, sql_type: text, type: string}
        - {column: Mail_Suppress, sql_type: text, type: string}
        - {column: PMM_Known_Deceased, sql_type: text, type: string}
        - {column: Deceased_Match_Type, sql_type: text, type: string}
        - {column: Property_Type, sql_type: text, type: string}
        - {column: MeshBlock_2016, sql_type: text, type: string}
        - {column: SA1_2016, sql_type: text, type: string}
        - {column: AARF_RES_IND, sql_type: text, type: string}
        - {column: AARF_NONRES_IND, sql_type: text, type: string}
        - {column: AARF_NAM_IND, sql_type: text, type: string}
        - {column: Undel_Code, sql_type: text, type: string}
        - {column: Gnaf_Pid, sql_type: text, type: string}
        - {column: Gnaf_Latitude, sql_type: text, type: string}
        - {column: Gnaf_Longitude, sql_type: text, type: string}
        - {column: Length_Of_Residence_Code, sql_type: text, type: string}
        - {column: Length_Of_Residence_Code_Desc, sql_type: text, type: string}
        - {column: Head_Of_HouseHold_Age_Code, sql_type: text, type: string}
        - {column: Head_Of_HouseHold_Age_Code_Desc, sql_type: text, type: string}
        - {column: Children_At_Address_Child0_10, sql_type: text, type: string}
        - {column: Children_At_Address_Child11_18, sql_type: text, type: string}
        - {column: Household_Composition, sql_type: text, type: string}
        - {column: Household_Composition_Desc, sql_type: text, type: string}
        - {column: LifeStage_Code, sql_type: text, type: string}
        - {column: LifeStage_Code_Desc, sql_type: text, type: string}
        - {column: Household_Income_Code, sql_type: text, type: string}
        - {column: Household_Income_Code_Desc, sql_type: text, type: string}
        - {column: Affluence_Code, sql_type: text, type: string}
        - {column: Affluence_Code_Desc, sql_type: text, type: string}
        - {column: AdultsAtAddress, sql_type: text, type: string}
        - {column: MOSAIC_Type_Group, sql_type: text, type: string}
        - {column: MOSAIC_Segment, sql_type: text, type: string}
        - {column: MOSAIC_Global, sql_type: text, type: string}
        - {column: Factors_perc_1, sql_type: text, type: string}
        - {column: Factors_perc_2, sql_type: text, type: string}
        - {column: Factors_perc_3, sql_type: text, type: string}
        - {column: Factors_perc_4, sql_type: text, type: string}
        - {column: Factors_perc_5, sql_type: text, type: string}
        - {column: Factors_dec_1, sql_type: text, type: string}
        - {column: Factors_dec_2, sql_type: text, type: string}
        - {column: Factors_dec_3, sql_type: text, type: string}
        - {column: Factors_dec_4, sql_type: text, type: string}
        - {column: Factors_dec_5, sql_type: text, type: string}
        - {column: Factors_score_1, sql_type: text, type: string}
        - {column: Factors_score_2, sql_type: text, type: string}
        - {column: Factors_score_3, sql_type: text, type: string}
        - {column: Factors_score_4, sql_type: text, type: string}
        - {column: Factors_score_5, sql_type: text, type: string}
        - {column: Risk_Insight, sql_type: text, type: string}
        - {column: Property_Size_Sq_Meters, sql_type: text, type: string}
        - {column: Property_Size_Sq_Meters_Band, sql_type: text, type: string}
        - {column: Credit_Demand, sql_type: text, type: string}
        - {column: Credit_Demand_Valid_to_Date, sql_type: text, type: string}
        - {column: PostalArea_2016, sql_type: text, type: string}
        - {column: Channel_Preference, sql_type: text, type: string}
        - {column: Channel_Preference_Desc, sql_type: text, type: string}
        - {column: Credit_Demand_FB_Grouped, sql_type: text, type: string}
        - {column: DateFirstSeen, sql_type: text, type: string}
        - {column: DateLastSeen, sql_type: text, type: string}
        - {column: ListedType, sql_type: text, type: string}
        - {column: ListedPrice, sql_type: text, type: string}
        - {column: Nbr_Bedrooms, sql_type: text, type: string}
        - {column: Nbr_Bedrooms_Src, sql_type: text, type: string}
        - {column: Nbr_Bathrooms, sql_type: text, type: string}
        - {column: Nbr_Bathrooms_Src, sql_type: text, type: string}
        - {column: Nbr_Carspaces, sql_type: text, type: string}
        - {column: Nbr_Carspaces_Src, sql_type: text, type: string}
        - {column: Land_size, sql_type: text, type: string}
        - {column: Land_size_desc, sql_type: text, type: string}
        - {column: Land_Size_Src, sql_type: text, type: string}
        - {column: Dwelling_Area, sql_type: text, type: string}
        - {column: Dwelling_Area_desc, sql_type: text, type: string}
        - {column: Dwelling_Area_Src, sql_type: text, type: string}
        - {column: Home_Market_Value, sql_type: text, type: string}
        - {column: Home_Market_Value_Desc, sql_type: text, type: string}
        - {column: Home_Market_Value_Src, sql_type: text, type: string}
        - {column: Rental_Market_Value, sql_type: text, type: string}
        - {column: Rental_Market_Value_Desc, sql_type: text, type: string}
        - {column: Rental_Market_Value_Src, sql_type: text, type: string}
        - {column: Rental_Listing_Price_24mth, sql_type: text, type: string}
        - {column: Rental_Listing_Price_24mth_Src, sql_type: text, type: string}
        - {column: Sale_Listing_Price_24mth, sql_type: text, type: string}
        - {column: Sale_Listing_Price_24mth_Src, sql_type: text, type: string}
        - {column: Wealth_Accumulation, sql_type: text, type: string}
        - {column: Wealth_Accumulation_Desc, sql_type: text, type: string}
        - {column: Wealth_Accumulation_Src, sql_type: text, type: string}
        - {column: Cooling_Indicator, sql_type: text, type: string}
        - {column: Cooling_Indicator_Src, sql_type: text, type: string}
        - {column: Heating_Indicator, sql_type: text, type: string}
        - {column: Heating_Indicator_Src, sql_type: text, type: string}
        - {column: Gas_Indicator, sql_type: text, type: string}
        - {column: Gas_Indicator_Src, sql_type: text, type: string}
        - {column: Solar_Indicator, sql_type: text, type: string}
        - {column: Solar_Indicator_Src, sql_type: text, type: string}
        - {column: Solar_Prediction, sql_type: text, type: string}
        - {column: Solar_Prediction_desc, sql_type: text, type: string}
        - {column: Solar_Prediction_Src, sql_type: text, type: string}
        - {column: Solar_First_Date, sql_type: text, type: string}
        - {column: Solar_First_Date_Src, sql_type: text, type: string}
        - {column: Occupant_Type, sql_type: text, type: string}
        - {column: Occupant_Type_Desc, sql_type: text, type: string}
        - {column: Occupant_Type_Src, sql_type: text, type: string}
        - {column: Pool_Indicator, sql_type: text, type: string}
        - {column: Pool_Indicator_Src, sql_type: text, type: string}
        - {column: SA1_MAIN16, sql_type: text, type: string}
        - {column: SA2_MAIN16, sql_type: text, type: string}
        - {column: SA2_5DIG16, sql_type: text, type: string}
        - {column: SA2_NAME16, sql_type: text, type: string}
        - {column: SA3_CODE16, sql_type: text, type: string}
        - {column: SA3_NAME16, sql_type: text, type: string}
        - {column: SA4_CODE16, sql_type: text, type: string}
        - {column: SA4_NAME16, sql_type: text, type: string}
        - {column: TPID, sql_type: text, type: string}
"""

# COMMAND ----------

# DBTITLE 1,Reading in base files
# The AVC unify.py persistent "linkage assets"
lex_email = spark.read.csv("s3://lexer-client-avc/datasets/lexer_email.csv", header=True, sep="|")
lex_mobile = spark.read.csv("s3://lexer-client-avc/datasets/lexer_mobile.csv", header=True, sep="|")


# NOTE this is just the normal experian_full ConsumerView file!
# We have pinched it from a different client bucket.
cv = read_ss("s3://lexer-client-cub/deltas/cub_experian/snapshots/current/experian_full.csv", cv_cols)


# COMMAND ----------

# DBTITLE 1,Shore up CV dataset
# We only need the identifier columns
cv = (
  cv
    .withColumnRenamed("PIN", 'pin')
    .withColumnRenamed("Phone_Mobile", 'mobile')
    .withColumnRenamed("EmailAddress", 'email')
    .select("pin", "email", "mobile")
)

# Mobile needs to be in ETL format
cv = cv.withColumn('mobile', F.concat(F.lit('61'), F.substring("mobile", 2,9)))


# COMMAND ----------

cv.select(F.count("email"), F.count("*")).toPandas().transpose()

# COMMAND ----------

# DBTITLE 1,Preliminary join helpers for lexer_* unify.py files (no decision logic yet)
def joinlink(df, linkage_snapshot, identifier):
  for c in linkage_snapshot.columns:
    linkage_snapshot = linkage_snapshot.withColumnRenamed(c, f"{c}_{identifier}")
  df = df.join(linkage_snapshot, df[identifier] == linkage_snapshot[f"{identifier}_{identifier}"], 'left')
  df = df.drop(f"{identifier}_{identifier}")
  return df


def identify(df, drop_helpers=True):
  id_cols = ['lexer_id_email', 'lexer_id_mobile']
  
  # Take any available lexer_id, preferring lexer_id_email for now
  df = df.withColumn("lexer_id", F.coalesce(*id_cols))
  
  # Flag when conflicting lexer_ids are visible.  These conflicts will be handled later.
  df = df.withColumn(
    "unify_conflict",
    F.col('lexer_id_email').isNotNull() &
    F.col('lexer_id_mobile').isNotNull() &
    (F.col('lexer_id_mobile') != F.col('lexer_id_email'))
  )
  
  # Can now discard helper columns
  if (drop_helpers):
    df = df.drop(*id_cols)
    
  return df
  

def base_joins(df, drop_helpers=True):  
  df = joinlink(df, lex_email, 'email')
  df = joinlink(df, lex_mobile, 'mobile')
  df = identify(df, drop_helpers)
  return df

# COMMAND ----------

# DBTITLE 1,Joining up to yield pin_to_lexer_id
# Preliminary joins to unify.py assets.
# Do not discard any results yet
cv = base_joins(cv, True)


# No lexer_id discoverable means this record cannot be matched
cv = cv.where(F.col("lexer_id").isNotNull())


# `unify_conflict=True` means the Experian email + mobile linked to different Lexer profiles
# We do not assume either is correct, and we do not unify further off Experian data
# So, we just reject both results.
cv = cv.where(F.col('unify_conflict') != F.lit(True))


# If a lexer profile matched to multiple Experian profiles, 
# We do not assume any are correct, and we do not unify further off Experian data
# So, we just reject both results
ids = (
  cv
    .select("pin", "lexer_id")
    .groupBy("lexer_id")
    .agg(F.count('lexer_id').alias('count'))
    .where('count == 1')
    .drop('count')
)

# Now we have the right ids, just need id|pin
pin_to_lexer_id = ids.join(cv, "lexer_id", 'left')
pin_to_lexer_id = pin_to_lexer_id.select("pin", "lexer_id")
pin_to_lexer_id = pin_to_lexer_id.cache()


# COMMAND ----------

# DBTITLE 1,Quick sense check
pin_to_lexer_id.limit(10).display()
pin_to_lexer_id.count()

# COMMAND ----------

# DBTITLE 1,Save output
write_single_csv(pin_to_lexer_id, "s3://lexer-client-avc/datasets/pin_to_lexer_id.csv")
