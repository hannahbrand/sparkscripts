from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas

# -------------------
# 1. Start Spark
# -------------------
spark = SparkSession.builder \
    .appName("Eurostat Education Gender Data Analysis") \
    .getOrCreate()

# -------------------
# 2. File paths
# -------------------
# Change to hdfs:/// if using HDFS
path1 = "hdfs:////project/edu.csv"
path2 = "hdfs:////project/sdg.csv"

# -------------------
# 3. Load CSVs
# -------------------
df1 = spark.read.csv(path1, header=True, inferSchema=True)
df2 = spark.read.csv(path2, header=True, inferSchema=True)

# -------------------
# 4. Drop unused columns
# -------------------
cols_to_drop = ["DATAFLOW", "unit", "nace_r2", "OBS_FLAG"]
df1 = df1.drop(*[c for c in cols_to_drop if c in df1.columns])
df2 = df2.drop(*[c for c in cols_to_drop if c in df2.columns])

# -------------------
# 5. Align schemas
# -------------------
missing_cols_1 = set(df2.columns) - set(df1.columns)
missing_cols_2 = set(df1.columns) - set(df2.columns)

for col in missing_cols_1:
    df1 = df1.withColumn(col, F.lit(None))

for col in missing_cols_2:
    df2 = df2.withColumn(col, F.lit(None))

df1 = df1.select(sorted(df1.columns))
df2 = df2.select(sorted(df2.columns))

# -------------------
# 6. Merge datasets
# -------------------
df_merged = df1.join(df2, on=['TIME_PERIOD', 'geo'], how='left')

# -------------------
# 7. Create SQL view
# -------------------
# Save to Parquet (overwrite if it exists)
df_merged.write.mode("overwrite").parquet("eurostat_refreshed.parquet")

# -------------------
# 8. Example SQL queries
# -------------------

#

