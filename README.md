# sparkscripts
<it> for our Informatica Feminale big data project </it>

PySpark Steps

- Install pyspark, run start-all.sh & check jps
- Write whatever python script you wish to run (i.e. project.py)
- Do not start pyspark cmd line (don’t type pyspark into terminal). Instead run spark-submit project.py from your terminal.


**Create Spark session:** spark = SparkSession.builder.appName("Eurostat").getOrCreate()

**Read CSV files: ** df = spark.read.csv("path", header=True, inferSchema=True)

**Drop columns:** df.drop("col1", "col2")

**Add missing columns with default value:**
import pyspark.sql.functions as F

df = df.withColumn("newcol", F.lit(None))

**Reorder/select columns:** df = df.select(sorted(df.columns))

**Union two DataFrames by column names: ** df_merged = df1.unionByName(df2)

**Show results:** df.show(20, truncate=False)

**Create a SQL view:** df.createOrReplaceTempView("eurostat")

**Run SQL queries: ** spark.sql("SELECT ... FROM eurostat WHERE ...")


Note on steps 8 and 9. In order to run SQL queries from within the Spark command line, we must first define the dataset we are working with. Here we define as a Parquet for better performance:



pyspark

df_merged = spark.read.parquet("eurostat_clean.parquet")

df_merged.createOrReplaceTempView("eurostat")
