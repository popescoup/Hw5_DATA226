from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("HandleLogFiles").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# Load all .gz files in the directory into a DataFrame
df = spark.read.text("*.gz")

# Check the number of partitions
print(df.rdd.getNumPartitions())
df.show(truncate=False)

# Create a parsed dataframe (log_df)
# Extract the necessary information from log data using regular expressions
pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'
log_df = df.select(
F.regexp_extract("value", pattern, 1).alias("ip"),
F.regexp_extract("value", pattern, 2).alias("timestamp"),
F.regexp_extract("value", pattern, 3).alias("method"),
F.regexp_extract("value", pattern, 4).alias("url"),
F.regexp_extract("value", pattern, 5).alias("status").cast("integer"),
F.regexp_extract("value", pattern, 6).alias("size").cast("integer")
)
log_df.show()


# Step 4: (status, method) counts using DataFrame operations
status_method_count = (
    log_df
    .filter(log_df.method != "DELETE")
    .groupBy("status", "method")      
    .count()            
    .orderBy(F.asc("count"))
)
status_method_count.show()


# Step 5: (status, method) counts using Spark SQL
log_df.createOrReplaceTempView("logs")

status_method_count_sql = spark.sql("""
SELECT status, method, COUNT(1) AS count
FROM logs
WHERE method != 'DELETE'
GROUP BY status, method
ORDER BY count ASC
""")
status_method_count_sql.show()