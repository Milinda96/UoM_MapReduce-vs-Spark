import time
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Set input and output paths
# input_path = "s3://airline-delay/DelayedFlights-updated.csv"
partition_path = "s3://airline-delay/DelayedFlights-updated-df"
output_path = "s3://airline-delay/Year_wise_late_aircraft_delay"
log_file = "Year_wise_late_aircraft_delay_execution.log"

# Create SparkSession
spark = SparkSession.builder.appName("DelayedFlightsQuery").getOrCreate()

# Read input data and create DataFrame
# df1 = spark.read.format("csv").load(input_path)
# df1.write.format("parquet").partitionBy("_c1").save(partition_path)

df2 = spark.read.format("parquet").load(partition_path)
df2.createOrReplaceTempView("delay_flights")

# Run Spark query 5 times and record execution time
with open(log_file, "w") as f:
    for i in range(1, 6):
        start_time = time.time()
        result_df = spark.sql("SELECT _c1 as Year, avg((_c29/_c15)*100) as Year_wise_late_aircraft_delay from delay_flights GROUP BY _c1 ORDER BY _c1 DESC")
        result_df.write.format("csv").option("header", "true").mode("overwrite").save(output_path)
        end_time = time.time()
        execution_time = end_time - start_time
        f.write(f"Iteration {i}: {execution_time:.2f} seconds\n")
