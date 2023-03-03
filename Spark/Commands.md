<!-- Load data from CSV-->
var df = spark.read.format("csv").load("s3://airline-delay/DelayedFlights-updated.csv")
df.show()

<!-- Partition the data by Year -->
df.write.format("parquet").partitionBy("_c1").save("s3://airline-delay/DelayedFlights-updated-df.csv")
df.show()

<!-- View partition data -->
val df2 = spark.read.format("parquet").load("s3://airline-delay/DelayedFlights-updated-df.csv")
df2.show()

<!-- Create table called `delay_flights` -->
df2.createOrReplaceTempView("delay_flights")

<!-- This is the command in which you can run the python script directly in the Hadoop terminal. Here Year_wise_carrier_delay.py contains all the steps I mentioned above and it's a short way to run the query and find the query execution time as well. -->
spark-submit --master yarn Year_wise_carrier_delay.py