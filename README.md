# UoM_MapReduce-vs-Spark
In23-S1-CS5229 - Big Data Analytics Technologies

# Spark => Run the queries using python script

1. Year_wise_carrier_delay.py
2. Year_wise_late_aircraft_delay.py
3. Year_wise_NAS_delay.py
4. Year_wise_security_delay.py
5. Year_wise_Weather_delay.py

# Step 1:

First, run the Year_wise_carrier_delay.py using the below command

spark-submit --master yarn Year_wise_carrier_delay.py

This is the command in which you can run the python script directly in the Hadoop terminal. Here Year_wise_carrier_delay.py contains all the steps I mentioned in running over the spark shell and it's a short way to run the query and find the query execution time as well.

# Step 2:

Run other scripts as you want and see the execution logs to view the query execution time
