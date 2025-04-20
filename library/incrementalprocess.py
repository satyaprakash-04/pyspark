from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('incrementalApp').master('local[*]').getOrCreate()
spark.stop()

# Create a function that fetches timestamp from a file
# create a table using duck db and insert fake data in regular basis
# Create a class that fetches incremental data using its timestamp from the table of the duck db and fetch and process
# using spark and then put it in the datalake.
