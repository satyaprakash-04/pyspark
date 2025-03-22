from pyspark.sql import SparkSession

# sc = SparkSession.builder.appName('test read csv').config(
#     "spark.driver.extraJavaOptions",
#     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
# ).getOrCreate()

sc = SparkSession.builder.appName('test read csv').getOrCreate()

df = sc.read.csv('car_price_dataset.csv', header=True, inferSchema=True)
df.head()