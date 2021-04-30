import pyspark.sql
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Warmup2") \
    .getOrCreate()


sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")

# How many distinct products have been sold in each day?
distinct_products = spark.sql("SELECT COUNT(DISTINCT(product_id)) AS distinct_products, date FROM sales GROUP BY date ORDER BY date")
distinct_products.show()