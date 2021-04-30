import pyspark.sql
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Warmup1") \
    .getOrCreate()


sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")

# Find out how many orders, how many products and how many sellers are in the data.
total_orders = spark.sql("SELECT COUNT(DISTINCT(order_id)) AS total_orders FROM sales")
total_products = spark.sql("SELECT COUNT(DISTINCT(product_id)) AS total_products FROM products")
total_sellers = spark.sql("SELECT COUNT(DISTINCT(seller_id)) AS total_sellers FROM sellers")


# How many products have been sold at least once? Which is the product contained in more orders?
products_sold_once = spark.sql("SELECT COUNT(DISTINCT(product_id)) AS products_sold_once FROM sales")
product_max_ordered = spark.sql("SELECT COUNT(order_id) AS total_orders_of_product, product_id FROM sales GROUP BY product_id ORDER BY COUNT(order_id) DESC")


total_orders.show()
total_products.show()
total_sellers.show()
products_sold_once.show()
product_max_ordered.show(1)



