import pyspark.sql
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise2") \
    .getOrCreate()


sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")

# For each seller, what is the average % contribution of an order to the seller's daily quota?
# Example:
# If Seller_0 with `quota=250` has 3 orders:
# Order 1: 10 products sold
# Order 2: 8 products sold
# Order 3: 7 products sold
# The average % contribution of orders to the seller's quota would be:
# Order 1: 10/250 = 0.04
# Order 2: 8/250 = 0.032
# Order 3: 7/250 = 0.028
# Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333

avg_contribution = spark.sql("SELECT CAST((SUM(a.num_pieces_sold/b.daily_target))/count(distinct(order_id)) as Decimal(10,9)) as avg_contribution , a.seller_id FROM sales AS a INNER JOIN sellers AS b ON a.seller_id = b.seller_id GROUP BY a.seller_id")
avg_contribution.show()