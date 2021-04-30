import pyspark.sql
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()


sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")

#What is the average revenue of the orders?
revenue_of_order = spark.sql("SELECT AVG(a.num_pieces_sold*b.price) AS revenue, a.order_id FROM sales AS a INNER JOIN products AS b ON a.product_id = b.product_id GROUP BY a.order_id")
revenue_of_order.show()