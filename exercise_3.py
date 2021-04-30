import pyspark.sql
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3gb") \
    .appName("Exercise3") \
    .getOrCreate()

sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")

# Who are the second most selling and the least selling persons (sellers) for each product?
most_selling_seller = spark.sql("SELECT CAST(SUM(num_pieces_sold) as Decimal) AS total_sales, product_id , seller_id FROM sales GROUP BY product_id, seller_id ORDER BY total_sales DESC LIMIT 2") 
most_selling_seller = most_selling_seller.withColumn("id", monotonically_increasing_id())
second_most_selling_seller= most_selling_seller.filter(F.col('id')==1)

least_selling_seller = spark.sql("SELECT CAST(SUM(num_pieces_sold) as Decimal) AS total_sales, product_id , seller_id FROM sales GROUP BY product_id, seller_id ORDER BY total_sales ASC LIMIT 1")
# Who are those for product with `product_id = 0`
product_0_top_sellers = spark.sql("SELECT CAST(SUM(num_pieces_sold) as Decimal) AS total_sales,seller_id FROM sales WHERE product_id = '0' GROUP BY seller_id ORDER BY total_sales") 


second_most_selling_seller.show()
least_selling_seller.show()
product_0_top_sellers.show()

