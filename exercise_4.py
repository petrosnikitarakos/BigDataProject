import pyspark.sql
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import hashlib
from hashlib import md5
from hashlib import sha256
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3gb") \
    .appName("Exercise4") \
    .getOrCreate()


sellers = spark.read.parquet("file:///home/savas//data/coursework/sellers_parquet/*.parquet")
products = spark.read.parquet("file:///home/savas//data/coursework/products_parquet/*.parquet")
sales = spark.read.parquet("file:///home/savas//data/coursework/sales_parquet/*.parquet")

sellers.createOrReplaceTempView("sellers")
products.createOrReplaceTempView("products")
sales.createOrReplaceTempView("sales")


# Create a new column called "hashed_bill" defined as follows:
# - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. 
#   E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
# - if the order_id is odd: apply SHA256 hashing to the bill text
# Finally, check if there are any duplicate on the new column

df = spark.sql("SELECT order_id,bill_raw_text FROM sales")
order_id = df.select(df["order_id"])
bill_raw_text = df.select(df["bill_raw_text"])


def transformation(order_id,bill_raw_text):
    if int(order_id) % 2 == 0:
        #print("even") 
        number_of_A = bill_raw_text.count("A")
        #print(number_of_A)
        if number_of_A == 0:
            return bill_raw_text
        else:
            for i in range(number_of_A):
                #print(i)
                if i==0:
                    result = hashlib.md5(bill_raw_text.encode("utf-8"))
                    #print(result.hexdigest())
                else:
                    result = hashlib.md5(result.hexdigest().encode("utf-8"))    
                    #print(result.hexdigest())
            return result.hexdigest()                   
    else:
        #print("odd")
        result = hashlib.sha256(bill_raw_text.encode("utf-8"))
        return result.hexdigest()

transformationUDF = udf(lambda x,y:transformation(x,y),StringType())   
df.withColumn('hashed_bill', transformationUDF('order_id','bill_raw_text')).show()