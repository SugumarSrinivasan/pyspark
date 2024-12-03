# importing the required libraries
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

# creating the spark session
spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()

products_df = spark.read \
.format("csv") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/products/part-00000")

df1 = products_df.toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")
df1.show()
df1.printSchema()

# increase the product_price by 20%:
from pyspark.sql.functions import expr
df2 = df1.withColumn("product_price", expr("product_price * 1.2"))
df2.show()
df2.printSchema()

"""increase the product_price based on the brand name:
Nike - increase 20%
Armour - increase 10%
Others - 0% No change in price"""

df2 = df1.withColumn("product_price",expr("CASE WHEN product_name LIKE '%Nike%' THEN product_price * 1.2 WHEN product_name LIKE '%Armour%' THEN product_price * 1.1 ELSE product_price END"))
df2.show()
df2.printSchema()

spark.stop()