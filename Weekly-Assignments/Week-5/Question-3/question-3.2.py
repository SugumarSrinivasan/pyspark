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

# 3.2. Find the top 5 most common last names among the customers.

customers_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/customers/*")

column_renamed_df = customers_df.withColumnRenamed("_c0", "cust_id").withColumnRenamed("_c1", "cust_fname") \
.withColumnRenamed("_c2", "cust_lname").withColumnRenamed("_c3", "cust_email").withColumnRenamed("_c4", "cust_password") \
.withColumnRenamed("_c5", "cust_street").withColumnRenamed("_c6", "cust_city").withColumnRenamed("_c7", "cust_state") \
.withColumnRenamed("_c8", "cust_zipcode")

results = column_renamed_df.groupBy("cust_lname").count().orderBy("count", ascending = False).limit(5)
results.show()

column_renamed_df.createOrReplaceTempView("customers")
results = spark.sql("select cust_lname, count(*) as count from customers group by cust_lname order by count desc limit 5")
results.show()

spark.stop()