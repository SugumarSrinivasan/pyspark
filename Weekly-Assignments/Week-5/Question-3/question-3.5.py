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

# 3.5. Find the number of customers from each city in the state of California(CA).

customers_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/customers/*")

column_renamed_df = customers_df.withColumnRenamed("_c0", "cust_id").withColumnRenamed("_c1", "cust_fname") \
.withColumnRenamed("_c2", "cust_lname").withColumnRenamed("_c3", "cust_email").withColumnRenamed("_c4", "cust_password") \
.withColumnRenamed("_c5", "cust_street").withColumnRenamed("_c6", "cust_city").withColumnRenamed("_c7", "cust_state") \
.withColumnRenamed("_c8", "cust_zipcode")

results = column_renamed_df.filter("cust_state == 'CA'").groupBy("cust_city").count()
results.show()

column_renamed_df.createOrReplaceTempView("customers")
results = spark.sql("select cust_city, count(*) as cust_count from customers where cust_state = 'CA' group by cust_city")
results.show()

spark.stop()