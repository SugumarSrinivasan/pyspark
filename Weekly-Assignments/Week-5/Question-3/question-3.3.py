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

# 3.3. Check whether there are any customers whose zip codes are not valid (i.e., not equal to 5 digits).

customers_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/customers/*")

column_renamed_df = customers_df.withColumnRenamed("_c0", "cust_id").withColumnRenamed("_c1", "cust_fname") \
.withColumnRenamed("_c2", "cust_lname").withColumnRenamed("_c3", "cust_email").withColumnRenamed("_c4", "cust_password") \
.withColumnRenamed("_c5", "cust_street").withColumnRenamed("_c6", "cust_city").withColumnRenamed("_c7", "cust_state") \
.withColumnRenamed("_c8", "cust_zipcode")

from pyspark.sql.functions import length
invalid_zips = column_renamed_df.filter(length("cust_zipcode")!=5)

if invalid_zips.count() == 0:
    print("All the zipcodes are valid")
else:
    print("There are customers with invalid zipcode:")
    invalid_zips.show()

column_renamed_df.createOrReplaceTempView("customers")
results = spark.sql("select * from customers where length(cust_zipcode)!=5")
results.show()

spark.stop( )