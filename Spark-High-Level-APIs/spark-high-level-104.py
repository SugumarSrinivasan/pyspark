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

# creating the managed table
spark.sql("create table itv011877_retail.orders (order_id integer, order_date string, customer_id integer, order_status string) using csv")

# creating the external table
spark.sql("create table itv011877_retail.orders_ext (order_id integer, order_date string, customer_id integer, order_status string) using csv location '/public/trendytech/retail_db/orders'")

# print the first 5 records
spark.sql("select * from itv011877_retail.orders_ext limit 5").show()

# describe the external table
spark.sql("describe extended itv011877_retail.orders_ext").show(truncate = False)

# truncate the data from external table
spark.sql("truncate table itv011877_retail.orders_ext")

# insert a record to the external table
spark.sql("insert into itv011877_retail.orders_ext values (1111,'12-02-2023', 2222,'CLOSED')")

