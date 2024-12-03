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

# load the data from hdfs to dataframe using standard reader API
orders_df = spark.read \
.format("json") \
.load("/public/trendytech/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json")

# switch to database
spark.sql("use itv011877_retail")

# create a temprorary view from the dataframe
orders_df.createOrReplaceTempView("orders")

# create a managed spark table
spark.sql("create table itv011877_retail.orders (customer_id long, order_date string, order_id long, order_status string) using json")

# load the data into managed table from temprorary view 
spark.sql("insert into itv011877_retail.orders select * from orders")

# create spark external table
spark.sql("create table itv011877_retail.orders_ext (customer_id long, order_date string, order_id long, order_status string) using json location '/public/trendytech/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json'")

# list the table
spark.sql("show tables").show()

# describe the spark external table
spark.sql("describe formatted itv011877_retail.orders").show(truncate = False)

# describe the spark external table
spark.sql("describe formatted itv011877_retail.orders_ext").show(truncate = False)

# view the data by executing the select statement
spark.sql("select * from itv011877_retail.orders limit 5").show()

# view content
spark.sql("select * from itv011877_retail.orders_ext limit 5").show()

# drop the spark managed table and verify both metadata and data is gone
spark.sql("drop table itv011877_retail.orders")

# checking the metadata existance
spark.sql("describe formatted itv011877_retail.orders").show(truncate = False)

# hadoop fs -ls /user/itv011877/warehouse/itv011877_retail/orders (output shouldn't give any o/p)

# drop the spark external table and verify metadata is gone
spark.sql("drop table itv011877_retail.orders_ext")

# checking the metadata existance
spark.sql("describe formatted itv011877_retail.orders_ext").show(truncate = False)

# hadoop fs -ls /public/trendytech/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json (file must be present)

spark.stop()