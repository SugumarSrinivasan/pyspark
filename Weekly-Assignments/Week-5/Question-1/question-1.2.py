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

# switch into database
spark.sql("use itv011877_retail")

# create spark external table
spark.sql("create table itv011877_retail.groceries_ext (order_id string, location string, item string, order_date string, quantity integer) using csv options(header='true') location '/public/trendytech/groceries.csv'")

# list the table
spark.sql("show tables").show()

# describe the spark external table
spark.sql("describe formatted itv011877_retail.groceries_ext").show(truncate = False)

# view content
spark.sql("select * from itv011877_retail.groceries_ext limit 5").show()

# stop spark session
spark.stop()