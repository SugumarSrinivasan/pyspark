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

# view content from spark managed table
spark.sql("select * from itv011877_retail.groceries limit 10").show()

# view content from spark external table
spark.sql("select * from itv011877_retail.groceries_ext limit 10").show()

# stop spark session
spark.stop()
