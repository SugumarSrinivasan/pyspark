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

# drop the spark managed table and verify both metadata and data is gone
spark.sql("drop table itv011877_retail.groceries")

# checking the metadata existance
spark.sql("describe formatted itv011877_retail.groceries").show(truncate = False)

# hadoop fs -ls /user/itv011877/warehouse/itv011877_retail/groceries (output shouldn't give any o/p)

# drop the spark external table and verify metadata is gone
spark.sql("drop table itv011877_retail.groceries_ext")

# checking the metadata existance
spark.sql("describe formatted itv011877_retail.groceries_ext").show(truncate = False)

# hadoop fs -ls /public/trendytech/groceries.csv (file must be present)

spark.stop()