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

# schema definition for nested data using schema ddl type
customer_schema = "customer_id long, fullname struct<firstname:string,lastname:string>,city string"

customer_df = spark.read \
.format("json") \
.schema(customer_schema) \
.load("/public/trendytech/datasets/customer_nested/*")

customer_df.printSchema()

customer_df.show()

spark.stop()