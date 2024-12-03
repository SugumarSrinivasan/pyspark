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

customer_list = [
	(1,("sumit","mittal"),"bengalore"),
	(1,("ram","kumar"),"hyderabad"),
	(1,("vijay","shankar"),"pune")]

# create dataframe with schema ddl type
customer_schema = 'customer_id long, fullname struct<firstname:string,lastname:string>,city string'
customer_df = spark.createDataFrame(customer_list,customer_schema)
customer_df.printSchema()

customer_df.show()

spark.stop()