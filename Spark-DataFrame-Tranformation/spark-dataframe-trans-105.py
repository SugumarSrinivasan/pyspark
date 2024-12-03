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

order_list = [
(1,'2023-07-25 00:00:00.0',11599,'CLOSED')
(2,'2023-07-25 00:00:00.0',256,'PENDING_PAYMENT')
(3,'2023-07-25 00:00:00.0',12111,'COMPLETE')
]

# updating the column names using toDF()
orders_df = spark.createDataFrame(order_list).toDF("order_id","order_date","customer_id","order_status")
orders_df.printSchema()
orders_df.show()

# create a dataframe with the schema in a single step
order_schema = ['order_id', 'order_date', 'customer_id', 'order_status']
new_df = spark.createDataFrame(order_list, order_schema)
new_df.show()

# create a dataframe with schema & datatype in a single step
order_schema = 'order_id long, order_status string, customer_id interger, order_status string'
new_df = spark.createDataFrame(order_list,order_schema)
new_df.show()
