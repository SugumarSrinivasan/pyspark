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

order_schema = 'order_id long, order_date string, customer_id integer, order_status string'
orders_df = spark.createDataFrame(order_list, order_schema)

# converting string to timestamp
from pyspark.sql.functions import to_timestamp
orders_df.withColumn("order_date", to_timestamp('order_date'))

orders_df.show()
orders_df.printShema()

