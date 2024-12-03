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

# converting rdd to dataframe
orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/part-00000")
new_orders_rdd = orders_rdd.map(lambda x: (int(x.split(",")[0]),x.split(",")[1],int(x.split(",")[2]),x.split(",")[3]))
new_orders_rdd.take(5)

order_schema = 'order_id long, order_date string, customer_id integer, order_status string'
orders_df = spark.createDataFrame(new_orders_rdd, order_schema)
orders_df.show()
orders_df.printSchema()

# using toDF() we can incorporate the column name
orders_df = spark.createDataFrame(new_orders_rdd).toDF("order_id","order_date","customer_id","order_status")
orders_df.show()
orders_df.printSchema()

# using toDF()
order_schema = 'order_id long, order_date string, customer_id integer, order_status string'
orders_df = new_orders_rdd.toDF(order_schema)
orders_df.show()
orders_df.printSchema()

