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

orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_mapped = orders_base.map(lambda x: (x.split(",")[2], x.split(",")[3]))


customers_base = spark.sparkContext.textFile("/public/trendytech/retail_db/customers/part-00000")
customers_mapped = customers_base.map(lambda x: (x.split(",")[0], x.split(",")[8]))

customers_broadcast = spark.sparkContext.broadcast(customers_mapped.collect())

def get_pincode(customer_id):
    try:
        return customers_broadcast.value[customer_id]
    except:
        return "-1"

joined_rdd = orders_mapped.map(lambda x: (get_pincode(int(x[0])),x[1]))
joined_results = joined_rdd.saveAsTextFile("data/broadcastresults")

spark.stop()