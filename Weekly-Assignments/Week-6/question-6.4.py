from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.ui.port", '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()

# 1. Read the dataset using the "permissive" mode and count the number of records read
sales_data_df = spark.read \
.format("json") \
.load("/public/trendytech/datasets/sales_data.json")

records_read = sales_data_df.count()
print("Count of records after reading the dataset using permissive mode:"+" "+str(records_read))

# 2. Read the dataset using the "dropmalformed" mode and display the number of malformed records
sales_data_df = spark.read \
.format("json") \
.option("mode","dropMalFormed") \
.load("/public/trendytech/datasets/sales_data.json")

exclude_malformed_record_count = sales_data_df.count()
malformed_records = (records_read - exclude_malformed_record_count)

print("Count of records read:"+" "+str(records_read))
print("Total Number of Malformed Records:"+" "+str(malformed_records))

# 3. Read the dataset using the "failfast" mode

sales_data_df = spark.read \
.format("json") \
.option("mode","failfast") \
.load("/public/trendytech/datasets/sales_data.json")

spark.stop()