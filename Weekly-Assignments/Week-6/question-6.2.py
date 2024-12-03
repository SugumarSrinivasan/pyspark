from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
        builder. \
        config("spark.ui.port", '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        getOrCreate()

from pyspark.sql.types import *

library_schema = StructType([
    StructField("library_name", StringType()),
    StructField("location", StringType()),
    StructField("books",ArrayType(StructType([
                                        StructField("book_id", StringType()),
                                        StructField("book_name",StringType()),
                                        StructField("author",StringType()),
                                        StructField("copies_available",IntegerType())
                                        ])
    )),
    StructField("members",ArrayType(StructType([
                                        StructField("member_id",StringType()),
                                        StructField("member_name",StringType()),
                                        StructField("age",IntegerType()),
                                        StructField("books_borrowed",StringType())
        ])                      
    ))                                       
])

library_df = spark.read.schema(library_schema).json("/public/trendytech/datasets/library_data.json")

library_df.printSchema()

library_df.show()

spark.stop()