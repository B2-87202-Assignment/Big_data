#3. Load Fire Service Calls Dataset into Spark Managed Table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .config("spark.sql.shuffle.partitions","2")\
        .config("spark.sql.warehouse.dir","file:////home/sunbeam/bigdata/spark-warehouse")\
        .enableHiveSupport()\
        .getOrCreate()


df = spark.read\
     .option("inferSchema","True")\
     .option("header","True")\
     .csv("/home/sunbeam/Cdac/BigDataAssignments/data/Fire_Service_Calls_Sample.csv")


df.write.mode('overwrite').partitionBy("Call Type").saveAsTable("fore_service_call")

spark.stop()