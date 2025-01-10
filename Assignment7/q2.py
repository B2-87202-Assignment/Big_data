#2. Read ncdc data from mysql table and print average temperature per year in DESC order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("q2")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()



df = spark.read\
     .option("driver","com.mysql.cj.jdbc.Driver")\
     .option("user","root")\
     .option("password","manager")\
     .jdbc("jdbc:mysql://localhost:3306/dbda_db","ncdc_tb")

df.groupBy("year").agg(round(avg("temp"),2).alias("avgtemp")).show()


spark.stop()
