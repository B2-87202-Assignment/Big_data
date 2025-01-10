#Count number of movie ratings per month using sql query (using temp views).



from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("q6")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()


ratingpath = "/home/sunbeam/Cdac/BigDataAssignments/data/movies/ratings.csv"
ratingschm = "userid INT ,movieid INT ,rating DOUBLE ,timestamp BIGINT"


ratings = spark.read\
          .option("header",True)\
          .schema(ratingschm)\
          .csv(ratingpath)

ratings = ratings.select("userid","movieid","rating",to_date(from_unixtime(col("timestamp"))).alias("dt"))

ratings.createOrReplaceTempView("raw_view")

result = spark.sql("SELECT year(dt) as yr ,count(1) FROM raw_view GROUP BY year(dt)")

result.show()



spark.stop()