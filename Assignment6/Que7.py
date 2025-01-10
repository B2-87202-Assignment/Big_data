from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("ques7")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()


ratingpath ="/home/sunbeam/Cdac/BigDataAssignments/data/movies/ratings.csv"
rtschema ="userid INT ,movieid INT ,rating DOUBLE ,timestamp BIGINT"

moviepath= "/home/sunbeam/Cdac/BigDataAssignments/data/movies/movies.csv"
moviechm = "movieid INT ,title STRING"

ratings = spark.read\
          .option("header",True)\
          .schema(rtschema)\
          .csv(ratingpath)

movies = spark.read\
         .option("header",True)\
         .schema(moviechm)\
         .csv(moviepath)



rtjoin = ratings.alias("rt1").join(ratings.alias("rt2"),col("rt1.userid")==col("rt2.userid"),"inner")\
         .filter(col("rt1.movieid") < col("rt2.movieid"))\
         .select(col("rt1.userid").alias("userid"),col("rt1.movieid").alias("movie1"),col("rt1.rating").alias("rating"),col("rt2.movieid").alias("movie2"),
                 col("rt2.rating").alias("rating2"))

rtjoin.show()


spark.stop()