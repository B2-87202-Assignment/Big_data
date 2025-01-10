#5. Movie recommendation using Spark dataframes

from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("q2")\
        .config("spark.sql.shuffle.partitions","2")\
        .getOrCreate()


moviepath= "/home/sunbeam/Cdac/BigDataAssignments/data/movies/movies.csv"
moviechm = "movieid INT ,title STRING"


ratingpath = "/home/sunbeam/Cdac/BigDataAssignments/data/movies/ratings.csv"
ratingschm = "userid INT ,movieid INT ,rating DOUBLE ,timestamp BIGINT"

movie = spark.read\
     .option("header", True)\
     .schema(moviechm)\
     .csv(moviepath)

rating = spark.read\
     .option("header", True)\
     .schema(ratingschm)\
     .csv(ratingpath)


rating_join = rating.alias('r1').join(rating.alias('r2'),col("r1.userid") == col("r2.userid"),"inner")\
              .select(col("r1.userid").alias("userid"),col("r1.movieid").alias("movie1"),col("r1.rating").alias("rating1"),col("r2.movieid").alias("movie2"),col("r2.rating").alias("rating2")).where("rating1 < rating2")

movieinp = input("Enter movieid")

rating_join=rating_join.groupBy("movie1","movie2").corr("rating1","rating2")

rating_join.show()
# rec_rating = rating_join.select('movie1',"movie2").where(f"movie1 = '{movieinp}'").orderBy()

# rec_rating.show()

# movie.show()
# rating.show()



spark.stop()