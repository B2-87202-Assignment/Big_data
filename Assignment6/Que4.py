from click import option
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Que4")\
            .config("spark.sql.shuffle.partitions", "2")\
            .getOrCreate()

rating_file = "/home/tejas/Desktop/BigData/data/movies/ratings.csv"
rating_schema = "userid INT, movieid INT, rating DOUBLE, timestamp STRING"

df = spark.read\
            .option("header", True)\
            .csv(rating_file, rating_schema)

df2 = df.select(
    "userid",
    "movieid",
    "rating",
    from_unixtime(col("timestamp")).alias("date")
)

result = df2\
            .groupby(year("date").alias("Year"))\
            .agg(count("rating").alias("Ratings"))

result.show()

# +----+-------+
# |Year|Ratings|
# +----+-------+
# |2009|   3432|
# |1996|   6239|
# |1999|   5901|
# |2004|   4658|
# |2003|   4463|
# |2016|   6225|
# |2010|   2520|
# |1998|   1825|
# |2011|   4449|
# |2000|  13869|
# |2006|   7493|
# |2005|   7161|
# |2014|   2224|
# |2012|   3850|
# |2001|   4658|
# |2002|   3937|
# |2007|   1548|
# |2013|   1969|
# |2015|   6610|
# |2008|   3676|
# +----+-------+
# only showing top 20 rows
