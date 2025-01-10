from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName("Que3")\
            .config("spark.sql.shuffle.partitions", "2")\
            .getOrCreate()

empfile = "/home/tejas/Desktop/BigData/data/emp.csv"
empshcema = "empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT"

deptfile = "/home/tejas/Desktop/BigData/data/dept.csv"
deptschema = "deptno INT, deptname STRING, location STRING"

emp_df = spark.read\
            .csv(empfile, empshcema)
dept_df = spark.read\
            .csv(deptfile, deptschema)

# emp_df.show()
# dept_df.show()

result = emp_df\
            .join(dept_df, "deptno", "right")\
            .groupby("deptname")\
            .agg(ifnull(sum("sal"), lit(0)).alias("max_sal"))

result.show()

# +----------+-------+
# |  deptname|max_sal|
# +----------+-------+
# |ACCOUNTING| 8750.0|
# |  RESEARCH|10875.0|
# |     SALES| 9400.0|
# |OPERATIONS|    0.0|
# +----------+-------+
