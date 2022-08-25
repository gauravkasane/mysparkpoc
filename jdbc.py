from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

uname="gaurav"
password="gaurav97"

host="jdbc:mysql://mysqlbda.cu6pv8vnnheo.us-east-1.rds.amazonaws.com:3306/mysqldba?useSSL=false"
df = spark.read.format("jdbc").option("url",host).option("user", uname).option("password", password)\
    .option("dbtable", "(select * from emp where sal>2000) t").option("driver","com.mysql.jdbc.Driver").load()
#process data
res=df.na.fill(0,['comm']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate", date_format(col("hiredate"),"yyyy/MMM/dd"))
res.write.mode("overwrite").format("jdbc").option("url",host).option("user", uname).option("password", password)\
    .option("dbtable", "empclean").option("driver","com.mysql.jdbc.Driver").save()

#alternative approch (read based on query)
#df = spark.read.format("jdbc").option("url",host).option("user", uname).option("password", password)\
    #.option("query", "select e.*, d.loc from emp e join dept d on e.deptno=d.deptno").option("driver","com.mysql.jdbc.Driver").load()

# export data to mysql
#data="C:\\bigdata1\\drivers\\donations.csv"
#df=spark.read.format('csv').options(header='true', inferSchema='true').load(data)
res.show()
res.printSchema()
#like sqoop no need to create any table in advance mysql.spark automatic create table in mysql
#df.write.format("jdbc").option("url",host).option("user", "gaurav").option("password", "gaurav97")\
    #.option("dbtable", "donate").option("driver","com.mysql.jdbc.Driver").save()