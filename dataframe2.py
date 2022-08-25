from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\bigdata1\\drivers\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)
#by default spark every field consider as string, but i want to change columns appropriate datatype like int, double, string, use inforschema, true option
#if u not mention like this 1000+1000 ... if int .. 2000 if string, u ll get 10001000
#df.show()
#df.printSchema()

##data processing programing friendly
#res=df.where(col("age")>90)
#for multiple condition
#res=df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) & (col("marital")!="married"))
#res = df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
#res = df.groupBy(col("marital")).count()
#res = df.groupBy(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb")).orderBy(col("smb").desc())

##process sql friendly
df.createOrReplaceTempView("tab")
##createOrReplaceTempView .. register this dataframe as a table. its very useful to run sql queries.
res=spark.sql("select marital, sum(balance) sumbal from tab group by marital")
res.show()
#res.printSchema()