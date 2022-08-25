from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
#sc = spark.sparkContext

#data="C:\\bigdata1\\drivers\\donations.csv"
#df=spark.read.format("csv").option("header","true").load(data)
#if you mention header true, first line of data consider as column
#df.show()

#when 1st line is not header
#skip first line, second line onword orignal data is available
data="C:\\bigdata1\\drivers\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata, header=True, inferSchema=True)
df.printSchema()
#printScemea is printing column and its datatype in nice tree format
df.show(5)
#if u want to display top 5 lines