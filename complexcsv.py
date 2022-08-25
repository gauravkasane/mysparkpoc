from pyspark.sql import *
from pyspark.sql.functions import *

#creating sparksession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()

data="C:\\bigdata1\\drivers\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)
import re
#re = replace.. except all small characters and number ecxept all special characters (Regular expression)
num = int(df.count())
cols = [re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
#toDf is used to rename all the columns, and convert rdd to new dataframe
ndf = df.toDF(*cols)
ndf.show(21,truncate=True)
ndf.printSchema()
#dataframe column names and its datatype display properly by df.printSchema()

# data processing progamming fiendly (dataframe api)
res=ndf.groupBy(col("gender")).agg(count(col("*")).alias("cnt"))
res.show()
