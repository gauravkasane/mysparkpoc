from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

host="jdbc:mysql://mysqlbda.cu6pv8vnnheo.us-east-1.rds.amazonaws.com:3306/mysqldba?useSSL=false"
data="C:\\bigdata1\\drivers\\IBM.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)

df.printSchema()
df.show()

df.write.mode("overwrite").format("jdbc").option("url",host).option("user", "gaurav").option("password", "gaurav97")\
    .option("dbtable", "IBM").option("driver","com.mysql.jdbc.Driver").save()

#spark-submit --master local --deploy-mode client pract1.py
