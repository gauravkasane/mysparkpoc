from pyspark.sql import *
from pyspark.sql.functions import *
from configparser import ConfigParser
conf=ConfigParser()

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

#to use credential directly from file
conf.read(r"C:\\bigdata1\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","passw")
data=conf.get("input","data")
qry="(select table_name from information_schema.tables where TABLE_SCHEMA='mysqldba') aaa"

#its for local csv data to mysql
#df=spark.read.format('csv').options(header='true', inferSchema='true').load(data)

df1=spark.read.format("jdbc").option("url",host).option("user", user).option("password", pwd)\
.option("dbtable", qry).option("driver","com.mysql.jdbc.Driver").load()

tabs=[x[0] for x in df1.collect()]
for i in tabs:
    df=spark.read.format("jdbc").option("url",host).option("user", user).option("password", pwd)\
        .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
    df.show
