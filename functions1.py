from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\bigdata1\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",",").load(data)
#ndf=df.groupBy(df.state).agg(count("*").alias("cnt"), collect_list(df.first_name).alias("names")).orderBy(col("cnt").desc())
#ndf=df.groupBy(df.state).agg(count(col("city")).alias("cnt"), collect_set(df.city).alias("names")).orderBy(col("cnt").desc())
#ndf=df.withColumn("state",when(df.state=="NY","NewYork").when(df.state=="CA","Cali").otherwise(df.state))
#ndf=df.withColumn("address1",when(col("address").contains("#"),"*****").otherwise(col("address"))).withColumn("address2",regexp_replace(col("address"),"#","-"))
#ndf1=df.withColumn("emails",substring_index(col("email"),"@",-1)).withColumn("username", substring_index(col("email"),"@",1))
#ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())

#processing sql friendly
df.createOrReplaceTempView("tab")
#ndf=spark.sql("select *, concat_ws(' ', first_name, last_name) full_name, substring_index(email,'@',1) mail from tab")

#using CTE (with)
qry="""with temp as (select *, concat_ws(' ', first_name, last_name) full_name, substring_index(email,'@',-1) mail from tab)
select mail, count(*) cnt from temp group by mail order by cnt desc"""

ndf=spark.sql(qry)
ndf.printSchema()
ndf.show(truncate=False)