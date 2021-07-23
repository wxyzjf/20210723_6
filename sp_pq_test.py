from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
#from pyspark import TaskContext

#spark = SparkSession.builder.appName("Py_test").getOrCreate()
#spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

def func_part(x):
        if x <2000:
                return 1
        elif x >=2000 and x<=5000:
                return 2
        else :
                return 3

#df = spark.read.text("file:///kahome/spk_py/test_1m.csv")

     #  .repartitionAndSortWithinPartitions(3,lambda x:func_part(x),True) \
     	#.groupByKey(3,lambda k:func_part(k))\

rdd = sc.textFile("file:///kahome/spk_py/test_1m.csv",minPartitions=3,use_unicode=False) \
	.map(lambda x:(x,x)) \
	.repartitionAndSortWithinPartitions(3,lambda k:func_part(int(k)),True)  \
        .map(lambda (k,v):Row(v)) 
#print rdd.take(4)


sch=StructType([
        StructField("col1",StringType(),True)
])

df_srcFile=spark.createDataFrame(rdd,sch)
#df_srcFile.createOrReplaceTempView("tbl_df_srcFile")
#df_srcFile.count()

#spark.sql("select count(*) from tbl_df_srcFile ").show()


#df_srcFile.write.csv("file:///kahome/spk_py/res",mode="append")
#df_srcFile.write.format('parquet').mode("append").save("file:///kahome/spk_py/res",foramt='parquet',mode='append',compression='gzip')
df_srcFile.write.save("file:///kahome/spk_py/res",foramt='parquet',mode='append',compression='gzip')


#print "\n----------->"+res
df_srcFile.printSchema()














	.map(lambda x:(x,x)) \
	.repartitionAndSortWithinPartitions(3,lambda k:func_part(int(k)),True)  \
        .map(lambda (k,v):Row(v)) 
        
        
        
        
        
        
        
        
        
        
        
        
        
        hdfs:///HDS_VOL_TMP/test_1m.csv
        
        
        
        ?dataframe ?sql write result file to hdfs
        
        
[??11:53] Kevin Ou
    ??? mapr ?table 
?[??11:54] Kevin Ou
    fwcdr_OLD
?[??11:54] Kevin Ou
    ??????src ??sql ??extract ??


        
[??11:55] Kevin Ou
    ?csv ? parquet ??
?[??11:55] Kevin Ou
     format ????


        
        
[??11:56] Kevin Ou
    ??? fw_cdr_old ?? ??? ip ??count ???????????

?[??11:56] Kevin Ou
    function ????????console. ?????task ?????

        
        \\gzpc-master7\IS-Dev\GZDWTeam\MAPR\Upgrade
        
        
        
        
        maprfs:/HDS_VOL_HIVE/FWCDR
        
         /HDS_VOL_HIVE/FWCDR_OLD/start_date=20200510
        
        
        
010.069.131.045_20200505045928  2020-05-05 04:59:28     10.69.131.45    {"send_volume":"74","receive_volume":"64","service":"19471","dst":"206.51.26.192"}    20200506
010.069.008.119_20200505050027  2020-05-05 05:00:27     10.69.8.119     {"send_volume":"74","receive_volume":"64","service":"19471","dst":"206.51.26.192"}    20200506



structType json







https://spark.apache.org/docs/2.4.1/api/python/pyspark.sql.html?highlight=spark%20read%20text#pyspark.sql.DataFrameReader








































