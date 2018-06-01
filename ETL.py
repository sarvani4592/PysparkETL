# -*- coding: utf-8 -*-
"""
Created on Thu May 31 11:51:42 2018

@author: Sarvani.Ippagunta
"""

# -*- coding: utf-8 -*-
"""
Created on Thu May 24 17:31:10 2018

@author: Sarvani.Ippagunta
"""

#help('modules')
import pyspark
from pyspark import SparkConf,SparkContext,SQLContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import time
import os
import urllib

#Declare Variables
NumOfPartitions=10
Directory="/usr/sreecharan/sampleData/ETL/"

#Download files from S3
#urllib.urlretrieve ("https://s3.amazonaws.com/tmp1.sl.com/20170701_20170701165514569.gz",Directory+"/20170701_20170701165514569.gz")
#urllib.urlretrieve ("https://s3.amazonaws.com/tmp1.sl.com/20170701_20170702004210139.gz",Directory+"/20170701_20170702004210139.gz")
#files_path = [os.path.abspath(x) for x in os.listdir(Directory)]

#Create Sparkcontext, Sqlcontext and configure
conf = SparkConf().setAppName("ETL Using Pypark").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext=SQLContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions",str(NumOfPartitions))

#Read the files
data=sc.textFile("file:///"+Directory,NumOfPartitions)
structuredData=data.map(lambda r: r.split(",")).map(lambda r: \
                       (r[1],r[2],\
                        round(float(r[6]),3),round(float(r[7]),3),\
                        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(r[5])))))
#structuredData.take(10)

columnarData=sqlContext.createDataFrame(structuredData,['ad_id', 'id_type', 'lat', 'long', 'timestamp'])
distinctRecords=columnarData.groupBy("ad_id","id_type","lat","long").agg(F.max("timestamp"))
distinctRecords.cache()
distinctRecords.toJSON().saveAsTextFile("file:///usr/sarvani/sampleData/sarvaniippagunta-scanbuy-20180530")

print("Unique Ad_id count: "+ str(distinctRecords.select("ad_id").distinct().count()))#124246
print("Total Record count: "+ str(distinctRecords.count()))#300498
    

sc.stop()