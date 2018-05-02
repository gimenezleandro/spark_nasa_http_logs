#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql.functions import split, regexp_extract
from pyspark.sql.functions import col, sum, udf
from pyspark.sql import SparkSession
from pyspark import SparkContext


def strToDate(dt_str):
  dic_month = {
  'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4,  'May':5,  'Jun':6, 
  'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12
  }
  

  return "{0:02d}-{1:02d}-{2:04d}".format(
                                    int(dt_str[7:11]), 
                                    dic_month[dt_str[3:6]], 
                                    int(dt_str[0:2]), 
  )  

def loadDataset (spark, path):
    log_file_path = path+'*'
    base_df = spark.read.text(log_file_path) 
  
    tab_df = base_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'), 
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('url'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('code'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('bytes'))    
 
    u_strToDate = udf(strToDate)

    logs_df = tab_df.select('*', u_strToDate(tab_df['timestamp']).cast('date').alias('date'))
    
    
    return logs_df

if __name__ == '__main__':  
  if len(sys.argv) != 2:
    print("Indique o caminho da pasta onde o arquivo está localizado.")
    exit(-1)
    
  sc = SparkContext()
  sc.setLogLevel('ERROR')

  spark = SparkSession.builder.appName("Spark Logs Nasa").getOrCreate()    
   
  logs_df = loadDataset(spark, sys.argv[1])

  print("")
  print("#########################################")
  print("##########   ANALISE DE LOGS   ##########")
  print("")
  #1. Número de hosts únicos.

  unique_host = logs_df.select('host').distinct().count()
  print("1. Total de hosts unicos: "+str(unique_host))
  print("")
  print("")

  #2. O total de erros 404.

  cod404_df = logs_df.filter('code=404')
  tt_404 = cod404_df.count()
  print("2. Total de erros 404: "+str(tt_404))

  print("")
  print("")

  #3. Os 5 URLs que mais causaram erro 404.

  top404_df = cod404_df.groupBy('url').count().orderBy(col('count').desc())
  top5_404 = top404_df.select(col('url'), col('count').alias('tt')).take(5)

  print("3. Os 5 URLs que mais causaram erro 404")
  print("")
  cod404_df.groupBy('url').count().orderBy(col('count').desc()).show(5, truncate=False)

  print("")

  #4. Quantidade de erros 404 por dia.

  day = cod404_df.select('date').distinct().count()
  mean_error = round(float(tt_404) / float(day),2)

  print("4. a) Quantidade media de erros por dia: "+ str(mean_error))
  print("")
  print("4. b) Quantidade de erros por data:")
  print("")
  days = cod404_df.groupBy('date').count().orderBy(col('date').asc())
  days.show(days.count(), truncate=False)

  print("")

  #5. O total de bytes retornados.

  tt_bytes = logs_df.select(sum(logs_df.bytes)).collect()[0] 
  print("5. Total de bytes retornados: "+ str(tt_bytes[0]))

  print("")
  print("")
 
  spark.stop()


