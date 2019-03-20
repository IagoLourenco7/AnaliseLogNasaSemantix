# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 10:22:23 2019

@author: idomingos
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
spark = SparkSession.builder.getOrCreate()

#leitura do arquivo de agosto
agosto=spark.read.text("access_log_Aug95.log")

#tratamento e populando dataframe
agosto_limpo= agosto.select(regexp_extract('value',r'^([^\s]+\s)',1).alias('hosts'),
                            regexp_extract('value', r'((\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}))', 1).alias('data'),
                            regexp_extract('value', r'^.*"\w+\s+([^\s+]+)\s+HTTP.*', 1).alias('URL'),
                            regexp_extract('value', r'^.*\s+([^\s]+)\s', 1).cast('integer').alias('codigo_HTTP'),
                            regexp_extract('value', r'^.*\s+([^\s]+)', 1).cast('integer').alias('byte'))


#count de hosts
agosto_limpo.groupby('hosts').count().show()
#count de http=404
agosto_limpo.groupBy('codigo_HTTP').count().filter('codigo_HTTP = "404"').show()
#count dos 5 maiores http=404 por url
agosto_limpo.filter('codigo_HTTP = "404"').groupby('URL').count().sort(col("count").desc()).show(5, truncate=False)
#count de erros 404 por dia (tratamento do campo timestamp, para retirada do campo hora)
agosto_limpo.filter('codigo_HTTP = "404"').groupby(agosto_limpo.data.substr(0,11)).count().show()
#Soma de bytes total
agosto_limpo.select('byte').groupby().sum().show()
#Soma por byte agrupado, pois me fiquei em duvida de qual alternativa é a correta
agosto_limpo.groupby('byte').sum().show()


#leitura do arquivo de julho
julho=spark.read.text("access_log_Jul95.log")

#tratamento e populando dataframe
julho_limpo= julho.select(regexp_extract('value',r'^([^\s]+\s)',1).alias('hosts'),
                            regexp_extract('value', r'((\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}))', 1).alias('data'),
                            regexp_extract('value', r'^.*"\w+\s+([^\s+]+)\s+HTTP.*', 1).alias('URL'),
                            regexp_extract('value', r'^.*\s+([^\s]+)\s', 1).cast('integer').alias('codigo_HTTP'),
                            regexp_extract('value', r'^.*\s+([^\s]+)', 1).cast('integer').alias('byte'))


#count de hosts
julho_limpo.groupby('hosts').count().show()
#count de http=404
julho_limpo.groupBy('codigo_HTTP').count().filter('codigo_HTTP = "404"').show()
#count dos 5 maiores http=404 por url
julho_limpo.filter('codigo_HTTP = "404"').groupby('URL').count().sort(col("count").desc()).show(5, truncate=False)
#count de erros 404 por dia (tratamento do campo timestamp, para retirada do campo hora)
julho_limpo.filter('codigo_HTTP = "404"').groupby(agosto_limpo.data.substr(0,11)).count().show()
#Soma de bytes total
julho_limpo.select('byte').groupby().sum().show()
#Soma por byte agrupado, pois me fiquei em duvida de qual alternativa é a correta
julho_limpo.groupby('byte').sum().show()
