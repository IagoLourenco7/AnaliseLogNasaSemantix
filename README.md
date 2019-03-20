# AnaliseLogNasaSemantix

#Fontes para desenvolvimento do código e métodos com SparkSQL
https://pt.stackoverflow.com/questions/30418/localizar-aspas-com-espa%C3%A7os-em-branco-com-regex

https://stackoverflow.com/questions/38626071/how-to-extract-timestamp-and-remove-tailing-portion-from-weblog-using-regex-in-p

https://regex101.com/r/oH4xS8/1

https://www.w3schools.com/python/python_regex.asp

https://stackoverflow.com/questions/37949494/how-to-count-occurrences-of-each-distinct-value-for-every-column-in-a-dataframe

https://stackoverflow.com/questions/34514545/spark-dataframe-groupby-and-sort-in-the-descending-order-pyspark

#Exercicios questionário
Qual o objetivo do comando cache em Spark?
R: Método para persistencia de processamento em memoria.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
R: Porque o MapReduce funciona em Disco, enquanto o Spark funciona em memoria

Qual é a função do SparkContext?
R:Objeto de conexão entre o Spark e o Programa em desenvolvimento.

Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
R: Objetos responsáveis pelas execuções de processamento de dados. Funciona como uma coleções de objetos distribuidos em clusters,
   que podem trabalhar paralelamente.

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
R: Porque com reduceByKey é feito a verificação de pares de chaves repetidas e reduzidas, enquanto com GroupByKey é mantida as chaves iguais.

Explique o que o código Scala abaixo faz.
R:  val textFile = sc.textFile("hdfs://...") -- Salva o caminho do HDFS na variavel TextFile
	val counts = textFile.flatMap(line => line.split(" ")) -- Conta a quantidade de arquivos no caminho e trata as aspas dos arquivos
				.map(word => (word, 1)) -- mapeando os arquivos do filesystem
				.reduceByKey(_ + _) -- metodo ReduceByKey agrupando os arquivos com par chave diferente
		  counts.saveAsTextFile("hdfs://...") -- Salva a quantidade de arquivos no HDFS
