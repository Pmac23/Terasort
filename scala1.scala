val t0 = System.nanoTime
val textFile = spark.sparkContext.textFile("hdfs://ec2-54-236-10-239.compute-1.amazonaws.com:9000/pmac/64/128GB_Input")

val mappedfile=textFile.map(line => (line.substring(0,10),line.substring(10,line.length())))
//mappedfile.collect()

val sort1 = mappedfile.sortByKey()
val sorted=sort1.map {case (key,value) => s"$key $value"}
textFile.unpersist
sorted.saveAsTextFile("/pmac/output/")
val duration0 = (System.nanoTime – t2) / 1e9d
