from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf= SparkConf().setAppName("First_RDD").setMaster("local[*]")
    sc= SparkContext(conf=conf)
    # data = [{"a":1},{"b":2},{"c":3},{"d":4}]
    # rdd1= sc.parallelize(data)
    # print("parallelized_rdd:")
    # print(rdd1.collect())
    # # print(parallelised_rdd.getNumPartitions())
    #
    # rdd2=rdd1.flatMap(lambda x: x.items())
    # outputrdd = rdd2.filter(lambda x:x[1]==1 or x[1]==2 or x[1]==3)
    # print(outputrdd.collect())
    #
    # multiplyrdd= outputrdd.map(lambda x:x[1]*25)
    # print(multiplyrdd.collect())
    #
    # evenrdd= multiplyrdd.map(lambda x:x%2==0)
    # print(evenrdd.collect())

    lst = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(lst)
    # print(rdd1.getNumPartitions())
    # print(rdd1.glom().collect())
    print(rdd1.context)


