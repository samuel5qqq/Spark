 from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, split

conf = SparkConf().setAppName("Q1").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#function to map friend list
def my_fun(line):

    data = line.split("\t")

    if(len(data) == 1 or data[0] == ''):
        return []

    if(len(data) == 2):
        id = data[0]
        friend = data[1].split(",")

        out = []
        for temp in friend:
            if temp !='':
                if(int(id) < int(temp)):
                    out.append((str(id + "," + temp), friend))
                if(int(id) > int(temp)):
                    out.append((str(temp + "," + id), friend))
        return out

#function to reduce same friend
def get_mutualfriend(friend1, friend2):

    out = []

    for temp1 in friend1:
        for temp2 in friend2:
            if temp1 == temp2:
                out.append(temp1)

    return out


tokens = sc.textFile("soc-LiveJournal1Adj(1).txt").flatMap(lambda x : my_fun(x))
output = tokens.reduceByKey(lambda x, y : get_mutualfriend(x, y)).filter(lambda x : (len(x[1]) >= 1)).map(lambda x:(x[0], len(x[1])))

#create dataframe
mutual_friend = spark.createDataFrame(output)
mutual_friend = mutual_friend.select(col('_1').alias('user'), col('_2').alias('number'))
usercol = split(mutual_friend['user'], ',')
mutual_friend = mutual_friend.withColumn('usera', usercol.getItem(0)).withColumn('userb', usercol.getItem(1)).drop('user')

#convert to rdd foramt to output
out = mutual_friend.rdd.map(list).map(lambda x:str(x[1])+","+str(x[2])+"\t"+str(x[0]))
out.saveAsTextFile("Q1_SQL")


