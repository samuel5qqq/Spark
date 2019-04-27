from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, split, desc

conf = SparkConf().setAppName("Q2").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

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

def get_mutualfriend(friend1, friend2):

    out = []

    for temp1 in friend1:
        for temp2 in friend2:
            if temp1 == temp2:
                out.append(temp1)

    return out


tokens = sc.textFile("soc-LiveJournal1Adj(1).txt").flatMap(lambda x : my_fun(x))
output = tokens.reduceByKey(lambda x, y : get_mutualfriend(x, y)).filter(lambda x : (len(x[1]) >= 1)).map(lambda x:(x[0], len(x[1])))

#create mutual friend list dataframe
mutual_friend = spark.createDataFrame(output)
mutual_friend = mutual_friend.select(col('_1').alias('user'), col('_2').alias('number'))
usercol = split(mutual_friend['user'], ',')
mutual_friend = mutual_friend.withColumn('usera', usercol.getItem(0)).withColumn('userb', usercol.getItem(1))\
    .orderBy(desc("number")).limit(10)

#create dataframe based on id and correspond address
userdataread = sc.textFile("userdata.txt").map(lambda x : x.split(",")).map(lambda x:(x[0],(str(x[3]+" "+x[4]+" "+x[5]+" "+x[6]+" "+x[7]))))
userdata = spark.read.text("userdata.txt")
userdata_col = split(userdata['value'], ',')
address = spark.createDataFrame(userdataread).select(col('_1').alias('id'), col('_2').alias('address'))

#create dataframe based on id and name, and then join with address dataframe
userdata = userdata.withColumn('id', userdata_col.getItem(0)).withColumn('first', userdata_col.getItem(1))\
    .withColumn('last', userdata_col.getItem(2)).drop('value')
userdata = userdata.join(address, userdata['id'] == address['id'], 'inner').dropDuplicates().drop(address.id)

#temp1 store first user's personal information, temp2 store second user's personal information
#Both of them join with mutual_friend dataframe to obtain mutual friend number and lsit
temp1 = mutual_friend.join(userdata, mutual_friend['usera'] == userdata['id'], 'inner').dropDuplicates().drop(userdata.id)
temp2 = mutual_friend.join(userdata, mutual_friend['userb'] == userdata['id'], 'inner').dropDuplicates()\
    .drop(userdata.id).drop('number')

#join first user and second user to get final result
result = temp1.join(temp2, temp1['user'] == temp2['user'], 'inner').dropDuplicates().\
    drop('user').drop('usera').drop('userb').drop('usera')\
    .limit(10).orderBy(desc('number'))

#convert to rdd foramt to output
out = result.rdd.map(list).map(lambda x:str(x[0])+"\t"+str(x[1])+"\t"+str(x[2])+"\t"+str(x[3])
                                        +"\t"+str(x[4])+"\t"+str(x[5])+"\t"+str(x[6]))
out.saveAsTextFile("Q2_SQL")
