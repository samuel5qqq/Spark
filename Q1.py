from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Q1").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)


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
output = tokens.reduceByKey(lambda x, y : get_mutualfriend(x, y)).filter(lambda x : (len(x[1]) >= 1))
final = output.map(lambda x : str(x[0])+"\t"+str(len(x[1])))
final.saveAsTextFile("Q1")


