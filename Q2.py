from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Q2").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)



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
user = sc.textFile("userdata.txt").map(lambda x : x.split(",")).map(lambda x:(x[0],(x[1], x[2], str(x[3]+" "+x[4]+" "+x[5]+" "+x[6]+" "+x[7]))))
friend = tokens.reduceByKey(lambda x, y : get_mutualfriend(x, y)).filter(lambda x : (len(x[1]) >= 1))
topfriendlist = friend.map(lambda x: (x[0], len(x[1]))).top(10, key=lambda x : x[1])
topfriend = sc.parallelize(topfriendlist).map(lambda x : (x[0].split(","), x[1]))
list1 = topfriend.map(lambda x : (x[0][0], (str(x[0]), x[1])))
list2 = topfriend.map(lambda x : (x[0][1], (str(x[0]), x[1])))
list1_join = user.join(list1).map(lambda x : (str(x[1][1][0]),(x[1][0], x[1][1][1])))
list2_join = user.join(list2).map(lambda x : (str(x[1][1][0]),(x[1][0], x[1][1][1])))
final = list1_join.join(list2_join).map(lambda x : str(x[1][0][1])+"\t"+str(x[1][0][0][0])+"\t"+str(x[1][0][0][1])+"\t"+str(x[1][0][0][2])+"\t"
                                        +str(x[1][1][0][0])+"\t"+str(x[1][1][0][1])+"\t"+str(x[1][1][0][2]))
final.saveAsTextFile("Q2")
