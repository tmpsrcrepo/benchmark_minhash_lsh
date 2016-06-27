#from __future__ import print_function
import pyspark
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import json
from pyspark import SparkContext, SparkConf,storagelevel
from pyspark.sql import SQLContext,Row
import pickle
import sys
import redis
from operator import add
from itertools import combinations


REDIS_NODE_ = "ec2-52-39-121-57.us-west-2.compute.amazonaws.com"
PASSWORD_ = "Lxs-@91q"
DB_LSH_BANDS = 5
DB_WORD_MAP = 9


conf = SparkConf().setAppName('read_comments').set("spark.cores.max", "20")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)
k=400
band_row_width = 10
mod_val = 756888*2
start_index = 756888
word_map = pickle.load(open('../word_map.p'))

#calcMinHash([0,1],hash_funcs)

def calcMinHash(row,hash_funcs):
    return [min(map(lambda x:((x*hash_func[0]+hash_func[1])%mod_val), row)) for hash_func in hash_funcs]

def createBands(row,band_row_width):
    return [(i/band_row_width,hash(frozenset(row[i:i+band_row_width]))) for i in xrange(0,len(row),band_row_width)]

def jaccard_similarity_set(a,b):
    a_ = set(a)
    b_ = set(b)
    common = a_.intersection(b_)
    if len(a_)+len(b_)-len(common)==0:
        return 0
    return len(common)*1.0/(len(a_)+len(b_)-len(common))


def hashText(text):
    return [hash(t) for t in text]

def loadDict(path):
    return pickle.load(open(path,'rb'))

hash_funcs = loadDict('../400_hash_funcs.p')

def getVal(x):
    #x[0] = kafka offset, x[1] = objects
    f = json.loads(x[1])
    return (start_index+x[0],int(hash(f['link_id'])),f['body'])

def updateDB(rdd):
    r = redis.StrictRedis(REDIS_NODE_,db=0,password=PASSWORD_)
    for item in rdd:
        r.set(item[0],item[1])

def minHashFunction(vec2,k,band_row_width,hash_funcs,write_to_db):
    minhash = vec2.map(lambda row:(row[0],calcMinHash(row[1],hash_funcs),row[1]))
    bands = minhash.map(lambda hash_list:(hash_list[0],createBands(hash_list[1],band_row_width),hash_list[1]))
    band_hash_list = bands.flatMap(lambda x:[((i,x[1][i]),[x[0]]) for i in xrange(len(x[1]))])
    band_hash_list = band_hash_list.reduceByKey(add).filter(lambda x:len(x[1])>1)
    if write_to_db:
        band_hash_list.foreachPartition(export_minhash)
    
    return band_hash_list

def minhash_lsh(hash_list,hash_funcs,band_row_width):
    row = calcMinHash(hash_list,hash_funcs)
    return createBands(row,band_row_width)


def findGroups(band_hash_list,id_)
r = redis.StrictRedis(REDIS_NODE_,db=DB_LSH_BANDS,password=PASSWORD_)
    groups = []
    for band in band_hash_list:
        band_tuple = str(band)
        tmp = r.lrange(band_tuple,0,-1)
        groups+=tmp
        #update the list
        r.rpush(band_tuple,*[id_])
return groups

word_map = loadDict('../400_hash_funcs.p')

#sublinear calculation for individual lookup
def findBest(groups,text):
    r = redis.StrictRedis(REDIS_NODE_,db=DB_WORD_MAP,password=PASSWORD_)
    group_pairs = groups
    jaccard_sims = [jaccard_similarity_set(text,ast.literal_eval(r.get(i))) for i in set(groups)]
    return groups[np.argmax(jaccard_sims)]




#windowedWordCounts = pairs.reduceByKeyAndWindow(add, 30, 10)

brokers = 'ec2-52-34-227-251.us-west-2.compute.amazonaws.com:9092,ec2-52-34-144-206.us-west-2.compute.amazonaws.com:9092,ec2-52-39-7-242.us-west-2.compute.amazonaws.com:9092'

kafkaStream = KafkaUtils.createDirectStream(ssc, ['posts'], {"metadata.broker.list": brokers})

#kafkaStream = KafkaUtils.createStream(ssc, "ec2-52-34-227-251.us-west-2.compute.amazonaws.com:2181", "receivers", {"posts": 1})


comments = kafkaStream.map(getVal).filter(lambda x:x[1]!='[deleted]')
hash_list = comments.map(lambda x:(x[0],hashText(x[2].split())))
.foreachRDD(lambda rdd: rdd.foreachPartition(updateDB))

print 'print !!!!!!!!!!!'



ssc.start()
ssc.awaitTermination()