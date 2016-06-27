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
import numpy as np 
import ast

REDIS_NODE_ = "ec2-50-112-43-106.us-west-2.compute.amazonaws.com"
PASSWORD_ = "Lxs-@91q"
DB_LSH_BANDS = 5 
DB_WORD_MAP = 9
DB_TOP_1 = 10
DB_INDEX_WORDS = 0

sc = SparkContext("spark://ip-172-31-2-77:7077", appName="Streaming_2015")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)

k=400
band_row_width = 10
mod_val = 756888*2
start_index = 756888

def loadDict(path):
    return pickle.load(open(path,'rb'))

hash_funcs = loadDict('../400_hash_funcs.p')

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



def getVal(x):
    #x[0] = kafka offset, x[1] = objects
    f = json.loads(x[1])
    return (f['index'],f['text'])

def minhash_lsh(hash_list,hash_funcs,band_row_width):
    row = calcMinHash(hash_list,hash_funcs)
    return createBands(row,band_row_width)
    

def findGroups(band_hash_list,id_):
    r = redis.StrictRedis(REDIS_NODE_,db=DB_LSH_BANDS,password=PASSWORD_)
    groups = []
    for band in band_hash_list:
        band_tuple = str(band)
        tmp = r.lrange(band_tuple,0,-1)
        groups+=tmp
        #update the list
        #r.rpush(band_tuple,*[id_])
    return groups


#sublinear calculation for individual lookup
def findBest(groups,text,r):
    
    #groups
    if len(groups)>0:
        print 'groups::::::::',groups
        jaccard_sims = [jaccard_similarity_set(text,ast.literal_eval(r.get(int(i)))) for i in set(groups) if r.get(int(i))]
        #jaccard_sims.pprint()
        print jaccard_sims
        return groups[np.argmax(jaccard_sims)]
    else:
        return -1


def calculation(x,r,r_db):
    index = int(x[0])
    #text = x[1].split()
    text = x[1]
    hash_list = hashText(text)
    r.set(index,text)
    band_hash_list = minhash_lsh(hash_list,hash_funcs,band_row_width)
    groups = findGroups(band_hash_list,index)
    top_item= findBest(groups,text,r)
    if top_item!=-1:
        r_db.rpush(0,*[index])
        r_db.rpush(index,*[top_item])
    r_db.rpush(-1,*[index])
    

def updateDB(rdd):
    r = redis.StrictRedis(REDIS_NODE_,db=DB_WORD_MAP,password=PASSWORD_)
    r_db = redis.StrictRedis(REDIS_NODE_,db=DB_TOP_1,password=PASSWORD_)
    for item in rdd:
        if len(item[1])>0:
            calculation(item,r,r_db)
        
    #item[0] = index
    #item[1] = text
    
        
#windowedWordCounts = pairs.reduceByKeyAndWindow(add, 30, 10)

brokers = 'ec2-52-41-146-248.us-west-2.compute.amazonaws.com:9092,ec2-52-27-111-183.us-west-2.compute.amazonaws.com:9092,ec2-52-25-133-159.us-west-2.compute.amazonaws.com:9092'
kafkaStream = KafkaUtils.createDirectStream(ssc, ['posts2015'], {"metadata.broker.list": brokers})

#kafkaStream = KafkaUtils.createStream(ssc, "ec2-52-27-111-183.us-west-2.compute.amazonaws.com:2181", "receiver", {"posts2015": 1})

comments = kafkaStream.map(getVal).foreachRDD(lambda rdd: rdd.foreachPartition(updateDB))
#comments.pprint()
#print 'test!!!!!!!'

    
ssc.start()
ssc.awaitTermination()