import random,math
import sys
from random import randint,randrange
from pyspark import SparkContext, SparkConf,storagelevel
from pyspark.sql import SQLContext,Row
import itertools
from itertools import combinations
from collections import defaultdict
from operator import add  
import redis
import ast
import time

DB_HASH_FUNCS = 1
DB_LSH_BANDS = 5 
DB_POST_SIM = 3
DB_POST_TITLES = 0


def jaccard_similarity_dict(a,b):
    a_union_b = len(a)+len(b)
    intersection = 0
    lookup = defaultdict(int)
    for i in a:
        lookup[i]+=1
    for j in b:
        if j in lookup and lookup[j]>0:
            lookup[j]-=1
            intersection+=1
    return intersection*1.0/a_union_b
        
def jaccard_similarity_set(a,b):
    a_ = set(a)  
    b_ = set(b)
    common = a_.intersection(b_)
    if len(a_)+len(b_)-len(common)==0:
        return 0
    return len(common)*1.0/(len(a_)+len(b_)-len(common))



def preprocessTokenList(v):
    return ast.literal_eval(v)

def calcMinHash(row,hash_funcs):
    return [min(map(lambda x:((x*hash_func[0]+hash_func[1])%mod_val), row)) for hash_func in hash_funcs]

def createBands(row,band_row_width):
    return [hash(frozenset(row[i:i+band_row_width])) for i in xrange(0,len(row),band_row_width)]

def threshold(k,width):
    return math.pow(width*1.0/k,1.0/width)

    
def hashText(text):
    return [hash(t) for t in text]


def getPost_Titles(count):
    r = redis.StrictRedis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=DB_POST_TITLES)
    return sc.parallelize([(i,hashText(r.get(i))) for i in xrange(count)])


def createNumericMatrix(dim,numrange,rowCount):
    #key-value pairs
    return sc.parallelize([(j,[random.randint(0,numrange) for i in xrange(rowCount)]) for j in xrange(dim)])


def hashFunctions(num_hashes):
    a_hash = [randrange(sys.maxint) for _ in xrange(0, num_hashes)]
    b_hash = [randrange(sys.maxint) for _ in xrange(0, num_hashes)]
    return zip(a_hash,b_hash)


def generateHashFunc(k):
    r = redis.StrictRedis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=DB_HASH_FUNCS)
    if not r.get(k-1): #if the hash functions are not found from redis, then calculate it
        pipe = r.pipeline()
        hash_funcs =hashFunctions(k)
        #store them 
        for i,h in enumerate(hash_funcs):
            pipe.set(i,h)
        pipe.execute()
        return hash_funcs
    else:
        return [r.get((i)) for i in xrange(k)]


def export_minhash(val):
    r = redis.StrictRedis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=DB_LSH_BANDS)
    r.flushall()
    pipe = r.pipeline()
    
    for i,v in val:
        pipe.rpush(i,*v)
    pipe.execute()


def minHashFunction(vec2,k,band_row_width,hash_funcs,write_to_db):
    minhash = vec2.map(lambda row:(row[0],calcMinHash(row[1],hash_funcs),row[1]))
    
    
    bands = minhash.map(lambda hash_list:(hash_list[0],createBands(hash_list[1],band_row_width),hash_list[1]))
    band_hash_list = bands.flatMap(lambda x:[((i,x[1][i]),[x[0]]) for i in xrange(len(x[1]))])
    band_hash_list = band_hash_list.reduceByKey(add).filter(lambda x:len(x[1])>1)
    
    
    if write_to_db:
        band_hash_list.foreachPartition(export_minhash)
    
    return band_hash_list
    

def exportRedis_posts(rdd):
    r = redis.StrictRedis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=DB_POST_TITLES)
    r.flushall()
    #pipe = r.pipeline()
    for item in rdd:
        r.set(item[0],item[2])
    pipe.execute()

def exportOnS3(qr, path, name):
    qr.write.save(path+name, format="parquet")


def pairwise_similiary(vec1,vec2,threshold_val,N):
    query = None
    if N>0:
        vec1 = vec1.partitionBy('index')
        vec2 = vec2.partitionBy('index')
        query = vec1.cartesian(vec2)
        print query.first()
    
    else:
        vec1.registerTempTable("table1")
        vec2.registerTempTable("table2")
        query = sqlContext.sql("SELECT * FROM table1 JOIN table2 ON table1.index<table2.index")
        
    
    result = query.map(lambda x:Row(index1=x[0],index2=x[3],sim=jaccard_similarity_set(x[2],x[5])))\
    .filter(lambda x:x[2]>=threshold_val)
    return result

def calculate_output(out,k,b,word_map):
    out_tuples = out.flatMap(lambda x:list(combinations(x[1],2))).reduceByKey(lambda x,y:(x))\
    .map(lambda x:Row(index1= x[0], index2=x[1], token1=word_map[x[0]],token2=word_map[x[1]]))\
    .map(lambda x:Row(index1=x[0],index2=x[1],sim=jaccard_similarity_set(x[2],x[3])))
        
    return out_tuples


def bruteforce_test(vec,N):
    print "*********bruteforce**********************"
    #write_to_redis = False
    #k = 10
    #band_row_width = 2
    #threshold_val = threshold(k,band_row_width)
    start = time.time()
    result = pairwise_similiary(vec,vec,False,N)
    
    print 'brute-force',time.time()-start
    exportOnS3(result.toDF(),"s3a://redit-preprocessed/","join_result_2014_12_100k")

def minhash_test(vec,N):
    print "*********min hash**********************"
    
def pairs_test_(vec2, pairs):  
    for p in pairs:
        s = time.time()
        k,w = p
        threshold_val = threshold(k,w)
        hash_funcs = hashFunctions(k)
        out = minHashFunction(vec2,k,w,hash_funcs,False)
        out_tuples = calculate_output(out,k,w,word_map)
        print out_tuples.first()
        print "***********"
        print 'time', time.time()-s
        print 'total',out_tuples.count(),'total'
        print "***********"
        print 'filtered',out_tuples.filter(lambda x:x[2]>=threshold_val).count(),'filtered'
        print "***********"
        print 
        print
        print
        print
        print
        print


        
if __name__ == "__main__":   
    appName='minhash_lsh'
    conf = SparkConf().setAppName(appName).set("spark.cores.max", "30")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    #vec = sqlContext.read.load("s3a://redit-preprocessed/2007_all.parquet")  #279724
    vec = sqlContext.read.load("s3a://redit-preprocessed/output_12.parquet") #756888
    count = 756888
    mod_val = count*1.5
    #vec = sc.parallelize(vec.take(100000))
    word_map = vec.map(lambda x:(x['index'],x['filtered'])).collectAsMap()
    vec1 = vec.map(lambda x:(x['index'],hashText(x['filtered'])))
    vec2 = vec1.filter(lambda x:len(x[1])>0)
    print vec2.first()
    
    print "*****************************************"
    print "**********threshold ~0.44- 0.45**********"
    #pairs = [(10,2), (100,5),(200,5),(300,5)]
    #pairs_test_(vec2, pairs)
    pairs_test_(vec2, [(900,6)])
    
        
    print "*****************************************"
    print "**********threshold ~0.65- 0.66**********"    
    #pairs = [(20,4), (40,5), (256,8), (500,10)]
    pairs = [(256,8),(500,10),(1000,10)]
    pairs_test_(vec2, pairs)
    
    
    print "*****************************************"
    print "**********threshold ~0.81- 0.85**********"    
    pairs = [(10,5), (50,10), (300,15),(400,20),(1000,25)]
    pairs_test_(vec2, pairs)
    
    


    '''
    #Brute-force
    

    print "***************************************"
    print 'minhash+LSH is done. start bruteforce..'
    print "***************************************"
    

    '''
    print "*********bruteforce**********************"
    print "*********bruteforce**********************"    
    #N=1
    #vec = sc.parallelize(vec.take(100000))
    #bruteforce_test(vec,1)   
    
    
    
    