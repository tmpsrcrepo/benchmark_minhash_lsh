from pyspark.ml.feature import HashingTF, IDF, Tokenizer,RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import StringIndexer,IndexToString
from operator import add
import redis
from pyspark.ml.feature import StopWordsRemover
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import time

def matchcomments(path_submits,path_comments):
    sqlContext.read.load(path_submits).registerTempTable('submits')
    sqlContext.read.load(path_comments).registerTempTable('comments')
    return sqlContext

def preprocessData(filepath):
    data = sqlContext.read.load(filepath)
    print data.count()
    data.registerTempTable('table1')
    return sqlContext.sql("SELECT id, title FROM table1")


def addItemRedis(dict_,path):
    r = redis.StrictRedis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=0)
    pipe = r.pipeline()
    for k,v in dict_.items():
        pipe.set(k,v)
    pipe.execute()

#TF-IDF item collection
def tf_idf_feature(wordsData):
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    for features_label in rescaledData.select("features", "id").take(3):
        print(features_label)


def exportRedis(rdd):
    r = redis.Redis(host='ec2-52-25-222-13.us-west-2.compute.amazonaws.com', port=6379, db=0)
    r.flushall()
    with r.pipeline() as pipe:
        for item in rdd:
            pipe.set(item[0],item[2])
        pipe.execute()



#write it on s3
def exportOnS3(qr, path, name):
    qr.write.save(path+name, format="parquet")

#preprocessing: tokenize + stopword removal
#tokenize data
def preprocessing_titles(path,name):
    query = preprocessData(path)
    tokenizer = Tokenizer(inputCol="title", outputCol="tokenized_title")
    wordsData = tokenizer.transform(query)
    #after Stopword removal
    remover = StopWordsRemover(inputCol="tokenized_title", outputCol="filtered")
    wordsData= remover.transform(wordsData)
    
    df = wordsData.map(lambda x:x['id']).zipWithUniqueId().toDF(["id","index"])
    df.registerTempTable("indices")
    wordsData.registerTempTable("words")
    
    qr = sqlContext.sql("SELECT index,words.id,filtered FROM indices JOIN words ON words.id = indices.id")
    if name!='':
        exportOnS3(qr,"s3a://redit-preprocessed/",name)
    qr = qr.map(lambda Row:(Row['index'],Row['id'],Row['filtered']))
#qr.foreachPartition(exportRedis)



if __name__ == "__main__":
    appName='preprocessing'
    #master='spark://ec2-50-112-76-236.us-west-2.compute.amazonaws.com:7077'
    conf = SparkConf().setAppName(appName).set("spark.cores.max", "30")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    start_time = time.time()
    
    preprocessing_titles("s3a://reddit-posts-partitioned/year=2015/month=1", "2015_1.parquet")



