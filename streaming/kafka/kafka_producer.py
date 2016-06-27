import random
import sys
import json
from kafka.client import SimpleClient
from kafka import KafkaProducer,KeyedProducer
import threading
from time import sleep


class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        #self.producer  = KeyedProducer(self.client)
        self.producer = KafkaProducer(bootstrap_servers=addr+":9092",value_serializer=lambda v: v.encode('utf-8'),acks=0, linger_ms=500)

    def produce_msgs(self):
        with open('2015_1_posts.json') as json_file:
            for line in json_file:
                print line
                self.producer.send('posts2015',line)
                sleep(0.05)
        json_file.close()
        
if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    #partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs() 


'''
class Producer(threading.Thread):

    def run(self):
        producer = KafkaProducer(bootstrap_servers=["localhost:9092"],\
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),\
                                      acks=0,\
                                      linger_ms=500)
        
    
        message_counts = 0
        with open('2015_1_posts.json') as json_file:
            for line in json_file:
                print message_counts
                print line
                producer.send('posts2015',json.loads(line))
                message_counts+=1
                time.sleep(1)
            print 'finished sending 2015/1 data'

        json_file.close()


    
if __name__ == "__main__":
    #args = sys.argv
    #ip_addr = str(args[1])
    #partition_key = str(args[2])
    Producer().start()
    
    time.sleep(10)
'''