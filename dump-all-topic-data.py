#!/usr/bin/env python
from kafka import KafkaConsumer
import threading, time, logging, json, sys



def showdata(myconsumer):
    for x in myconsumer:
        yield x
        myconsumer.commit()


def testjson(jsonstr):
    try:
        myobj=json.loads(jsonstr)
        return True
    except:
        pass
    return False

mymessages=[]
problems=[]
def runit(topicname,groupid):
    global mymessages, problems
    bootstrap_servers = ['192.168.1.2:29092']
    print(topicname + " " + groupid)
    consumer = KafkaConsumer (topicname, group_id =groupid,bootstrap_servers =bootstrap_servers, auto_offset_reset='earliest',max_poll_records=10000)
    generator = showdata(consumer)
    for k in generator:
        msg=k.value
        isjson=testjson(msg)
        if isjson:
            if msg not in mymessages:
                mymessages.append(msg)
        else:
            if msg not in problems:
                problems.append(msg)

def counter():
    global mymessages
    while True:
        time.sleep(3)
        #print("Valid Messages:")
        
        #for msg in mymessages:
        #    print(json.loads(msg))
        print("\n\n")
        print("Invalid Messages:")

        for msg in problems:
            print(json.loads(msg))

        print("Valid Message Count:")
        print(len(mymessages))       
        print("Invalid Message Count:")             
        print(len(problems))        

if __name__ =="__main__":
    
    t1 = threading.Thread(target=runit, args=('isim_user_log','83'))
    t1.start()    
    t4 = threading.Thread(target=counter)
    t4.start()    
    print("ok")
    




