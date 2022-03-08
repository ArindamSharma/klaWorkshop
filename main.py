from sqlite3 import paramstyle
from time import sleep
import yaml
import pandas as pd
from datetime import datetime as dt
import constant as C
import threading


def log(*arg,**kwarg):
    global logFile
    print(*arg,**kwarg,sep="")
    logFile.writelines([str(i) for i in arg]+["\n"])
    # print(*arg,**kwarg)

def taskHandler(taskName,data):
    """Task handler Executes the Task"""
    # Error handling
    if(data[C.TYPE]!=C.TASK):
        raise ValueError("FlowType required 'Flow', given ",data[C.TYPE])

    log(dt.now(),";",taskName," Entry")
    param=""
    for p in data[C.INPUTS].values():
        param+=p+','
    log(dt.now(),";",taskName," Executing ",data[C.FUNCTION],"(",param[:-1],")")
    exeTime=int(data[C.INPUTS][C.EXECUTIONTIME])
    sleep(exeTime)
    log(dt.now(),";",taskName," Exit")

def flowHandler(flowName,data):
    """Flow handler Controls the Flow"""
    # Error handling
    if(data[C.TYPE]!=C.FLOW):
        raise ValueError("FlowType required 'Flow', given ",data[C.TYPE])
    
    log(dt.now(),";",flowName," Entry")
    if(data[C.EXECUTION]==C.SEQUENTIAL):
        for activity in data[C.ACTIVITIES]:
            if(data[C.ACTIVITIES][activity][C.TYPE]==C.TASK):
                taskHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            elif(data[C.ACTIVITIES][activity][C.TYPE]==C.FLOW):
                flowHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            else:
                raise ValueError("Unknown Activity Type passed ",data[C.ACTIVITIES][activity][C.TYPE])

    elif(data[C.EXECUTION]==C.CONCURRENT):
        threadList=[]
        for activity in data[C.ACTIVITIES]:
            if(data[C.ACTIVITIES][activity][C.TYPE]==C.TASK):
                currentThread=threading.Thread(target=taskHandler,args=(flowName+"."+activity,data[C.ACTIVITIES][activity]))
                threadList.append(currentThread)
                currentThread.start()
                # taskHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            elif(data[C.ACTIVITIES][activity][C.TYPE]==C.FLOW):
                currentThread=threading.Thread(target=flowHandler,args=(flowName+"."+activity,data[C.ACTIVITIES][activity]))
                threadList.append(currentThread)
                currentThread.start()
                # flowHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            else:
                raise ValueError("Unknown Activity Type passed ",data[C.ACTIVITIES][activity][C.TYPE])
        for localThread in threadList:
            localThread.join()
    else:
        raise ValueError("Unknown Execution Parameter passed",data[C.EXECUTION])
    
    log(dt.now(),";",flowName," Exit")

def milestone1(path):
    with open(path,"r") as f:
        flowData=yaml.safe_load(f)
        for flowName in flowData:
            flowHandler(flowName,flowData[flowName])


def milestone2(path):
    pass

if __name__=="__main__":

    with open("M1Alog.txt","w") as logFile:
        milestone1("Milestone1/Milestone1A.yaml")

    with open("M1Blog.txt","w") as logFile:
        milestone1("Milestone1/Milestone1B.yaml")
    
    # with open("M2Alog.txt","w") as logFile:
    #     milestone1("Milestone2/Milestone2A.yaml","Milestone2/Milestone2A.yaml")

    