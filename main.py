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

def timeFunction(taskName,data):
    param=""
    print(data[C.INPUTS])
    for p in data[C.INPUTS].values():
        param+=p+','
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",param[:-1],")")
    exeTime=int(data[C.INPUTS][C.EXECUTIONTIME])
    sleep(exeTime)

def dataLoadFunction(taskName,data):
    param=""
    for p in data[C.INPUTS].values():
        param+=p+','
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",param[:-1],")")
    
    tmp=pd.read_csv(C.DIRECTORY+"/"+data[C.INPUTS][C.FILENAME])
    C.CSVDATA[taskName+"."+C.NOOFDEFECTS]=str(len(tmp))
    print(taskName,C.CSVDATA)

def conditionValidation(condition)->bool:
    
    condition=condition.split()
    condition[0]=condition[0][2:-1]
    try:
        condition=C.CSVDATA[condition[0]]+"".join(condition[1:])
        return eval(condition)
    except KeyError as e:
        raise e

def taskHandler(taskName,data)->None:
    """Task handler Executes the Task"""
    # Error handling
    if(data[C.TYPE]!=C.TASK):
        raise ValueError("FlowType required 'Flow', given ",data[C.TYPE])

    log(dt.now(),";",taskName," "+C.ENTRY)
    
    if(C.CONDITION in data):
        if(conditionValidation(data[C.CONDITION])):
            print("Condition True")
            if(data[C.FUNCTION]==C.TIMEFUNCTION):
                timeFunction(taskName,data)
            elif(data[C.FUNCTION]==C.DATALOAD):
                dataLoadFunction(taskName,data)
            else:
                raise ValueError("Unknown Function Type Passed ",data[C.FUNCTION])
        else:
            print("Condition True")
            log(dt.now(),";",taskName," "+C.SKIPPED)
    else:
        print("Condition Dont Exist")
        
        if(data[C.FUNCTION]==C.TIMEFUNCTION):
            timeFunction(taskName,data)
        elif(data[C.FUNCTION]==C.DATALOAD):
            dataLoadFunction(taskName,data)
        else:
            raise ValueError("Unknown Function Type Passed ",data[C.FUNCTION])
        
    log(dt.now(),";",taskName," "+C.EXIT)

def flowHandler(flowName,data):
    """Flow handler Controls the Flow"""
    # Error handling
    if(data[C.TYPE]!=C.FLOW):
        raise ValueError("FlowType required 'Flow', given ",data[C.TYPE])
    
    log(dt.now(),";",flowName," "+C.ENTRY)
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
                currentThread.name=flowName+"."+activity
                threadList.append(currentThread)
                currentThread.start()
                # taskHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            elif(data[C.ACTIVITIES][activity][C.TYPE]==C.FLOW):
                currentThread=threading.Thread(target=flowHandler,args=(flowName+"."+activity,data[C.ACTIVITIES][activity]))
                currentThread.name=flowName+"."+activity
                threadList.append(currentThread)
                currentThread.start()
                # flowHandler(flowName+"."+activity,data[C.ACTIVITIES][activity])
            else:
                raise ValueError("Unknown Activity Type passed ",data[C.ACTIVITIES][activity][C.TYPE])
        for localThread in threadList:
            localThread.join()
    else:
        raise ValueError("Unknown Execution Parameter passed",data[C.EXECUTION])
    
    log(dt.now(),";",flowName," "+C.EXIT)

def milestone(dir,yamlFileName):
    C.DIRECTORY=dir
    C.YAMLFILENAME=yamlFileName

    with open(C.DIRECTORY+"/"+yamlFileName,"r") as f:
        flowData=yaml.safe_load(f)
        for flowName in flowData:
            flowHandler(flowName,flowData[flowName])

if __name__=="__main__":

    # with open("M1Alog.txt","w") as logFile:
    #     milestone("Milestone1/Milestone1A.yaml")

    # with open("M1Blog.txt","w") as logFile:
    #     milestone("Milestone1/Milestone1B.yaml")
    
    with open("M2Alog.txt","w") as logFile:
        milestone("Milestone2","Milestone2A.yaml")

    with open("M2Blog.txt","w") as logFile:
        milestone("Milestone2","Milestone2B.yaml")

    