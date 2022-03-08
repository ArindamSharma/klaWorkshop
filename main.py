from operator import index
from sqlite3 import paramstyle
from time import sleep
import yaml
import pandas as pd
from datetime import datetime as dt
import constant as C
import threading

# Uitility Functions 
def log(*arg,**kwarg):
    '''Creates a Log Entry to the Log File Globaly defined'''

    global logFile
    print(*arg,**kwarg,sep="")
    logFile.writelines([str(i) for i in arg]+["\n"])
    # print(*arg,**kwarg)

def parseKey(string:str)->str:
    '''Used to Parse the Key from the Input String'''
    return string.strip("$").strip("(").strip(")")
    # return string[2:-1]


def binning(rule:pd.DataFrame,table:pd.DataFrame)->pd.DataFrame:
    '''This Function Is Used to add Bin Code to the Input Table according to rules
    return a Dataframe'''
    data=pd.DataFrame(columns=[*table.keys()]+[C.BIN_ID])
    count=0
    for b in range(len(rule)):
        for i in range(len(table)):
            signalvalue=table.at[i,C.SIGNAL]
            if(eval(rule.at[b,C.RULE].replace(C.SIGNAL,str(signalvalue)))):
                data.loc[count]=[*table.loc[i]]+[rule.at[b,C.BIN_ID]]
                count+=1
    return data

def orderFile(filename:str)->dict:
    """Read Rules from File and Return a Dictionary With the Order"""
    string=None
    with open(filename,"r") as f:
        string=f.readline()
    d={}
    count=0
    for i in string.split(">>"):
        d[int(i.strip())]=count
        count+=1
    return d

def orderValidator(val1:int,val2:int,order:dict)->bool:
    """This Function is use to validate bicode as per the rules dictionary passed """
    if(val1 not in order or val1 not in order):
        raise Exception("Error")
    if(order[val1]>order[val2]):
        return val2
    return val1

def mergebins(validationFile:str,*toMergeTable:pd.DataFrame)->pd.DataFrame:
    """This Function is takes Rules File Name as first parameter and variable Dataframes as further parameters
    and return the merged table Dataframe with Applied rules from the file"""
    order=orderFile(validationFile)
    if(len(toMergeTable)==0):
        raise Exception("No Table Passed ")

    result=pd.DataFrame(columns=toMergeTable[0].keys())
    count=0
    for table in toMergeTable:
        for i in range(len(table)):
            x=result.index[result[C.ID]==table.at[i,C.ID]].tolist()
            if(len(x)!=0):
                result.at[x,C.BIN_ID]=orderValidator(table.at[i,C.BIN_ID],table.at[x,C.BIN_ID],order)
            else:
                result.loc[count]=table.loc[i]
                count+=1
    return result

# User Defined Functions
def mergeResult(taskName:str,data:dict)->None:
    """This Function Calls Merge bin Function with Parameters definec in Configure File"""
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",")")
    dataframeList=[]
    for key in data[C.INPUTS]:
        if(key[:len(C.DATASET)]==C.DATASET):
            dataframeList.append(C.CSVDATA[parseKey(data[C.INPUTS][key])].copy())
    C.CSVDATA[taskName+"."+C.MERGEDRESULTS]=mergebins(C.DIRECTORY+"/"+data[C.INPUTS][C.PRECEDENCEFILE],*dataframeList)
    C.CSVDATA[taskName+"."+C.MERGEDRESULTS].rename(columns={C.BIN_ID:C.BINCODE},inplace=True)

def exportResult(taskName:str,data:dict)->None:
    """This Function Finally Saves the CSV to the Path mentioned in the Config /YAML file"""
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",")")
    C.CSVDATA[parseKey(data[C.INPUTS][C.DEFECTTABLE])].to_csv(C.DIRECTORY+"/"+data[C.INPUTS][C.EXPORTFILENAME],index=False)

def binningFunction(taskName:str,data:dict)->None:
    """Bin Function Binns the Table into Binned Table where it follow certain Rules which also need to be mentioned in the Yaml file"""
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",")")
    table=C.CSVDATA[parseKey(data[C.INPUTS][C.DATASET])].copy()
    rule=pd.read_csv(C.DIRECTORY+"/"+data[C.INPUTS][C.RULEFILENAME])
    C.CSVDATA[taskName+"."+C.BINNINGRESULTSTABLE]=binning(rule,table)
    C.CSVDATA[taskName+"."+C.NOOFDEFECTS]=len(C.CSVDATA[taskName+"."+C.BINNINGRESULTSTABLE])

def timeFunction(taskName:str,data:dict)->None:
    '''This Function is Use to Sleep For Given Seconds to Simulate the Task Execution Time'''

    # print(data[C.INPUTS])
    if(C.FUNCTIONINPUT in data[C.INPUTS]):
        tmp=parseKey(data[C.INPUTS][C.FUNCTIONINPUT])
        if(tmp in C.CSVDATA):
            data[C.INPUTS][C.FUNCTIONINPUT]=C.CSVDATA[tmp]
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",data[C.INPUTS][C.FUNCTIONINPUT],",",data[C.INPUTS][C.EXECUTIONTIME],")")
    exeTime=int(data[C.INPUTS][C.EXECUTIONTIME])
    sleep(exeTime)

def dataLoadFunction(taskName:str,data:dict)->None:
    '''Load Csv Data into respective Task'''
    param=""
    for p in data[C.INPUTS].values():
        param+=p+','
    log(dt.now(),";",taskName," ",C.EXECUTE," ",data[C.FUNCTION],"(",param[:-1],")")
    
    C.CSVDATA[taskName+"."+C.DATATABLE]=pd.read_csv(C.DIRECTORY+"/"+data[C.INPUTS][C.FILENAME])
    C.CSVDATA[taskName+"."+C.NOOFDEFECTS]=str(len(C.CSVDATA[taskName+"."+C.DATATABLE]))
    # print(taskName,C.CSVDATA)

def conditionValidation(condition:str)->bool:
    '''Checks If the Sting consist of any Key or Not'''
    condition=condition.split()
    condition[0]=parseKey(condition[0])
    try:
        condition=C.CSVDATA[condition[0]]+"".join(condition[1:])
        return eval(condition)
    except KeyError as e:
        raise e

# Handler Functions 
def taskHandler(taskName:str,data:dict)->None:
    """Task handler Executes the Task"""
    # Error handling
    if(data[C.TYPE]!=C.TASK):
        raise ValueError("FlowType required 'Flow', given ",data[C.TYPE])

    log(dt.now(),";",taskName," "+C.ENTRY)
    
    # Condition Parameter Check
    flag=0
    if(C.CONDITION in data):
        if(conditionValidation(data[C.CONDITION])):
            flag=1
    else:
        flag=1
      
    if(flag==1):
        if(data[C.FUNCTION]==C.TIMEFUNCTION):
            timeFunction(taskName,data)
        elif(data[C.FUNCTION]==C.DATALOAD):
            dataLoadFunction(taskName,data)
        elif(data[C.FUNCTION]==C.BINNING):
            binningFunction(taskName,data)
        elif(data[C.FUNCTION]==C.MERGERESULTS):
            mergeResult(taskName,data)
        elif(data[C.FUNCTION]==C.EXPORTRESULTS):
            exportResult(taskName,data)
        else:
            raise ValueError("Unknown Function Type Passed ",data[C.FUNCTION])
    else:
        log(dt.now(),";",taskName," "+C.SKIPPED)
    
    log(dt.now(),";",taskName," "+C.EXIT)

def flowHandler(flowName:str,data:dict)->None:
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

# Main Function
def milestone(dir:str,yamlFileName:str)->None:
    """This Function Runs Check the Flow and Call Flow Handler"""
    
    # Few Global Defination
    C.DIRECTORY=dir
    C.YAMLFILENAME=yamlFileName

    with open(C.DIRECTORY+"/"+yamlFileName,"r") as f:
        flowData=yaml.safe_load(f)
        for flowName in flowData:
            flowHandler(flowName,flowData[flowName])

if __name__=="__main__":
    """Use to Do File Check"""

    # with open("M1Alog.txt","w") as logFile:
    #     milestone("Milestone1/Milestone1A.yaml")

    # with open("M1Blog.txt","w") as logFile:
    #     milestone("Milestone1/Milestone1B.yaml")
    
    # with open("M2Alog.txt","w") as logFile:
    #     milestone("Milestone2","Milestone2A.yaml")

    # with open("M2Blog.txt","w") as logFile:
    #     milestone("Milestone2","Milestone2B.yaml")

    with open("M3Alog.txt","w") as logFile:
        milestone("Milestone3","Milestone3A.yaml")

    