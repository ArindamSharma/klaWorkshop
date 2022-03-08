import pandas as pd
import constant as C

def binning(rule:pd.DataFrame,table:pd.DataFrame)->pd.DataFrame:
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
    if(val1 not in order or val1 not in order):
        raise Exception("Error")
    if(order[val1]>order[val2]):
        return val2
    return val1

def mergebins(validationFile:str,*toMergeTable:pd.DataFrame)->pd.DataFrame:
    order=orderFile(validationFile)
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

rule0=pd.read_csv("Milestone3/Milestone3A_BinningRule_500.csv")
rule1=pd.read_csv("Milestone3/Milestone3A_BinningRule_501.csv")
rule2=pd.read_csv("Milestone3/Milestone3A_BinningRule_502.csv")
table=pd.read_csv("Milestone3/Milestone3A_DataInput1.csv")

order=orderFile("Milestone3/Milestone3A_PrecedenceFile1.txt")

table1=binning(rule0,table)
table2=binning(rule1,table)
table3=binning(rule2,table)
# print(table1)
# print(table2)
print(order,orderValidator(501,503,order))
mergerTable=mergebins("Milestone3/Milestone3A_PrecedenceFile1.txt",table1,table2,table3)
mergerTable.to_csv("m3out.csv",index=False)