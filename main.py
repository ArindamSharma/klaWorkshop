import yaml
import pandas as pd

def milestone1():
    with open("Milestone1/Milestone1A.yaml","r") as f:
        data=yaml.safe_load(f)
        print(data)

if __name__=="__main__":
    milestone1()