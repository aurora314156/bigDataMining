
# coding: utf-8

# In[10]:


import pandas as pd
import numpy as np
import math
import csv
import re
import json
import time
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions 
from pyspark.sql import Row
from collections import OrderedDict




# In[11]:


# parsing data from sgm file

from bs4 import BeautifulSoup,SoupStrainer


def readData(fileName):

    data = ''
    f = open(fileName, 'rb')
    soup = BeautifulSoup(f, 'html.parser')
    # get body content
    contents = soup.findAll('body')
            
    return contents

# parsing data from sgm file


# In[12]:


# Q1


def Q1():

    # result txt name
    resName = "Q1_Ans.txt"

    sTime = time.time()
    file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
    cal = 0
    for i in range(1):

        fileName = "/home/ethan/pythonwork/ipynotebook/HW3/Data/reut2-"
        fileName = fileName + file[i] + ".sgm"
        print(fileName +" start!")
        # read body content from each file
        data = readData(fileName)
        # retrival all body content
        for content in data:
            # save each .sgm file ans
            ans = ''
            textList = []
            splitContent = content.get_text().split("\n")
            # split body content by \n
            for c in splitContent:
                eachLine = c.strip()
                # remove header space and end space
                for s in eachLine.split(" "):
                    #print(s)
                    if s is not '' or s is not "&#3;":
                        textList.append(s.lower())
            # storage for Shingle
            twoShingleList = set()
            # select two shingle on each time
            for ind in range(len(textList)-1): 
                shingle = textList[ind] + ',' + textList[ind+1]
                if shingle not in twoShingleList:
                    cal += 1
                    # add shingle
                    twoShingleList.add(shingle)
            del textList
            for t in twoShingleList:
                ans = ans + t + " "
            # release memory
            del twoShingleList
        ans = ans +"\n\n" 
        # write result to txt
        with open(resName, 'a') as out:
            out.write(ans)

        del ans

        print(fileName +" done!\n")
    print(cal)
    print("Q1 took %.2f" % (time.time()-sTime))

    
#Q1()    

# Q1


# In[13]:


# Q2

def Q2():
    
    # result txt name
    resName = "Q2_Ans.txt"

    sTime = time.time()
    file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
    cal = 0
    for i in range(1):
        
        fileName = "/home/ethan/pythonwork/ipynotebook/HW3/Data/reut2-"
        fileName = fileName + file[i] + ".sgm"
        print(fileName +" start!")
        # read body content from each file
        data = readData(fileName)
        # retrival all body content
        for content in data:
            # save each .sgm file ans
            ans = ''
            textList = []
            splitContent = content.get_text().split("\n")
            # split body content by \n
            for c in splitContent:
                eachLine = c.strip()
            


#Q2()
    
    
# Q2


# In[14]:


print("HI")

