
# coding: utf-8

# In[139]:


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


# In[140]:


# parsing data from sgm file

from bs4 import BeautifulSoup,SoupStrainer


def readData(fileName):
    data = ''
    f = open(fileName, 'rb')
    soup = BeautifulSoup(f, 'html.parser')
    # get body content
    contents = soup.findAll('body')
    return contents

def reExp(d):
    # remove header space and footer space
    sentence = d.strip()
    # regular expresisson
    pat = '[a-zA-Z]+'
    reContent = re.findall(pat, sentence)
    return reContent

# parsing data from sgm file


# In[141]:


# Q1

# ==== initial setting ====
sTime = time.time()
file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
bodyLen, shingleLen = 0, 0
# storage for term
wordSet = set()
# storage for term Frequency
wordList = []
# ==== initial setting ====

# process all data set
print("Start count word Frequency")
for i in range(len(file)):
    fileName = "/home/ethan/pythonwork/ipynotebook/HW4/Data/reut2-"
    fileName = fileName + file[i] + ".sgm"
    # read body content from each file
    data = readData(fileName)
    # retrival all body content
    for content in data:
        if i < 2:
            bodyLen +=1
        # split one body content by \n
        for d in content.get_text().split("\n"):
            # regular expresisson process
            reContent = reExp(d)
            # select one word on each time
            for ind in range(len(reContent)):
                # add word
                wordSet.add(reContent[ind])
    print("file ", i, " done")

# for speed up word count matrix
for w in wordSet:
    wordList.append(w)
wordList.sort()
# release memory
del wordSet
wordLen = len(wordList)

print("Count word Frequency done\n")
print("Total body number: %d" % bodyLen)
print("Total word number: %d" % wordLen)
print("Count word Frequency took: %.2fs" % (time.time()-sTime))

# Q1


# In[142]:


# Q1

# ==== initial setting ====

sTime = time.time()
file = ["000","001","002","003","004","005","006","007","008","009","010","011","012","013","014","015","016","017","018","019","020","021"]
matchNum = 0
col = 0
MxN_matrix = np.zeros((wordLen,bodyLen), int)
totalWord = 0
# ==== initial setting ====

# process specify document
print("Start calculate word Frequency matrix")
for i in range(2):
    fileName = "/home/ethan/pythonwork/ipynotebook/HW3/Data/reut2-"
    fileName = fileName + file[i] + ".sgm"
    print(fileName +" start!")
    # read body content from each file
    data = readData(fileName)
    contInd = 0
    for content in data:
        # save total term frequency on one body content
        bContWordDict = {}
        # split one body content by \n
        for d in content.get_text().split("\n"):
            # regular expresisson process
            reContent = reExp(d)
            # select one word on each time
            for r in range(len(reContent)):
                if reContent[r] not in bContWordDict:
                    bContWordDict[reContent[r]] = 1
                else:
                    bContWordDict[reContent[r]] +=1
        # sort result in alphabetical order
        orderRes = OrderedDict(sorted(bContWordDict.items(), key=lambda x: x[0]))
        totalWord += len(orderRes)
        # process all term frequency on each body content
        row = 0
        for w in range(wordLen):
            try:
                wordWithCount = list(orderRes.items())[row]
            except IndexError:
                print(row)
            # find out term appeared location, then append value to matrix
            if wordWithCount[0] == wordList[w]:
                MxN_matrix[w][col] = wordWithCount[1]
                matchNum += 1
                row +=1
            # when visit all term on one body content then break this loop
            if row == len(orderRes):
                break
        col +=1
        contInd +=1
        if contInd %100 == 0:
            print(contInd, " done!")
        
print("Calculate word Frequency matrix done")             
print("total Word num %d" % totalWord)
print("total match num %d" % matchNum)
print("Calculate word Frequency matrix took %.2fs" % (time.time()-sTime))

# save Q1 Result
# sTime = time.time()
# np.savetxt("Q1_Result.csv", MxN_matrix, delimiter=",")
# print("Save word Frequency matrix took %.2f" % (time.time()-sTime))



# Q1


# In[143]:


# Q2

# ==== initial setting ====
sTime = time.time()
NxR_matrix = np.random.randint(1,5,[bodyLen,1])
# ==== initial setting ====

print("Start process matrix multiplication!")

MxR_matrix = np.dot(MxN_matrix, NxR_matrix)
        
print("Process matrix multiplication done!")
print("MxR_Matrix multiplication took %.2f" % (time.time()-sTime))

# save Q2 Result
# sTime = time.time()
# np.savetxt("Q2_Result.csv", MxR_matrix, delimiter=",")
# print("Save MxR_Matrix took %.2fs" % (time.time()-sTime))


# Q2


# In[144]:



# Q3

# ==== initial setting ====
sTime = time.time()
# ==== initial setting ====

print("Start process singular value decomposition!")

#U, S, VT = np.linalg.svd(MxR_matrix, full_matrices = False)
U, S, VT = np.linalg.svd(MxN_matrix, full_matrices = False)
        
print("Process singular value decomposition done!")
print("singular value decomposition took %.2f" % (time.time()-sTime))

# save Q3 Result
sTime = time.time()
np.savetxt("Q3_U.csv", U, delimiter=",")
np.savetxt("Q3_S.csv", S, delimiter=",")
np.savetxt("Q3_VT.csv", VT, delimiter=",")

print("Save singular value decomposition took %.2fs" % (time.time()-sTime))


# Q3




