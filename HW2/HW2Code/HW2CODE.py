
# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np
import math
import csv
import re
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions 
from pyspark.sql import Row
from collections import OrderedDict



# In[24]:


# Q1 #

# Des : In news data, count the words in two fields: ‘Title’ and ‘Headline’ respectively, 
# and list the most frequent words according to the term frequency in descending order, 
# in total, per day, and per topic, respectively

# input data by using sqlContext, split data by comma, and use header as dataFrame column name
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', sep=',').load("file:/home/ethan/pythonwork/ipynotebook/HW2/HW2Data/News_Final.csv")


def regAndKeyValue(collectData):
    # regular expression setting
    # "[a-zA-Z]+"
    # key, value count 
    wordCount = {}
    orderResult = {}
    Data = collectData.collect()
    # retrivel all data from Title column
    for i in range(len(collectData.collect())):
        # split data by regular expression
        ans = re.findall('[a-zA-z]+', str(Data[i][0]))
        # key, value calculate
        for w in ans:
            if w not in wordCount:
                wordCount[w] = 1
            else:
                wordCount[w] += 1
    #print(wordCount)
    orderResult = OrderedDict(sorted(wordCount.items(), key=lambda t: t[1], reverse=True))
    return orderResult
    #print(orderResult)
    print(len(orderResult))
    print("Done\n")


# calculate Title and Headline Total key, value
def total():
    
    print("\t****** Starting processing total func ******")
    
    # get column data from Titla column
    columnData = ['Title','Headline']
    
    for c in columnData:
        # write result to txt
        filename = "Q1_total_" + c + ".txt"
        with open(filename, 'a') as out: 
            title = df.select([c])
            result = regAndKeyValue(title)
            out.write(json.dumps(result))

    print("\t****** Processing total func Done!******\n\n")

    
# calculate Title and Headline key, value by using topic
def topic():
    
    print("\t****** Starting processing topic func ******")
    
    topic = ["obama","economy","microsoft","palestine"]
    # get column data from Titla column
    columnData = ['Title','Headline']
    # retrivel columnData by four type topic
    for c in columnData:
        for i in range(4):
            # write result to txt
            filename = "Q1_perTopic_" + c + "_" + topic[i] + ".txt"
            with open(filename, 'a') as out:
                # filter data by topic and extract column data
                topicData = df.filter(df['Topic']==topic[i]).select([c])
                result = regAndKeyValue(topicData)
                out.write(json.dumps(result))
    print("\t****** Processing topic func Done!******\n\n")

    
# calculate Title and Headline key, value by using PublishDate
def publishDate():
    
    print("\t****** Starting processing day func ******\n")
    # get column data from Titla column
    columnData = ['Title','Headline']
    date = df.select(['PublishDate']).collect()
    flag = ""
    wordCount = {}
    orderResult = {}
    # retrivel columnData by publishDate
    for c in columnData:
        # get column data
        data = df.select([c]).collect()
        # retrivel publishDate column
        for d in range(len(data)):
            # split publishData string by " "
            day = date[d][0].split(" ")
            # avoid error input
            if len(day)==2:
                # split data by regular expression
                ans = re.findall('[a-zA-z]+', str(data[d][0]))
                # key, value calculate
                for w in ans:
                    if w not in wordCount:
                        wordCount[w] = 1
                    else:
                        wordCount[w] += 1            
                # when new date apeear then initital count dict
                if flag != day[0]:
                    # sorting result
                    orderResult = OrderedDict(sorted(wordCount.items(), key=lambda t: t[1], reverse=True))
                    flag = day[0]
                    wordCount = {}
                    # write result to txt
                    filename = "Q1_perDay_" + c + ".txt"
                    with open(filename, 'a') as out:
                        out.write(flag + "\n" + json.dumps(orderResult) + "\n")
                    orderResult = {}
                    
            #print(c,count," processing done!\n")
                    
    print("\t****** Processing day func Done!******\n\n")
    
    
total()
topic()
publishDate()

print("\nQ1 done!\n")

# # Q1 #


# In[25]:


# Q2 Done#

Facebook = ["Facebook_Economy","Facebook_Microsoft","Facebook_Obama","Facebook_Palestine"]
Google = ["GooglePlus_Economy","GooglePlus_Microsoft","GooglePlus_Palestine"]
Linked = ["LinkedIn_Economy","LinkedIn_Microsoft","LinkedIn_Obama","LinkedIn_Palestine"]

# processing Q2 request
def process(file):
    for f in file:
        print("Start processing "+ f)
        # input data from csv by using spark context
        rdd = sc.textFile("file:/home/ethan/pythonwork/ipynotebook/HW2/HW2Data/"+f+".csv")
        # remove header
        header = rdd.first()
        hrdd = rdd.filter(lambda x:x!= header)
        
        # retrivel all dataset, each time calculate one row
        for i in hrdd.take(rdd.count()):
            ID =0  # for remove IDLink
            perDaySum = 0  # for sum score
            # split row data by split comma
            for j in i.split(","):
                if ID !=0:
                    perDaySum +=float(j)
                else:
                    ID = float(j)
                    
            perDaySum = perDaySum - ID
            
        # write result to txt
        filename = "Q2_calAvePopularity_" + f + ".txt"
        with open(filename, 'a') as out:
            out.write("aveByDay: " + str(perDaySum/2) + "\n" + "aveByHour: " + str(perDaySum/48))
            
    print("Processing " + f + " done !")
        
process(Facebook)
process(Google)
process(Linked)

print("\nQ2 done!\n")

# Q2 #


# In[ ]:


# Q3 #
topic = ["obama","economy","microsoft","palestine"]


# input data by using sqlContext, split data by comma, and use header as dataFrame column name
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', sep=',').load("file:/home/ethan/pythonwork/ipynotebook/HW2/HW2Data/News_Final.csv")


def Q3():
    # retrivel four topic
    for i in range(4):
        print("\t******",topic[i],"******","\n")
        # calculate total score under select topic
        print("Start collect!")
        ans = df.filter(df['Topic']==topic[i]).select(['SentimentTitle']).collect()
        print("Collect done!")
        totalScore = 0
        for j in range(df.filter(df['Topic']==topic[i]).count()):
            totalScore += float(ans[j][0])
        # calculate topic number
        topicCount = df.filter(df['Topic']==topic[i]).count()
        # write result to txt
        filename = "Q3_calSentimentScore_" + topic[i] + ".txt"
        with open(filename, 'a') as out:
            out.write("aveScore: " + str(totalScore/topicCount) + "\nTotalScore: " + str(totalScore))
        print("\t******",topic[i],"******\n\n")

Q3()
        
# Q3 #

    
print("\nQ3 done!\n")



# In[ ]:


# Q4 #

# input data by using sqlContext, split data by comma, and use header as dataFrame column name
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', sep=',').load("file:/home/ethan/pythonwork/ipynotebook/HW2/HW2Data/News_Final.csv")

# calculate termFrequency            
def termFrequency(Data):
    # save term frequency and order result
    wordCount = {}
    orderResult = {}
    # retrivel filter data
    for i in range(len(Data)):
        # split data by regular expression
        rowData = re.findall('[a-zA-z]+', str(Data[i][0]))
        #key, value calculate
        for w in rowData:
            if w not in wordCount:
                wordCount[w] = 1
            else:
                wordCount[w] += 1
    # order term frequency
    orderResult = OrderedDict(sorted(wordCount.items(), key=lambda t: t[1], reverse=True))
    return orderResult
    
# calculate co-occurrence
def coOccurrence(termItem, Data, filename):
    # initial co-occurrence matrix by using numpy
    coMatrix = np.zeros((100,100),int)
    # retrivel filter data
    for i in range(len(Data)):
        # split data by regular expression
        rowData = re.findall('[a-zA-z]+', str(Data[i][0]))
        # retrivel order term frequency list
        for t in range(100):
            # retrivel each row data
            if termItem[t] in rowData:
                # retirvel other keyword from order list
                for w in range(100):
                    # avoid compare to keyword itself, and calculate two keyword appear in rowData
                    if termItem[w] in rowData and termItem[w] is not termItem[t]:
                        coMatrix[t][w] += 1
    # write result to txt
    with open(filename, 'a') as out:
        out.write("   ")
        for j in range(100):
            out.write(termItem[j] + " ")
        out.write("\n")
        for i in range(100):
            out.write(termItem[i] + " ")
            for k in range(100):
                out.write(str(coMatrix[i][k]) + " ")
            out.write("\n")
    
# get topic
topic = ["obama","economy","microsoft","palestine"]
# get columnData
columnData = ['Title','Headline']

# main func
def main():
    for c in columnData:
        for t in range(4):
            print(c, "start processing task ",t)
            # filter data by using topic
            Data = df.filter(df['Topic']==topic[t]).select([c]).collect()
            # get ordered term frequency <key,value> 
            orderTermFrequency = termFrequency(Data)
            # transfer OrderedDict to list
            termItem = list(orderTermFrequency)
            # write result to txt
            filename = "Q4_co-occurence_" + c + "_" + topic[t] + ".txt"
            # process co-occurrence matrix
            coOccurrence(termItem, Data, filename)
            
            print(c,"task ",t," processing done!")
            
# program start point
main()

print("\nQ4 done!\n")

# Q4 #

