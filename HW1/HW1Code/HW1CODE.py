
# coding: utf-8

# In[238]:


import pandas as ad
import csv

# read HW1 data by using spark context
textFile = sc.textFile("file:/home/ethan/pythonwork/ipynotebook/HW1/HW1Data/household_power_consumption.txt")


# In[239]:


# split data by spark mapping
splitedData = textFile.map ( lambda line : line.split (";"))


# In[240]:


# attribute setting
attr = ["Global_active_power","Global_reactive_power","Voltage","Global_intensity"]
# normalization formula
def normalization(x):
    max = x.max()
    min = x.min()
    ans = x.map(lambda x: (x-min)/(max-min))
    return ans


# In[241]:


# retrivel useful attribute data then calculate minimun, maximum,and column number
filename = "HW1.txt"
with open(filename, 'a') as out:
    for i in range(2,6):
        columnData = splitedData.map(lambda attr:attr[i]).filter(lambda x:x!=attr[i-2] and x!="?")
        floatData = columnData.map(lambda x:float(x))
        out.write(attr[i-2]+":\n"+str(floatData.stats())+"\n"+"normalization:"+str(normalization(floatData).collect())+"\n")
print("HW1 create success!")


# In[ ]:



    

