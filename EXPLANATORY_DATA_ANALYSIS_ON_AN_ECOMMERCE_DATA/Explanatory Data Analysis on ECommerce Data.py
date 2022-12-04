#!/usr/bin/env python
# coding: utf-8

# In[6]:


#necessary imports
import pyspark
import pandas as pd
from pyspark.sql import SparkSession


#spark by examples


# In[7]:


df = pd.read_csv('ecommerce.csv',low_memory = False) #reading the file named ecommerce.csv by using pandas module
print(df)


# In[8]:




columnCount = 0
for j in range(0,len(df.columns)):
    columnCount+=1
    
    
print(df.shape[0])
print(df.shape[1])
print(df.shape[0],df.shape[1])   
print(len(df)) #row count of the dataset
print(columnCount) #column count of the dataset
print("There are "+str(len(df))+" rows in the dataset.")
print("There are "+str(columnCount)+" columns in the dataset.")


# In[9]:


print(df.columns)


# In[10]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType, DateType

schema = StructType([
StructField("Order ID", StringType()),
StructField("Order Date", StringType()),
StructField("Ship Date", StringType()),
StructField("Aging", DoubleType()),
StructField("Ship Mode", StringType()),
StructField("Product Category", StringType()),
StructField("Product", StringType()),
StructField("Sales", StringType()),
StructField("Quantity", IntegerType()),
StructField("Discount", DoubleType()),
StructField("Profit", StringType()),
StructField("Shipping Cost", StringType()),
StructField("Order Priority", StringType()),
StructField("Customer ID", StringType()),
StructField("Customer Name", StringType()),
StructField("Segment", StringType()),
StructField("City", StringType()),
StructField("State", StringType()),
StructField("Country", StringType()),
 StructField("Region", StringType()),
  StructField("Months", StringType()),
])


spark = SparkSession.builder.appName('EcommerceDataAnalysis').getOrCreate() 
ecommerceDataFrame = spark.read.csv('ecommerce.csv', header = True, schema = schema) 


# In[11]:


ecommerceDataFrame.printSchema()
ecommerceDataFrame.show()


# In[12]:


ecommerceDataFrame.head(4) #Brings upmost 4 row


# In[13]:


ecommerceDataFrame.printSchema() #displaying the schema of the data frame, and name & data type of each column in the data frame. 


# In[ ]:





# In[14]:


print("Data row count:    " + str(ecommerceDataFrame.count()))
print("Data column count: " + str(len(ecommerceDataFrame.columns)))
ecommerceDataFrame.printSchema() 


# In[15]:


ecommerceDataFrame.groupBy("Profit").count().show()


# In[16]:


ecommerceDataFrame.columns


# In[17]:


ecommerceDataFrame = ecommerceDataFrame.na.fill('FA-2015-100000',subset=['Order ID'])
ecommerceDataFrame = ecommerceDataFrame.fillna('12/25/15', subset=['Order Date'])
ecommerceDataFrame = ecommerceDataFrame.na.fill('12/29/15',subset=['Order Date'])
ecommerceDataFrame = ecommerceDataFrame.na.fill(5.0,'Aging')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Standard Class','Ship Mode')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Fashion','Product Category')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Sports Wear','Product')
ecommerceDataFrame = ecommerceDataFrame.na.fill('$180.00','Sales')
ecommerceDataFrame = ecommerceDataFrame.na.fill(3,'Quantity')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Alex Souza','Customer Name')
ecommerceDataFrame = ecommerceDataFrame.na.fill('25000','Customer ID')
ecommerceDataFrame = ecommerceDataFrame.na.fill(0.05,'Discount')
ecommerceDataFrame = ecommerceDataFrame.na.fill('$54.00','Profit')
ecommerceDataFrame = ecommerceDataFrame.na.fill('$180.00','Shipping Cost')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Home Office','Segment')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Africa','Region')
ecommerceDataFrame = ecommerceDataFrame.na.fill('Medium','Order Priority')



df["Order ID"].fillna("FA-2015-100000", inplace = True)
df["Order Date"].fillna("12/31/15", inplace = True)
df["Ship Date"].fillna("12/29/15", inplace = True)
df["Aging"].fillna(5.0, inplace = True)
df["Ship Mode"].fillna("Standard Class", inplace = True)
df["Product Category"].fillna("Fashion", inplace = True)
df["Product"].fillna("Sports Wear", inplace = True)
df["Sales"].fillna("$180.00", inplace = True)
df["Quantity"].fillna(3, inplace = True)
df["Customer Name"].fillna("Alex Souza", inplace = True)
df["Customer ID"].fillna("25000", inplace = True)
df["Discount"].fillna(0.05, inplace = True)
df["Profit"].fillna("$54.00", inplace = True)
df["Shipping Cost"].fillna("$180.00", inplace = True)
df["Segment"].fillna("Home Office", inplace = True)
df["Region"].fillna("Africa", inplace = True)
df["Order Priority"].fillna("Medium", inplace = True)

df["Region"].replace({"4orth": "North", "So3th": "South"}, inplace=True)
df["Ship Mode"].replace({"45788": "First Class"}, inplace=True)
df["Quantity"].replace({"Standard Class": 1, "abc": 2}, inplace=True)
df["Discount"].replace({"xxx": 0.05}, inplace=True)


# In[18]:


from pyspark.sql import functions as F
ecommerceDataFrame = ecommerceDataFrame.withColumn('Region',
    F.when(ecommerceDataFrame['Region']=='4orth','North').
    otherwise(ecommerceDataFrame['Region']))

ecommerceDataFrame = ecommerceDataFrame.withColumn('Region',
    F.when(ecommerceDataFrame['Region']=='So3th','South').
    otherwise(ecommerceDataFrame['Region']))

ecommerceDataFrame.groupBy('Region').count().show() 


# In[19]:


ecommerceDataFrame.columns
ecommerceDataFrame.printSchema()


# In[20]:


ecommerceDataFrame.groupBy('Order Date').count().show() 
ecommerceDataFrame.groupBy('Ship Date').count().show() 
ecommerceDataFrame.groupBy('Aging').count().show() 

ecommerceDataFrame = ecommerceDataFrame.withColumn('Ship Mode',
    F.when(ecommerceDataFrame['Ship Mode']=='45788','First Class').
    otherwise(ecommerceDataFrame['Ship Mode']))
ecommerceDataFrame.groupBy('Ship Mode').count().show() 


ecommerceDataFrame.groupBy('Product Category').count().show() 
ecommerceDataFrame.groupBy('Product').count().show() 
ecommerceDataFrame.groupBy('Sales').count().show() 

ecommerceDataFrame = ecommerceDataFrame.withColumn('Quantity',
    F.when(ecommerceDataFrame['Quantity']=='Standard Class',1).
    otherwise(ecommerceDataFrame['Quantity']))

ecommerceDataFrame = ecommerceDataFrame.withColumn('Quantity',
    F.when(ecommerceDataFrame['Quantity']=='abc',2).
    otherwise(ecommerceDataFrame['Quantity']))

ecommerceDataFrame.groupBy('Quantity').count().show() 

ecommerceDataFrame = ecommerceDataFrame.withColumn('Discount',
    F.when(ecommerceDataFrame['Discount']=='xxx',0.05).
    otherwise(ecommerceDataFrame['Discount']))

ecommerceDataFrame.groupBy('Discount').count().show() 





ecommerceDataFrame.groupBy('Profit').count().show() 
ecommerceDataFrame.groupBy('Shipping Cost').count().show() 
ecommerceDataFrame.groupBy('Order Priority').count().show() 
ecommerceDataFrame.groupBy('Customer ID').count().show() 


# In[21]:


ecommerceDataFrame.groupBy('Customer Name').count().show() 
ecommerceDataFrame.groupBy('Segment').count().show() 
ecommerceDataFrame.groupBy('City').count().show() 
ecommerceDataFrame.groupBy('State').count().show()
ecommerceDataFrame.groupBy('Country').count().show() 
ecommerceDataFrame.groupBy('Region').count().show() 
ecommerceDataFrame.groupBy('Months').count().show() 


# In[30]:


##########Problem1#############################################################
#Region
#Product
#Quantity, Rank

from pyspark.sql.functions import sum,col
import pyspark.sql.functions as func
from pyspark.sql.functions import first,last

print("Problem 1 :")
my_initial_list = ecommerceDataFrame.select("Region").rdd.flatMap(lambda x: x).collect()
my_second_list = ecommerceDataFrame.select("Product").rdd.flatMap(lambda x: x).collect()
my_third_list = ecommerceDataFrame.select("Quantity").rdd.flatMap(lambda x: x).collect()
l = []
for i in range(0,len(my_initial_list)):
    lis = []
    lis.append(my_initial_list[i]);
    lis.append(my_second_list[i]);
    lis.append(my_third_list[i]);
    l.append(lis)
    
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number

d = spark.createDataFrame(l,["Region","Product","Quantity"])
partition  = Window.partitionBy("Region")
wind = partition.orderBy(sum("Quantity"))
a = d.groupBy(["Region"]).agg(sum("Quantity").alias("Quantity_Sum"),rank().over(wind).alias("rank")).sort(col('Quantity_Sum').desc())
a.show()
print(a.collect()[0]) #show the maximum sold products.
a.show(1)

##########Problem1#############################################################

#En çok satış hacmi hangi bölgede ? (Central)

#Bölge Adı, Satış Hacmi




import matplotlib.pyplot as plt



quantity_sum_list = a.select("Quantity_Sum").rdd.flatMap(lambda x: x).collect()
regions_list = ["Cen","Sou","Emea","Nor","Afr","Oce","Wes","Sout","East","Nor As","Cent As.","Car.","Can."]
plt.plot(regions_list, quantity_sum_list)
plt.xlabel("Region")
plt.ylabel("Quantity_Sum")
plt.title("Total quantities per region")
plt.show()




# In[23]:


##################PROBLEM-2 Solution#########################################
ecommerceDataFrame.groupBy('Segment','Months').sum('Quantity').show()
############################Problem-2 Solution#########################################

#Segment bazlı aylık toplam satış miktarı


# In[67]:


##########Problem3 Solution#############################################################
#Region
#Product
#Quantity, Rank

from pyspark.sql.functions import sum,col
import pyspark.sql.functions as func

print("Problem 3: ")
my_initial_list = ecommerceDataFrame.select("Region").rdd.flatMap(lambda x: x).collect()
my_second_list = ecommerceDataFrame.select("Product").rdd.flatMap(lambda x: x).collect()
my_third_list = ecommerceDataFrame.select("Quantity").rdd.flatMap(lambda x: x).collect()
l = []
for i in range(0,len(my_initial_list)):
    lis = []
    lis.append(my_initial_list[i]);
    lis.append(my_second_list[i]);
    lis.append(my_third_list[i]);
    l.append(lis)
    
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number

d = spark.createDataFrame(l,["Region","Product","Quantity"])
partition  = Window.partitionBy("Region")
wind = partition.orderBy(sum("Quantity").desc())

q = d.groupBy(["Region","Product"]).agg(sum("Quantity").alias("Quantity_Sum"),row_number().over(wind).alias("rank")).filter(col("rank")<=3)
q.show()
print(q.columns)
##########Problem3 Solution#############################################################
#her bölgede satılan top3 ürün













#plt.plot(lst3,lst4)

#plt.title("Total quantities for each product")

#plt.xlabel("Products")

#plt.ylabel("Quantities")

#plt.show()

############Plotting monthly customer numbers ########################



# In[71]:



###########################Problem 4 Solution####################################################
#2015 yılında hangi ayda en çok yeni müşteri gelmiş.
ecommerceDataFrame.groupBy('Order Date').count().show()

#date'e çevir where'e 2015 yazcaz.
#customer idleri count et.
#groupbya ayları yaz.
#maxı dön.

from pyspark.sql.functions import split,col,desc
#ecommerceDataFrame.withColumn("Yearssss", split(col("Order Date"), "/").getItem(2)).groupBy("Yearssss").count().show()
#ecommerceDataFrame.withColumn("Yearssss", split(col("Order Date"), "/").getItem(2)).select("Yearssss").show()
ecommerceDataFrame.groupBy("Months").count().show()

#ecommerceDataFrame.withColumn("Yearssss", split(col("Order Date"), "/")
#.getItem(2)).withColumn("Monthssss",split(col("Order Date"), "/").getItem(0)).filter("Yearssss=15").groupBy(["Monthssss"]).count().sort(col("count").desc()).show(1)

from pyspark.sql.functions import *

my_fir_lst = ecommerceDataFrame.select("Order Date").rdd.flatMap(lambda x: x).collect()
my_sec_lst = ecommerceDataFrame.select("Customer ID").rdd.flatMap(lambda x: x).collect()
my_thir_lst = ecommerceDataFrame.select("Months").rdd.flatMap(lambda x: x).collect()

l = []
for i in range(0,len(my_fir_lst)):
    lis = []
    lis.append(my_fir_lst[i])
    lis.append(my_sec_lst[i])
    lis.append(my_thir_lst[i])
    l.append(lis)
    

    
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number, date_trunc,sum,unix_timestamp


    
dff = spark.createDataFrame(l,["Order Date","Customer ID","Months"])
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id


windowSpec = Window.partitionBy(monotonically_increasing_id()>=0).orderBy(monotonically_increasing_id())
#windowSpec = Window.partitionBy(col("Customer ID")).orderBy("Order Date")

dff.show(truncate = False)
dff = dff.groupBy(["Months"]).agg(count(col("Customer ID")).alias("Monthly_Customer_Number"))
dff = dff.sort(unix_timestamp(col("Months"),"MMM"))
dff = dff.withColumn("Previous_Customer_Number", F.lag(dff["Monthly_Customer_Number"],1).over(windowSpec))
dff = dff.withColumn("Customer_Difference", F.when(F.isnull(dff["Monthly_Customer_Number"] - dff.Previous_Customer_Number), 0)
                              .otherwise(dff["Monthly_Customer_Number"] - dff.Previous_Customer_Number))

dff.sort(col("Customer_Difference").desc()).show(1)
##############################Problem 4 Solution#############################################


############Plotting monthly customer numbers ########################

import matplotlib.pyplot as plt

lst1 = dff.select("Months").rdd.flatMap(lambda x: x).collect()


lst2 = dff.select("Monthly_Customer_Number").rdd.flatMap(lambda x: x).collect()

plt.plot(lst1,lst2)

plt.title("Monthly Customer Numbers")

plt.xlabel("Months")

plt.ylabel("Customer Amounts")

plt.show()

############Plotting monthly customer numbers ########################





    



# In[76]:


from pyspark.sql.functions import col, unix_timestamp, to_date, lag
from pyspark.sql.functions import *

#2015 yılında hangi ayda en çok yeni müşteri gelmiş.

spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")   



#d = d.withColumn("Yearssss", split(col("Order Date"), "/").getItem(2)).#
#.withColumn("Yearssss", split(col("Order Date"), "/").getItem(2)).#


#Kargolama bütçesinde açık olduğunu fark ettik, planlanan bütçeden daha fazlası harcanmış. Bunun nedenini bulabilir
#misiniz?


my_Data_Set = ecommerceDataFrame.groupBy('Ship Mode').count()
my_Data_Set.show() #%20'si first class + same day, bütçe tasarrufu için first class ve same day ship sayıları azaltılabilir.




##########Plotting ship mode counts per ship mode##########################
import matplotlib.pyplot as plt
my_fourth_list = ecommerceDataFrame.select("Ship Mode").rdd.flatMap(lambda x: x).collect()
b = []
for j in range(0,len(my_fourth_list)):
    a = []
    a.append(my_fourth_list[j])
    b.append(a)
    
dfff = spark.createDataFrame(b,["Ship Mode"])

dfff = dfff.groupBy(col("Ship Mode")).agg(count(col("Ship Mode")).alias("Ship_Mode_Count"))

firstLst = dfff.select("Ship Mode").rdd.flatMap(lambda x: x).collect()
secondLst = dfff.select("Ship_Mode_Count").rdd.flatMap(lambda x: x).collect()

plt.plot(firstLst,secondLst)
plt.title("Total count per ship mode")
plt.show()

#bar , pie charta çevir. 


# In[27]:




#######Problem-5 Solution#############################################################################
###Description: For each country, find the state names where the total sold product amount is the least for that state.
#Region
#Product
#Quantity, Rank

from pyspark.sql.functions import sum,col
import pyspark.sql.functions as func

my_initial_list = ecommerceDataFrame.select("Country").rdd.flatMap(lambda x: x).collect()
my_second_list = ecommerceDataFrame.select("State").rdd.flatMap(lambda x: x).collect()
my_third_list = ecommerceDataFrame.select("Product").rdd.flatMap(lambda x: x).collect()
my_fourth_list = ecommerceDataFrame.select("Quantity").rdd.flatMap(lambda x: x).collect()
l = []
for i in range(0,len(my_initial_list)):
    lis = []
    lis.append(my_initial_list[i]);
    lis.append(my_second_list[i]);
    lis.append(my_third_list[i]);
    lis.append(my_fourth_list[i]);
    l.append(lis)
    
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number



d = spark.createDataFrame(l,["Country","State","Product","Quantity"])
partition  = Window.partitionBy("Country")
wind = partition.orderBy(sum("Quantity").asc())
d.groupBy(["Country","State"]).agg(rank().over(wind).alias("rank")).filter(col("rank")==1).drop(col("rank")).show()

#######Problem-5 Solution###############################################################################


# In[87]:


#Plotting Country Count per Region
import matplotlib.pyplot as plt

h = []
fLis = ecommerceDataFrame.select("Region").rdd.flatMap(lambda x: x).collect()
sLis = ecommerceDataFrame.select("Country").rdd.flatMap(lambda x: x).collect()
for x in range(0,len(fLis)):
    i = []
    i.append(fLis[x])
    i.append(sLis[x])
    h.append(i)
    
my_dt = spark.createDataFrame(h,["Region","Country"])

my_dt = my_dt.groupBy([col("Region")]).agg(count(col("Country")).alias("Country_Amount_Per_Region"))

firstLst = ["Cen","Sou","Emea","Nor","Afr","Oce","Wes","Sout","East","Nor As","Cent As.","Car.","Can."]
secondLst = my_dt.select("Country_Amount_Per_Region").rdd.flatMap(lambda x: x).collect()
print(secondLst)
plt.plot(firstLst,secondLst)
plt.title("Country amount per region")

plt.show()


# In[ ]:





# In[ ]:





# In[28]:


import matplotlib.pyplot as plt

my_lst = ecommerceDataFrame.select("Quantity").rdd.flatMap(lambda x: x).collect()
my_second_lst = ecommerceDataFrame.select("Discount").rdd.flatMap(lambda x: x).collect()
plt.plot(my_lst,my_second_lst)
plt.xlabel('Quantity')
plt.ylabel('Discount')
plt.title("Quantity vs Discount")
plt.show()


my_ls = ecommerceDataFrame.select("Aging").rdd.flatMap(lambda x: x).collect()
my_second_ls = ecommerceDataFrame.select("Discount").rdd.flatMap(lambda x: x).collect()
plt.plot(my_ls,my_second_ls)
plt.xlabel('Aging')
plt.ylabel('Discount')
plt.title("Aging vs Discount")
plt.show()

myList = ecommerceDataFrame.select("Quantity").rdd.flatMap(lambda x: x).collect()
mySecondList= ecommerceDataFrame.select("Aging").rdd.flatMap(lambda x: x).collect()
plt.plot(myList,mySecondList)
plt.xlabel('Quantity')
plt.ylabel('Aging')
plt.title("Quantity vs Aging")
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:






# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




