import pandas as pd
import glob
import os
from itertools import repeat
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import boto3
spark = SparkSession.builder.appName("MukeshAssignment").getOrCreate()
path = "/Users/mukeshmoorthy/stock_analysis/*.csv"
filenames = glob.glob(path)
count = 0
df = {}
while (count < 21):
 df[count] =  pd.read_csv(filenames[count])
 head_tail = os.path.split(filenames[count])
 head = head_tail[1].split('.')[0]
 c = len(df[count])
 if head != 'symbol_metadata':
  Symbol = list(repeat(head_tail[1].split('.')[0], c))
  df[count]['Symbol'] = Symbol
 count = count + 1

i = 0
cdf = {}
while (i < 21):
 cdf[i] = spark.createDataFrame(df[i])
 table_name = f"table_{i}"
 cdf[i].createOrReplaceTempView(table_name)
 i = i+1

sql_query = """Select * from table_0
union all
Select * from table_1
union all
Select * from table_2
union all
Select * from table_3
union all
Select * from table_4
union all
Select * from table_5
union all
Select * from table_6
union all
Select * from table_7
union all
Select * from table_8
union all
Select * from table_9
union all
Select * from table_10
union all
Select * from table_12
union all
Select * from table_13
union all
Select * from table_14
union all
Select * from table_15
union all
Select * from table_16
union all
Select * from table_17
union all
Select * from table_18
union all
Select * from table_19
union all
Select * from table_20
"""
stk = spark.sql(sql_query)

stk.createOrReplaceTempView("stocks")

data = spark.sql("SELECT stocks.timestamp, stocks.open, stocks.high, stocks.low, stocks.close, stocks.volume, t11.Name, t11.Country, t11.Sector, t11.Industry, t11.Address, t11.Symbol FROM table_11 as t11 join stocks on t11.Symbol = stocks.Symbol")

avg_sector = data.groupBy("Sector").agg({"open":"avg", "close":"avg","high":"max","low":"min","volume":"avg"})
print("Report of Average By Sector")
print(avg_sector.show())


df_with_date = data.withColumn("date", to_date(data["timestamp"].cast("Date"), "yyyy-MM-dd"))
avg_date = df_with_date.filter(df_with_date["date"].between("2021-07-05", "2025-09-02")).groupBy("Sector","Symbol","date").agg({"open":"avg", "close":"avg","high":"max","low":"min","volume":"avg"})
print("Report of Average By Date")
print(avg_date.show())
avg_sector.write.csv("/Users/mukeshmoorthy/stock_analysis/avg_sector.csv", header=True)
avg_date.write.csv("/Users/mukeshmoorthy/stock_analysis/avg_date.csv", header=True)


#s3 = boto3.resource('s3')
#s3.Bucket('arn:aws:s3:::mukeshassignment').upload_file('/Users/mukeshmoorthy/stock_analysis/','s3://mukeshassignment')