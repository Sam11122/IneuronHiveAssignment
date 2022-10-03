"""
I am using pandas dataframe to fetch the results of sql query into 
it, and then performing the filter and join operation
"""
import puretransport
from pyhive import hive
import pandas as pd

transport= puretransport.transport_factory(host='192.168.56.101',port=10000,username='hive',password='cloudera')
hive_con= hive.connect(username='hive', thrift_transport=transport,database='default')
cur=hive_con.cursor()

cur.execute('select * from product')
products= cur.fetchall()
colsProd= [ i[0].split('.')[1] for i in cur.description ]
dfProd= pd.DataFrame(products, columns=colsProd)
  
print('------------------------------------------')
################
#Filter operation using column productline == Classic Cars
############

classicDF= dfProd[ dfProd['productline'] =='Classic Cars'  ]
print(classicDF) 

cur.execute('select * from sales_orders limit 200')
orders = cur.fetchall()
colsSales= [ i[0].split('.')[1] for i in cur.description ]
dfSales= pd.DataFrame(orders, columns=colsSales)

print('------------------------------------------')
################
#JOIN operation using column productCode 
############

dfSales= dfSales.merge(classicDF, how='inner', on= 'productcode')
print(dfSales[['ordernumber','status','productline','msrp']])


