import pandas as pd
import numpy as np
import pymysql
from datetime import timedelta,datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
start_date = (datetime.now() + timedelta(minutes=330)).strftime('%Y-%m-1 00:00:00')
end_date = (datetime.now() + timedelta(minutes=330)).strftime('%Y-%m-%d 23:59:59')


#State Mapping Data Frame
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file(r"D:\python_projects\MySql\pythonProject1\client_secrets2.json", scopes=scopes)
gc = gspread.authorize(credentials)
spreadsheet1 = gc.open_by_key('1sTgEUiDUM5sE5mroWl-f7qVTFOXu_TSbnYoaYTy5VB0')
worksheet_title1 = "State"
worksheet = spreadsheet1.worksheet(worksheet_title1)
print(worksheet.col_values(2)[1:])

state_mapping = pd.DataFrame(list(zip(worksheet.col_values(1)[1:],worksheet.col_values(2)[1:])),columns=['state','state_name'])
#Call B2B Database for invoice Mapping
print(start_date,end_date)
analyticsdb = pymysql.connect(host="prod-b2b-vellvette.cluster-czbh9byla2ok.ap-south-1.rds.amazonaws.com", user="navdeepm", passwd="I72jAajBRwbZyITKuTYJ", database="sugardb")
sql_query = f"Select internal_id,invoice_date,customer,tax_amount from invoice where invoice.invoice_date between '{start_date}' and '{end_date}'"
try:
    # analytics.execute(sql_query)
    invoice = pd.read_sql(sql_query, analyticsdb)
    # analyticsdb.close()
except:
    analyticsdb.close()
# Invoice Item
internal_id = ','.join(invoice['internal_id'].astype('str'))

sql_query = f"Select invoice as 'internal_id',qty,rate,amount,item from invoice_items where invoice in ({internal_id}) "
try:
    # analytics.execute(sql_query)
    invoice_item = pd.read_sql(sql_query, analyticsdb)
    # analyticsdb.close()
except:
    analyticsdb.close()
#SKU Details
item = ','.join(invoice_item['item'].astype('str').unique())
sql_query = f"Select internal_id as 'item',sku from product_master where internal_id in ({item}) "
try:
    # analytics.execute(sql_query)
    product_sku = pd.read_sql(sql_query, analyticsdb)
    # analyticsdb.close()
except:
    analyticsdb.close()

#Customer Details
customer = ','.join(invoice['customer'].astype('str').unique())
sql_query = f"Select internal_id as 'customer',customer_group,state from customer_master where internal_id in ({customer}) "
try:
    # analytics.execute(sql_query)
    customer_master = pd.read_sql(sql_query, analyticsdb)
except:
    analyticsdb.close()
#Merge Customer Master with Invoice
invoice = pd.merge(invoice,customer_master,on='customer')
del customer_master
#Merge item with sku
invoice_item = pd.merge(invoice_item,product_sku,on='item')
del product_sku
#Merge invoice and item
invoice = pd.merge(invoice,invoice_item,on='internal_id')
del invoice_item
 # Split - in SKUs
invoice[['sku', 'second']] = invoice['sku'].str.split('-', n=1, expand=True)
invoice.drop('second',inplace=True,axis=1)
invoice['sku'] = invoice['sku'].astype('str')
invoice['gross_amount'] = invoice['amount'] + round((invoice['amount']*18)/100,2)
invoice['order_date'] = pd.to_datetime(invoice['invoice_date']).dt.strftime('%Y-%m-%d')
invoice = invoice.groupby(['invoice_date','customer_group','sku','state'])[['gross_amount','qty']].sum().reset_index()

invoice = pd.merge(invoice,state_mapping,on='state',how='left').drop('state',axis=1)

#CAll EBO Database
analyticsdb = pymysql.connect(host='prod-sugar-retail.cluster-cifgtkd4jlx0.ap-south-1.rds.amazonaws.com',user='navdeep',passwd='UW4TgUeFlPpOdUv2',database='sugar_retail_management')
sql_query = f"Select order_id as 'id',store_id,order_date  From `sugar_retail_management`.retail_orders where order_date between  '{start_date}' and '{end_date}' and order_id not regexp 'ECM|_SR_'"
# analytics.execute(sql_query)
retail_orders = pd.read_sql(sql_query,analyticsdb)

r_order1 = list(retail_orders['id'].astype('str').unique())
r_order1 = "','".join(r_order1)
r_order1 = "'" + r_order1 + "'"

r_order3 = list(retail_orders['store_id'].astype('str').unique())
r_order3 = ",".join(r_order3)
#State
#retail_store_details
sql_query = f"Select online_store_id as 'store_id' ,state From `sugar_retail_management`.retail_stores where online_store_id in ({r_order3})"
# analytics.execute(sql_query)
retail_cust = pd.read_sql(sql_query,analyticsdb)
# analyticsdb.close()
retail_orders = pd.merge(retail_orders,retail_cust,on='store_id',how='left')
#retail_product
# sql_query = f"Select order_id as 'id' ,item_code as 'line_items_sku',rate as 'total_line_items_price',discount_amount as 'total_discounts' From `sugar_retail_management`.retail_orders where order_id in {(r_order1)}"
sql_query = f"Select order_id as 'id' ,item_code as 'sku',quantity,rate,discount_amount From `sugar_retail_management`.retail_product_details where order_id in ({r_order1})"
# analytics.execute(sql_query)
retail_product = pd.read_sql(sql_query,analyticsdb)
analyticsdb.close()
retail_product['rate'] = retail_product['rate'] - retail_product['discount_amount']
#Merge
retail_orders = pd.merge(retail_product,retail_orders,how='right',on='id')
retail_orders['order_date'] = pd.to_datetime(retail_orders['order_date']).dt.strftime('%Y-%m-%d')
#Group by
retail_orders = retail_orders.groupby(['retail_orders','sku','state']).agg({'quantity':'sum', 'rate': 'sum'}).reset_index().rename(columns={'quantity':'qty','rate':'gross_amount'})
retail_orders['customer_group'] = 'EBO'
#Call Shopify Data
host = "readreplicadeliverydb.c13rd4d5lne0.ap-south-1.rds.amazonaws.com"
username = "admin"
password = "vHVs7iPaPJXzHat"
db = "ordersdb"
df = pd.DataFrame()
analyticsdb = pymysql.connect(host=host, user=username, passwd=password, db=db)
lst = ['ordersdb.shopifyorders','quench.quench_orders','sugarpop.sugarpop_orders']
for i in lst:
    sql_select_Query = f"SELECT id,created_at,tags,line_items_sku, line_items_qty,total_line_items_price,total_discounts,fulfillment_status FROM {i} where created_at between '{start_date}' and '{end_date}'"
    # cursor = analyticsdb.cursor()
    temp = pd.read_sql(sql_select_Query, analyticsdb)
    df = pd.concat([df,temp])
analyticsdb.close()
df['total_discounts'] = pd.to_numeric(df['total_discounts'])
df['total_line_items_price'] = pd.to_numeric(df['total_line_items_price'])
df['discount'] = round(df['total_discounts']/df['total_line_items_price'],2)
df = df[~df['tags'].str.contains('test|blogger|replacement|fraud',case=False)]
df = df[~df['fulfillment_status'].isin(['restocked','None'])].reset_index(drop=True)
df['order_date'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d')
df.drop(['created_at','fulfillment_status','total_line_items_price', 'total_discounts'],axis=1,inplace=True)
cols= ['line_items_sku','line_items_qty']
for i in cols:
    df[i] = df[i].str.split('|')
df = df.set_index(['id', 'tags', 'discount', 'order_date']).apply(lambda x: x.apply(pd.Series).stack()).reset_index()
df.drop('level_4',axis=1,inplace=True)
df.dropna(subset='line_items_sku',inplace=True)
df = df[df['line_items_sku'].str.startswith('8')]
df[['sku', 'second']] = df['line_items_sku'].str.split('-', n=1, expand=True)
df.drop(['line_items_sku','second'],inplace=True,axis=1)
worksheet_title1 = "Product"
worksheet = spreadsheet1.worksheet(worksheet_title1)
state_mapping = pd.DataFrame(list(zip(worksheet.col_values(1)[1:],worksheet.col_values(2)[1:],worksheet.col_values(3)[1:],
                                      worksheet.col_values(4)[1:],worksheet.col_values(5)[1:],worksheet.col_values(6)[1:],
                                      worksheet.col_values(7)[1:],worksheet.col_values(8)[1:],worksheet.col_values(9)[1:])),
                                      columns=['EAN Code','SKU Description','MRP','Category 1','Category 2','Category 3','Range',
                                               'Umbrella Range','Brand'])
state_mapping['EAN Code'] = state_mapping['EAN Code'].astype('str')
df = pd.merge(df,state_mapping,left_on='sku',right_on='EAN Code',how='left')
df['MRP'].fillna(0,inplace=True)
df['MRP']  = pd.to_numeric(df['MRP'])
df['gross_amount'] = (df['MRP'] - (round(df['MRP']*df['discount'],2)))
df.groupby('order_date','sku',)