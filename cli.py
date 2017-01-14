import sys
import os
import pdb
import json
import numpy as np
import pandas as pds


args = sys.argv

dir_path = ''
query_num = ''
output = ''


for i in range(0,len(args)):
    if args[i] == '-d':
         dir_path = args[i+1]
    elif args[i] == '-n':
         query_num = args[i+1]
    elif args[i] == '-o':
         output = args[i+1]


data = os.listdir(sys.argv[2])

col_names = ['product_id', 'product_name', 'qty_sold']
d = []
print('Loading...')
for filename in data:
    if filename.endswith('.json'):
        h = open(sys.argv[2] + '/'+ filename)
        text = h.read()
        products = (json.loads(text))['products']
        for p in range(0,len(products)):


            d.append({'product_id': products[p]['product_id'], 'product_name': products[p]['product_name'], 'qty_sold': products[p]['qty_sold']})

df = pds.DataFrame(data=d, columns=col_names)
os.system('clear')
grouped = df.groupby(['product_id','product_name']).sum()
ordered = grouped.sort_values(by='qty_sold', ascending=False)

result = ordered.head(int(query_num))
# print(cache)


print(result)
