import sys
import os
import pdb
import json
import numpy as np
import pandas as pds
import datetime as dt

args = sys.argv

dir_path = ''
query_num = ''
output = ''

def get_arguments(data_path, number_of_items, output_path, arguments):
    params_args = {}
    for i in range(0,len(args)):
        if args[i] == '-d':
             params_args['dir_path'] = os.path.abspath(args[i+1])
        elif args[i] == '-n':
             params_args['query_num'] = args[i+1]
        elif args[i] == '-o':
             params_args['output'] = args[i+1]
             ####make sure this is absolute
    return params_args

params = get_arguments(dir_path, query_num, output, args)
data = os.listdir(params['dir_path'])
out_params = {'source_folder': params['dir_path'], 'run_date': str(dt.date(2017, 1, 14)),'file_count': len(data), 'best_sellers': [] }

col_names = ['product_id', 'product_name', 'qty_sold']
d = []
print('Loading...')
for filename in data:
    if filename.endswith('.json'):
        h = open(params['dir_path'] + '/'+ filename)
        text = h.read()

        products = (json.loads(text))['products']
        for p in range(0,len(products)):


            d.append({'product_id': products[p]['product_id'], 'product_name': products[p]['product_name'], 'qty_sold': products[p]['qty_sold']})

df = pds.DataFrame(data=d, columns=col_names)
os.system('clear')
grouped = df.groupby(['product_id', 'product_name']).sum()
ordered = grouped.sort_values(by='qty_sold', ascending=False)

result = ordered.head(int(params['query_num']))
# print(cache)
for i,r in result.iterrows():
    out_params['best_sellers'].append({'product_id': i[0], 'qty_sold': r['qty_sold'], 'rank': len(out_params['best_sellers'])+1})

str_out_params = str(out_params)
filepath = os.path.join(params['output'], '/item-count-test.txt')

file = open('item-count-test.txt', 'w')
file.write(str_out_params)
file.close()
newpath = params['output'] + '/item-count-test.txt'
os.rename('item-count-test.txt', params['output']+'/item-count-test.txt')
