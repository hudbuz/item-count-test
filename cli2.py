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


out_params = {'source_folder': params['dir_path'], 'run_date': str(dt.date(2017, 1, 14)),'file_count': len(data), 'best_sellers': [] }
collumn_names = ['product_id', 'product_name', 'qty_sold']

class = File_Reader:


    def __init__(self, path_to_dir, number_of_products, output_filepath ):
        self.directory_path = path_to_dir
        self.product_numb = number_of_products
        self.outut_directory = output_filepath


    def top_n_products(self, col_names):

        data = os.listdir(self.directory_path)
        d = []
        out_params = {'source_folder': self.directory_path, 'run_date': str(dt.date(2017, 1, 14)),'file_count': len(data), 'best_sellers': [] }

        print('Loading...')
        for filename in data:
            if filename.endswith('.json'):
                h = open(self.directory_path+ '/'+ filename)
                text = h.read()

                products = (json.loads(text))['products']
                for p in range(0,len(products)):


                    d.append({'product_id': products[p]['product_id'], 'product_name': products[p]['product_name'], 'qty_sold': products[p]['qty_sold']})

        df = pds.DataFrame(data=d, columns=col_names)
        os.system('clear')
        grouped = df.groupby(col_names[0], col_names[1]).sum()
        ordered = grouped.sort_values(by=col_names[2], ascending=False)

        result = ordered.head(int(self.product_numb))
        # print(cache)
        for i,r in result.iterrows():
            out_params['best_sellers'].append({'product_id': i[0], 'qty_sold': r['qty_sold'], 'rank': len(out_params['best_sellers'])+1})

        str_out_params = str(out_params)
        return str_out_params

    def create_and_send_file(self, json_result):

        filepath = os.path.join(params['output'], '/item-count-test.txt')

        file = open('item-count-test.txt', 'w')
        file.write(json_results)
        file.close()
        newpath = params['output'] + '/item-count-test.txt'
        os.rename('item-count-test.txt', self.output_directory+'/item-count-test.txt')

query = File_Reader(params['dir_path'], params['query_num'], params['output'])
query_result = query.top_n_products(collumn_names)
new_file = query.create_and_send_file(query_result)
