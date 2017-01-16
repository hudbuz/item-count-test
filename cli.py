import sys
import os
import pdb
import json
import pandas as pds
import datetime as dt

args = sys.argv

def get_arguments(data_path, number_of_items, output_path, arguments):
    params_args = {}
    for i in range(0,len(arguments)):
        if arguments[i] == '-d':
             params_args['dir_path'] = os.path.abspath(arguments[i+1])
        elif arguments[i] == '-n':
             params_args['query_num'] = arguments[i+1]
        elif arguments[i] == '-o':
            if os.path.isabs(arguments[i+1]):
                params_args['output'] = arguments[i+1]
            else:
                raise ValueError('Output path must be absolute')
    return params_args

params = get_arguments(dir_path, query_num, output, args)


collumn_names = ['product_id', 'product_name', 'qty_sold']

class File_Reader:


    def __init__(self, path_to_dir, number_of_products, output_filepath, collumns):
        self.directory_path = path_to_dir
        self.product_numb = number_of_products
        self.output_directory = output_filepath
        self.col_names = collumns


    def top_n_products(self):

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
                    if products[p]['product_name'] == 'Bottle Deposit' or products[p]['product_name'] == 'To Go' or products[p]['product_name'] == 'To Stay':
                        None
                    else:
                        d.append({'product_id': products[p]['product_id'], 'product_name': products[p]['product_name'], 'qty_sold': products[p]['qty_sold']})

        df = pds.DataFrame(data=d, columns=self.col_names)



        grouped = df.groupby('product_id').sum()
        ordered = grouped.sort_values(by='qty_sold', ascending=False)

        result = ordered.head(int(self.product_numb))
        # print(cache)
        for i,r in result.iterrows():
            out_params['best_sellers'].append({'product_id': i[0], 'qty_sold': r['qty_sold'], 'rank': len(out_params['best_sellers'])+1})

        str_out_params = str(out_params)
        return str_out_params

    def create_and_send_file(self, json_result):

        filepath = os.path.join(params['output'], '/item-count-test.txt')

        file = open('item-count-test.txt', 'w')
        file.write(json_result)
        file.close()
        newpath = self.output_directory + '/item-count-test.txt'
        os.rename('item-count-test.txt', newpath)

query = File_Reader(params['dir_path'], params['query_num'], params['output'], collumn_names)
query_result = query.top_n_products()
new_file = query.create_and_send_file(query_result)
print('Success!')
