import sys
import re
import csv

class other_func:
    def __init__(self):
        pass
    def format_string(self, string):
        return (re.sub(' +', ' ', string)).strip()

    def read_table_data(self, table_name):
        data = []
        file_name = table_name + '.csv'

        try:
            f = open(file_name, 'rb')
            reader = csv.reader(f)
            for row in reader:
                data.append(row)
            f.close()
        except IOError:
            sys.exit('No file was provided for the given table: \'' + table_name + '\'')
        return data
