import sys
import re
import csv
from other_func import *

oth = other_func()
FUNCS = ['disticnt', 'sum', 'max', 'avg', 'min']

class process:

    def __init__(self, dict):
        # self.obj = objtemp
        self.dict = dict
        self.comm_list = []
        self.clauses = []
        self.conditions = []
        self.tables = []
        self.tables_data = {}
        self.required_attr = []
        self.columns = []
        self.distinct_process = []
        self.fn_process = []

    def process_query(self,query):
        query = oth.format_string(query)

        if "from" not in query:
            sys.exit("Syntax ERROR: from statement missing")
        else:
            temp = query.split('from')
            before_from = oth.format_string(str(temp[0]))
            after_from = oth.format_string(str(temp[1]))

        if len(temp) != 2:
            sys.exit("Syntax ERROR: more than one from statement found")
        if 'select' not in before_from.lower():
            sys.exit("Syntax ERROR: no select statement found")
        elif query.lower().count('select') > 1:
            sys.exit("Syntax ERROR: more than one select statement found")

        self.clauses = after_from.split('where')
        self.tables = oth.format_string(self.clauses[0])
        self.tables = self.tables.split(',')

        for i in range(0, len(self.tables)):
            self.tables[i] = oth.format_string(self.tables[i])
            if self.tables[i] not in self.dict.keys():
                sys.exit('No such table as \'' + self.tables[i] + '\' Exists')
            self.tables_data[self.tables[i]] = oth.read_table_data(self.tables[i])

        self.required_attr = before_from[len('select '):]
        self.required_attr = oth.format_string(self.required_attr)
        self.required_attr = self.required_attr.split(',')

        self.process_select()
        if len(self.columns) + len(self.distinct_process) + len(self.fn_process) < 1:
            sys.exit('Nothing is given to select')

        if len(self.distinct_process) != 0 and len(self.fn_process) != 0:
            sys.exit('Distinct and aggregate fns cant be given at the same time')
        if len(self.clauses) > 1 and len(self.tables) == 1: #where condn on a single tables
            process_where()




    def process_select(self):
        column_name = ''
        for i in self.required_attr:
            flg = False;
            i = oth.format_string(i)
            for fn in FUNCS:
                if fn + '(' in i.lower():
                    flg = True
                    if ')' not in i:
                        sys.exit('Syntax ERROR: \')\' missing')
                    else:
                        column_name = i.strip(')').split(fn + '(')[1]
                    if fn == 'distinct':
                        self.distinct_process.append(column_name)
                    else:
                        self.fn_process.append([fn, column_name])
                    break
            if not flg:
                i = oth.format_string(i)
                if i != '':
                    self.columns.append(i.strip('()'))


    def process_where(self):
        self.clauses[1] = oth.format_string(self.clauses[1])

        if len(self.columns) == 1 and self.columns[0] == '*':
            self.columns = self.dict[self.tables[0]]
        print oth.print_header(self.tables[0], self.columns)
        for row in self.tables_data[self.tables[0]]:
            evaluator = oth.generate_eval(self.clauses[1], self.tables[0], self.dict, row)
            ans = ''
            if eval(evaluator):
                for column in self.columns:
                    ans += row[self.dict[self.tables[0]].index(column)] + ','
                    fl = True
                print ans.strip(',')
