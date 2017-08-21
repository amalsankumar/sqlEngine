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
        self.columns = []
        self.process_select()

        if len(self.columns) + len(self.distinct_process) + len(self.fn_process) < 1:
            sys.exit('Nothing is given to select')

        if len(self.distinct_process) != 0 and len(self.fn_process) != 0:
            sys.exit('Distinct and aggregate fns cant be given at the same time')
        if len(self.clauses) == 1 and len(self.tables) == 1 and len(self.fn_process) == 0:
            #for queries having * or a single column
            self.process_select_star()
        if len(self.clauses) > 1 and len(self.tables) == 1:
            #where condn on a single table
            self.process_where()
        elif len(self.clauses) > 1 and len(self.tables) > 1:
            #multiple table where condition
            self.process_multiple_where()
        elif len(self.fn_process) != 0:
            self.process_agg()



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


    def process_select_star(self):
        if len(self.columns) == 1 and self.columns[0] == '*':
            self.columns = self.dict[self.tables[0]]
        print self.columns
        print oth.print_header(self.tables[0], self.columns)
        for row in self.tables_data[self.tables[0]]:
            ans = ''
            for column in self.columns:
                ans += row[self.dict[self.tables[0]].index(column)] + ','
                fl = True
            print ans.strip(',')


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


    def process_multiple_where(self):
        self.clauses[1] = oth.format_string(self.clauses[1])
        phrase = self.clauses[1]
        logical_operators = ['<', '>', '=']
        operator = ''

        if 'or' in self.clauses[1]:
            self.clauses[1] = self.clauses.split('or')
            operator = 'or'
        elif 'and' in self.clauses[1]:
            self.clauses[1] = self.clauses.split('and')
            operator = 'and'
        else:
            self.clauses[1] = [self.clauses[1]]
        if len(self.clauses[1]) > 2:
            sys.exit('At max only one AND clause canbe given')

        condition1 = (self.clauses[1])[0]
        for i in logical_operators:
            if i in condition1:
                condition1 = condition1.split(i)
        if len(condition1) == 2 and '.' in condition1[1]:
            self.process_where_join([self.clauses[1], operator])
            return
        self.process_special_where(phrase)


    def process_where_join(self, clauses):
        #Processes where condition with join condition
        reqd_data = {}
        fail_data = {}
        operators = ['<', '>', '=']
        for i in clauses[0]:
            reqd = []
            operator = ''
            i = oth.format_string(i)
            for op in operators:
                if op in i:
                    reqd = i.split(op)
                    operator = op
                    if operator == '=':
                        oper *= 2
                    break
            if len(reqd) > 2:
                sys.exit('Error occured in where condition')
            col_condn, table_condn = oth.get_tables_col(reqd, self.tables, self.dict)
            table1 = self.tables[0]
            table2 = self.tables[1]
            col1 = self.dict[table1].index(col_condn[table1][0])
            col2 = self.dict[table2].index(col_condn[table2][0])
            reqd_data[i] = []
            reqd_data[i] = []
            for data in tables_data[table1]:
                for row in tables_data[table2]:
                    evaluator = data[col1] + operator + row[col2]
                    if eval(evaluator):
                        reqd_data[i].append(data + row)
                    else:
                        fail_data[i].append(data + row)

        if clauses[1] != '':
            join_data = oth.join_needed_data(clauses[1], clauses[0], reqd_data, fail_data)
        else:
            join_data = []
            for i in reqd_data.keys():
                for j in reqd_data[i]:
                    join_data.append(j)
        self.columns, self.tables = oth.get_tables_col(self.columns, self.tables, self.dict)
        oth.output(self.tables, self.columns, self.dict, join_data, True)


    def process_special_where(self, sentence):
        condition = []
        operator = ''
        if 'and' in sentence.lower().split():
            operator = 'and'
            condition = sentence.split('and')
        elif 'or' in sentence.lower().split():
            operator = 'or'
            condition = sentence.lower().split('or')
        else:
            condition = [sentence]
        reqd_data = oth.get_reqd_data(condition, self.tables, self.tables_data, self.dict)
        cols_in_table, tables_needed = oth.get_tables_col(self.columns, self.tables, self.dict)
        join_data = oth.join_needed_data(operator, tables_needed, reqd_data, self.tables_data)
        oth.output(tables_needed, cols_in_table, self.dict, join_data, True)


    def process_agg(self):
        #for agg fns
        header = ''
        result = ''
        for query in self.fn_process:
            fn_name = query[0]
            column_name = query[1]
            table = ''
            col = ''
            if '.' in column_name:
                table, col = column_name.split('.')
            else:
                count = 0
                for i in self.tables:
                    if column_name in self.dict[i]:
                        table = i
                        col = column_name
                        count += 1
                if count == 0:
                    sys.exit('No such column \'' + column_name + '\' found')
                elif count > 1:
                    sys.exit('Ambiguous column name \'' + column_name + '\' given')
            data = []
            header += table + '.' + col + ','
            for row in self.tables_data[table]:
                data.append(int(row[self.dict[table].index(col)]))

            if fn_name.lower() == 'min':
                result += str(min(data))
            elif fn_name.lower() == 'max':
                result += str(max(data))
            elif fn_name.lower() == 'sum':
                result += str(sum(data))
            elif fn_name.lower() == 'avg':
                result += str(float(sum(data)) / len(data))
            result += ','
        header.strip(',')
        print header
        print result
