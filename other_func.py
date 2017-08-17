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

    def print_header(self,table_name, columns):
        string = ''
        for column in columns:
            if string != '':
                string += ','
            string += table_name + '.' + column
        return string

    def search_col(self, column, tables, dict):
        #searches reqd cols in the given tables
        if '.' in column:
            table, column = column.split('.')
            table = self.format_string(table)
            column = self.format_string(column)
            if table not in tables:
                sys.exit('No table as \'' + table + '\' exists')
            return table, column
        f = 0
        table_needed = ''
        for table in tables:
            if column in dict[table]:
                f += 1
                if f > 1:
                    sys.exit('Invalid column name \'' + column + '\' given')
                table_needed = table
        if f == 0:
            sys.exit('No such column as \'' + column + '\' found')
        return table_needed, column


    def generate_eval(self, condition, table, table_info, data):
        #returns evaluation of each row to see if it matches the condition
        condition = condition.split(' ')
        evaluator = ''
        for i in condition:
            i = self.format_string(i)
            if '.' in i:
                table_cur, column = self.search_col(i, [table], table_info)
                if table_cur != table:
                    sys.exit('Table \'' + table_cur + '\' not found')
                elif column not in table_info[table]:
                    sys.exit('No such column as \'' + column + '\' found in \'' + table_cur + '\' is given')
                evaluator += data[table_info[table_cur].index(column)]
            elif i == '=':
                evaluator += i*2
            elif i.lower() == 'and' or i.lower() == 'or':
                evaluator += ' ' + i.lower() + ' '
            elif i in table_info[table]:
                evaluator += data[table_info[table].index(i)]
            else:
                evaluator += i
        return evaluator


    def get_tables_col(self, columns, tables, table_info):
        #To select the reqd tables and corresponding columns
        cols_in_table = {}
        tables_needed = []
        if len(columns) == 1 and columns[0] == '*':
            for table in tables:
                cols_in_table[table] = []
                for column in table_info[table]:
                    cols_in_table[table].append(column)
            return cols_in_table, tables
        for column in columns:
            table, column = self.search_col(column, tables, table_info)
            if table not in cols_in_table.keys():
                cols_in_table[table] = []
                tables_needed.append(table)
            cols_in_table[table].append(column)
        return cols_in_table, tables_needed


    def join_needed_data(self, operator, tables, reqd_data, tables_data):
        #Joins the data needed for where clause
        if operator == 'and':
            return self.join_data_and(tables, reqd_data)
        elif operator == 'or':
            return self.join_data_or(tables, reqd_data, tables_data)
        else:
            return self.join_data_single(tables, reqd_data, tables_data)


    def join_data_and(self, tables, reqd_data):
        #Join of data if the operator is AND
        data_final = []
        table1 = self.format_string(tables[0])
        table2 = self.format_string(tables[1])
        for i in reqd_data[table1]:
            for j in reqd_data[table2]:
                data_final.append(i + j)

        return data_final


    def join_data_or(self, tables, reqd_data, tables_data):
        #Joins the data when the operator ins OR
        data_final = []
        table1 = self.format_string(tables[0])
        table2 = self.format_string(tables[1])
        for i in reqd_data[table1]:
            for j in tables_data[table2]:
                if j not in reqd_data[table2]:
                    data_final.append(i + j)
        for i in reqd_data[table2]:
            for j in tables_data[table1]:
                if j not in reqd_data[table1]:
                    data_final.append(j + i)

        return data_final

    def join_data_single(self, tables, reqd_data, tables_data):
        #Joins the data when there is no AND/OR operator
        data_final = []
        table1 = reqd_data.keys()[0]
        flg = False
        table2 = tables[1]
        if table1 == tables[1]:
            table2 = tables[0]
            flg = True
        for i in reqd_data[table1]:
            for j in tables_data[table2]:
                if not flg:
                    data_final.append(j + i)
                else:
                    data_final.append(i + j)

        return data_final 
