import sys
import re
import csv
from other_func import *

oth = other_func()

class process:

    def __init__(self, dict):
        # self.obj = objtemp
        self.dict = dict
        self.comm_list = []
        self.clauses = []
        self.conditions = []
        self.tables = []
        self.tables_data = {}

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
