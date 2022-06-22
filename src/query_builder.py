import json
import argparse


class Querybuilder:
    """
    Class Querybuilder helps in parsing the json input parameters and 
    returing the sql query based on the input json paramters
    """

    def __init__(self, json_file):
        self.query_dict = json.loads(open(json_file).read())
        self.join_cod = self.query_dict['joins']
        self.table_name = self.query_dict['table_name']

    def get_operator(self, operation):
        """
            fuctions helps us in getting the right opertor based on the
            input json
            return: operator thats been configured
        """
        oper_dict = {
            "IN": "in",
            "LIKE": "like",
            "EQUAL": "=",
            "BETWEEN": "BETWEEN",
            "LESS THEN": "<",
            "GREATER THEN": ">",
            "LESS THEN EQUAL TO": "<=",
            "GREATER THEN EQUAL TO": ">="
        }

        try:
            opertor_value = oper_dict[operation.upper()]
        except KeyError:
            print(operation, 'Opertor is not available')
            raise SystemExit()
        return opertor_value

    def col_alias(self, col_list, table_name):
        """
            fuctions helps us in appending the tablenames to columnslist 
            return: columnlist with table alias for those
        """

        col_alias_query = ', '.join([f'{table_name}.{cols}' for cols in col_list])
        return col_alias_query

    def filter_query(self, filter_condtions, table_name):
        """
            fuctions helps us in preparing the filter logic
            by taking filters and the table name as parameter
            return: filter logic for the table
        """
        fil_query = []
        if filter_condtions:
            for filters in filter_condtions:
                oper = self.get_operator(filters['condition'])
                col = self.col_alias([filters['column']], table_name)
                if oper == 'BETWEEN':
                    btw_value = filters['value'].split('|')
                    col_value = ' AND '.join(btw_value)
                elif oper == 'like':
                    col_value = "'%"+filters['value']+"'"
                elif oper == 'in':
                    col_value = filters['value'].split('|')
                else:
                    col_value = "'"+filters['value']+"'"
                fil_query.append(f'{col} {oper} {col_value}')
        filter_query = ' and '.join(fil_query)

        return filter_query

    def join_util(self, query, type):
        """
            fuctions helps us in appending join related sections to main table
            by taking existing main table parameter and the type
            return: concated final query of those types
            Ex- if this function is invoked with type as 'select_list' and 
                exsting main table select list, 
                If loops through join tables section in the input json and picks 
                all the select list of joins and appends to main table select list
        """
        qry = []
        if self.join_cod:
            for joins in self.join_cod:
                tb_name = joins['table_name']
                if type == 'filters':
                    qry.append(self.filter_query(joins[type], tb_name))
                else:
                    qry.append(self.col_alias(joins[type], tb_name))
            if type == 'filters':
                qry = 'and '.join(qry)
                return f'{query} and {qry}'
            else:
                qry = ','.join(qry)
                return f'{query} , {qry}'
        return query

    def set_joining_condition(self):
        """
            fucntions helps us in building the joining logic if join
            is configured in the input json
            return: join logic between table along with joining keys
        """
        join_pair = []
        join_values = []
        if self.join_cod:
            for joins in self.join_cod:
                for join_keys in joins['join_key']:
                    first_tb = join_keys['table1']
                    first_column = join_keys['column1']
                    second_tb = join_keys['table2']
                    second_column = join_keys['column2']
                    join_pair.append(
                        f'{first_tb}.{first_column}={second_tb}.{second_column}')
                join_key = ' and '.join(join_pair)
                join_type = joins['join_type']
                join_type = join_type['type']+' join ' + \
                    join_type['table2'] + ' ' + join_type['table2'] + ' ON '
                join_values.append(f'{join_type} {join_key}')
            return ' '.join(join_values)
    
    def set_group_by(self, table_name,join_condition):
        """
            functions helps in configuring the group by query from the json
            return: returns group by query
        """
        query=''
        if self.query_dict['groupby']:
            qr_query = self.col_alias(
                self.query_dict['groupby'], table_name)
            query = f'group by {qr_query}'
        if join_condition:
            if query:
                query = self.join_util(query, 'groupby')
            else:
                # query=f'group by {query}'
                query = self.join_util(query, 'groupby')
                query= query.replace(' ,','group by',1)

        return query
        

    def query_parser(self):
        """
            Parses the json structure
            return: builds sql query based on the json struture
        """
        sel_query = self.col_alias(
            self.query_dict['select_list'], self.table_name)
        filter_cod = self.filter_query(
            self.query_dict['filters'], self.table_name)
        sel_query = self.join_util(sel_query, 'select_list')
        filter_cod = self.join_util(filter_cod, 'filters')
        query = f'select {sel_query} from {self.table_name} {self.table_name}'
        joining_condition = self.set_joining_condition()
        if joining_condition:
            query = f'{query} {joining_condition}'
        group_query = self.set_group_by(self.table_name,self.join_cod)
        query = f'{query} {group_query}'
        if filter_cod:
            query = f'{query} where {filter_cod}'
        return query


def parse_cmd_args():
    """
    Parse command line arguments
    return: parsed arguments
    """
    cmd_parser = argparse.ArgumentParser(description="Query Builder")
    cmd_parser.add_argument(
        "json_input", help="path to the query configuration file")
    cmd_args = cmd_parser.parse_args()

    return cmd_args


if __name__ == "__main__":
    """
    Invoking querbuilder class with config json
    return: sql query by parsing the json structure
    """
    args = parse_cmd_args()
    query_obj = Querybuilder(json_file=args.json_input)
    final_query = query_obj.query_parser()
    print(final_query)
