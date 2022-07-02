import json
import pandas as pd

TABLE_NAME = "tab"

# df = pd.read_json('data.json')

sqlstatement = ''
with open ('data.json','r') as f:
    jsondata = json.loads(f.read())
nodes = jsondata['nodes']
edges = jsondata['edges']
edge_list = []
for e in edges:
    edge_list.append(e['from'])
edge_list.append(e['to'])
node_length = len(nodes)
query = ''
# for edge in edge_list:
for r in range(node_length):
    for node in nodes:
        if node['type'] == 'INPUT':
            data = node['transformObject']
            table_name = data['tableName']
            fields = data['fields']
            query_input = 'SELECT' + ' ' + str(", ".join(fields)) + ' ' + 'FROM' + ' ' + table_name
            query_general = 'SELECT' + ' ' + str(", ".join(fields)) + ' ' + 'FROM'
            nodes.remove(node)

            break
        if node['type'] == 'FILTER':
            data = node['transformObject']
            where_clause = data['variable_field_name']
            join_oper = data['joinOperator']
            operations_dict = data['operations'][0]
            oper = operations_dict['operator']
            val = operations_dict['value']
            query_filter = query_general + ' ' + edge_list[0] +' ' + 'WHERE' + ' ' + where_clause + ' ' + oper + ' '+ val
            edge_list.pop(0)
            nodes.remove(node)
            break
        if node['type'] == 'SORT':
            clause_list = []
            data = node['transformObject']
            query_sort = query_general + ' ' + edge_list[0] + ' ' + 'ORDER BY' + ' '
            for var in range(len(data)):
                if data[var]:
                    query_sort += data[var]['target'] + ' ' + data[var]['order'] + ', ' + ' '
            edge_list.pop(0)
            nodes.remove(node)
            break
        if node['type'] == 'TEXT_TRANSFORMATION':
            query_text = ' '
            data = node['transformObject']
            for var in range(len(data)):
                if data[var]:
                    query_text += query_general.replace(data[var]['column'], (data[var]['transformation'] + ('(') + data[var]['column'] + (')')))
            query_text += ' ' + 'FROM' + ' '+ edge_list[0]
            edge_list.pop(0)
            nodes.remove(node)
            break
        if node['type'] == 'OUTPUT':
            query_output = 'SELECT * FROM' + ' ' + edge_list[0] + ' '
            data = node['transformObject']
            query_output += 'limit' + ' ' + str(data['limit']) + ' ' + 'offset' + ' ' + str(data['offset'])
            edge_list.pop(0)
            nodes.remove(node)
            print("nodes", nodes)
            break

edge_list = []
for e in edges:
    edge_list.append(e['from'])
edge_list.append(e['to'])
query_list = [query_input, query_filter, query_sort, query_text, query_output]
final_query = 'WITH'
for r in range(len(edge_list)):
    final_query +=  ' ' + edge_list[0] + ' ' + 'as' + ' ' + '(' + query_list[0] + ' ' + ')' + ','
    if len(edge_list) == 1:
        final_query += 'SELECT * FROM' + ' ' + edge_list[0]
    edge_list.pop(0)
    query_list.pop(0)

f = open("result.sql", "a")
f.write(final_query)
f.close()

print("result query is", final_query)
