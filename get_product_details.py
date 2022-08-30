import nest_asyncio
import time
nest_asyncio.apply()
import pandas as pd
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
%load_ext graph_notebook.magics


# making a graph instance and connecting to database
graph = Graph()
remoteConn = DriverRemoteConnection('wss://database-prg-instance-1.cbtcet4rvkih.eu-central-1.neptune.amazonaws.com:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)


def get_product_details(root):
    
    
    # function to find all possible nodes connected with the product
    def matching_node_value(root):
        x_value = g.V().has('~id',root).out().values('value').toList()
        x_label = g.V().has('~id',root).out().label().toList()
        product = g.V().has('~id',root).both('prod_of').values('value').toList()
        sub_category = g.V().has('value',product[0]).both('sub_of').values().toList()
        x_label.append('subcategory')
        x_value.append(sub_category[0])
        
        return product[0], x_label, x_value
    
    
    
    
    
    product_details = {}
    product, prod_label, prod_value = matching_node_value(root)
    product_details['product_name'] = product
    product_details['keyword'] = []
    for i in range(len(prod_label)):
        if prod_label[i] == 'keyword':
            product_details['keyword'].append(prod_value[i]) 
        else:
            product_details[prod_label[i]] = prod_value[i]
    
    return product_details


root = '667772005'

get_product_details(root)