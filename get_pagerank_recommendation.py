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


def get_pagerank_recommendation(root):

    # making a graph instance and connecting to database
    graph = Graph()
    remoteConn = DriverRemoteConnection('wss://database-prg-instance-1.cbtcet4rvkih.eu-central-1.neptune.amazonaws.com:8182/gremlin','g')
    g = graph.traversal().withRemote(remoteConn)
    
    
    
    # function to find all possible nodes connected with the product
    def matching_node_value(root):
        x_value = g.V().has('~id',root).out().values('value').toList()
        x_label = g.V().has('~id',root).out().label().toList()
        product = g.V().has('~id',root).both('prod_of').values('value').toList()
        sub_category = g.V().has('value',product[0]).both('sub_of').values().toList()
        x_label.append('subcategory')
        x_value.append(sub_category[0])
        return product[0], x_label, x_value
    
    # this function finds all product conneed with the required attributes of product
    def pagerank_recommendation(root):
        node_neighbourbood = {}
        main_product, x_label, x_value = matching_node_value(root)
        for x,y in zip(x_label, x_value):
            product_temp = []
            #print(x,y)
            if(x == 'subcategory'):
                product_temp = g.V().hasLabel(x).has('value',y).out('sub_of').out('prod_of').path().toList()
                product_temp = [x[2].id for x in product_temp]
                node_neighbourbood[x] = product_temp

            #print("Done \n")
        
            if(x != 'subcategory'):
                product_temp = g.V().hasLabel(x).has('value',y).out().hasLabel('variant').path().toList()
                product_temp = [x[1].id for x in product_temp]
                node_neighbourbood[x] = product_temp            
            #print("Done \n")
    
        
        return main_product,x_label, x_value, node_neighbourbood
    
    
    # function make unified list of product connected with required nodes
    def product_union( node_neighbourbood,x_label, priority_column = ['department_name', 'section_name' , 'index_group_name', 'subcategory']):
    
        prod_set = node_neighbourbood['subcategory']
        for col in priority_column:
            prod_set = set(prod_set) & set(node_neighbourbood[col]) 
        
        return prod_set
        
    # function find common products on all requied nodes
    def neighbour_attr_node_match(neighbour_product, node_neighbourbood, x_label, x_value):
    
        prod_dict = {}
        prod_score = {}
        for i in neighbour_product:
            attributes = []
            for label in x_label:
                if i in node_neighbourbood[label]: attributes.append(label)
            
            prod_dict[i] = attributes
            prod_score[i] = len(attributes)
    
        return prod_dict, prod_score
        
        
    ## this function gaves the similar product dictionary with count of similar nodes
    def Recommendation(root):
        main_product, x_label, x_value, node_neighbourbood =  pagerank_recommendation(root)
    
        priority_column = ['department_name', 'section_name' , 'index_group_name', 'subcategory']
        product_set = product_union( node_neighbourbood,x_label, priority_column )
    
        prod_dict, prod_score = neighbour_attr_node_match(product_set, node_neighbourbood, x_label, x_value)
        prod_score_sorted = sorted( prod_score.items(), key=lambda kv:
                 (kv[1], kv[0]),reverse=True)
        #display(prod_score_sorted[:10])
        return prod_score_sorted, prod_dict
    
    prod_attributes = [ 'colour_group_name','perceived_colour_value_name','graphical_appearance_name','perceived_colour_master_name']
    
    
    #function traverse the graph to find all the attributes of recommended product.
    def product_attributes_dict(recomd_prod):
        product_dict = {}
        for prod_id in recomd_prod:
            main_product, prod_label, prod_value = matching_node_value(str(prod_id))
            prod_label.append('main_product')
            prod_value.append(main_product)
            product_dict[prod_id] = {prod_label[i]: prod_value[i] for i in range(len(prod_label))}
        return product_dict
        
        
        
    #feature_weight obtain from PCA to Sales
    featur_weight = { 'graphical_appearance_name' : 1.319941e-01,'colour_group_name' : 8.119646e-02,'perceived_colour_value_name' : 5.173739e-02,'perceived_colour_master_name' :6.457032e-03}
    
    ##importing feature value weight obtain from PCA
    import pickle
    with open ('featur_value_weight.txt', 'rb') as fp:
        featur_value_weight = pickle.load(fp)

    ## function calculates the product score based on feature and its value weight
    def product_score(recomd_prod,product_dict,featur_weight,featur_value_weight):
        product_feature_score = {}
        for idx in recomd_prod:
            score = featur_weight[ 'graphical_appearance_name' ]  * featur_value_weight[product_dict[idx]['graphical_appearance_name']]
            score = score + featur_weight[ 'colour_group_name' ]  * featur_value_weight[product_dict[idx]['colour_group_name']]
            score = score + featur_weight[ 'perceived_colour_value_name' ]  * featur_value_weight[product_dict[idx]['perceived_colour_value_name']]
            #score = score + featur_weight[ 'perceived_colour_master_name' ]  * featur_value_weight[product_dict[idx]['perceived_colour_master_name']]
            product_feature_score[idx] = score
    
        return product_feature_score
    
    ## import product score based on sales data
    import pickle
    with open ('prod_score_dict.txt', 'rb') as fp:
        prod_score_dict = pickle.load(fp)
    
    
    ## this function call all function to obtain recommendation
    def get_recommendation(root):
    
        #getting matching nodes for the given product
        main_product, x_label, x_value, node_neighbourbood =  pagerank_recommendation(root)
    
        #getting all product for recommendation
        priority_column = ['department_name', 'section_name' , 'index_group_name', 'subcategory']
        product_set = product_union( node_neighbourbood,x_label, priority_column )
    
        prod_dict, prod_score = neighbour_attr_node_match(product_set, node_neighbourbood, x_label, x_value)
        prod_score_sorted = sorted( prod_score.items(), key=lambda kv:
                                   (kv[1], kv[0]),reverse=True)
    
        recomd_prod_list = [i[0] for i in prod_score_sorted[:10]]
    
        product_attributes_values = product_attributes_dict(recomd_prod_list)
    
        recomd_prod_score = {}
        for i in recomd_prod_list:
            recomd_prod_score[i] = prod_score_dict[i]

        recomd_prod_score = sorted( recomd_prod_score.items(), key=lambda kv:
                                   (kv[1], kv[0]),reverse=True)

        final_recomd_prod = [i[0] for i in recomd_prod_score[:3]] 
    
        recomd_prod_list = filter(lambda i: i not in final_recomd_prod, recomd_prod_list)
    
        product_feature_score = product_score(recomd_prod_list,product_attributes_values,featur_weight,featur_value_weight)
    
        product_feature_score = sorted( product_feature_score.items(), key=lambda kv:
                                       (kv[1], kv[0]),reverse=True)

        product_feature_score = [i[0] for i in  product_feature_score[:3]]
    
        final_recomd_prod = final_recomd_prod + product_feature_score
    
        final_recomd_prod_dict = {}
        for p in final_recomd_prod:
            final_recomd_prod_dict[p] = product_attributes_values[p]['main_product']
        
        return final_recomd_prod_dict
    
    
    
    return get_recommendation(root)
    

root = '834758001'
get_pagerank_recommendation(root)