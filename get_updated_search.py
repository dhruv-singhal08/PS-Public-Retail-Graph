import nest_asyncio
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


# Using sample query to search products
query = 't-shirt'
new_dict = {
            'product_type_name' :'sweater',
    'attribute_1':'knitted jumper wool',
 'attribute_2':'jacquard knit wool',
 'attribute_2':'wool blend',
 'attribute_4':'containing wool relaxed',
 'attribute_5':'pattern knit wool',
 'attribute_6':'jumper soft wool',
 'attribute_7':'knitted merino wool',
 'attribute_8':'fine knit wool',
 'attribute_9':'wool rib knit',
 'attribute_10':'red',
 'attribute_11':'solid'
           }
hier = {      'product_type_name' :'sweater',
    'attribute_1':'knitted jumper wool'
       }

#function to return category and subcategory from dictionary of category, subcategory and attributes
def GetCatandSubcat(new_dict):
    product_group_name = new_dict.get('product_group_name')
    product_type_name = new_dict.get('product_type_name')
    if product_group_name=='no_category' and product_type_name == 'no_subcategory':
        return 0,0
    
    elif product_group_name =='no_category' and product_type_name!=0:
        return 0,product_type_name
    
    elif product_group_name !=0 and product_type_name =='no_subcategory':
        return product_group_name,0
    
    elif product_group_name !=0 and product_type_name !=0:
        return product_group_name , product_type_name
    
#function to get Attributes from dictionary of category, subcategory and attributes
def GetAttributes(new_dict):
    all_attributes = [k.lower() for k, v in new_dict.items() if k!='product_group_name' and k!='product_type_name']
    all_values = [v.lower() for k, v in new_dict.items() if k!='product_group_name' and k!='product_type_name']
    return all_attributes,all_values

#getting id of category from graph as per category given in query
def GetCatId(product_group_name):
    if product_group_name==0:
        return 0
    else:
        try:
            cat = g.V().hasLabel('category').has('value' , product_group_name).toList()
            cat_id = cat[0].id
        except:
            cat_id =0
        return cat_id
    
#getting id of subcategory from graph as per subcategory given in query
def GetSubcatId(product_type_name):
    if product_type_name ==0:
        return 0
    else:
        try:
            sub_cat = g.V().hasLabel('subcategory').has('value', product_type_name).toList()
            sub_cat_id = sub_cat[0].id
        except:
            sub_cat_id = 0
        return sub_cat_id
#function to traverse the graph as per given query

def get_products(cat_id,sub_cat_id,all_attributes,all_values):
    all_products =[]
    list_prod = []
    
    #checking if both category and subcategory are not valid
    if sub_cat_id==0 and cat_id ==0:
        return "Please enter a valid product name"
    
    #traversal in case we find a valid category but no subcategory
    elif sub_cat_id ==0 and cat_id != 0 :
        #checking if any attribute is identified from query
        ##if no attribute found append the products inside a subcategory in a list
        if len(all_attributes) == 0:
            list_prod.append(g.V().has('~id' , cat_id).out().out().out().hasLabel('variant').path().by('value').limit(100).toSet())
        for i in list_prod:
            for j in i:
                all_products.append(j[3])
            try:
                return all_products[:9]
            except:
                return all_products
        else:
            
        #if attributes are found, traverse the graph to reach the variant node, 
        #check for connected attributes to that variant identified from query
            final_products = []
            for attr_val in all_values:
                final_products.append(list(g.V().has('~id' , cat_id).out().out().out().hasLabel('variant').out().has('value' , attr_val).path().toSet()))
            for i in range(0,len(all_attributes)):
                for j in range(0,len(final_products[i])):
                    list_prod.append(final_products[i][j][1])
        #making a dictionary of variant names,and number of attributes connected to it
            count_dict = {}
            for i in range(len(list_prod)):
                count_dict[list_prod[i].id] = list_prod.count(list_prod[i])
                
        #sorting the variants dictionary based on number of connected attributes in descending order
            sorted_dict = sorted(count_dict.items(), key=lambda x:x[1],reverse = True)
            arr = []
        #adding variant id from sorted dictionary to a list (arr)
            for k,v in sorted_dict:
                arr.append(g.V().has('~id' , k).toList())
            #adding variant names obtained from variant id to the list (all_products)
            for i in range(len(arr)):
                t = g.V().has('~id' , arr[i][0]).path().by('value').toList()
                all_products.append((t[0][0]))
            try:
                return all_products[:9]
            except:
                return all_products

    else:
        
        #if subcategory is found in query using that to traverse the graph to reach the variant nodes
        ## if not attribute is identified from query, returning list of variants inside the subcategory
        if len(all_attributes) == 0:
            list_prod.append(list(g.V().has('~id' , sub_cat_id).out().out().hasLabel('variant').out().path().by('value').toSet()))
        for i in list_prod:
            for j in i:
                all_products.append(j[2])
            try:
                return all_products[:9]
            except:
                return all_products
        else:
            
         #if attributes are found, traverse the graph to reach the variant node, 
        #check for connected attributes to that variant identified from query
            final_products = []
            for attr_val in all_values:
                final_products.append(list(g.V().has('~id' , sub_cat_id).out().out().hasLabel('variant').out().has('value' , attr_val).path().toSet()))
            for i in range(0,len(all_attributes)):
                for j in range(0,len(final_products[i])):
                    list_prod.append(final_products[i][j][2])
                    
        #making a dictionary of variant names,and number of attributes connected to it
            count_dict = {}
            for i in range(len(list_prod)):
                count_dict[list_prod[i].id] = list_prod.count(list_prod[i])
        #sorting the variants dictionary based on number of connected attributes in descending order
            sorted_dict = sorted(count_dict.items(), key=lambda x:x[1],reverse = True)
            arr = []
        #adding variant names obtained from variant id to the list (all_products)
            for k,v in sorted_dict:
                arr.append(g.V().has('~id' , k).toList())
            for i in range(len(arr)):
                t = g.V().has('~id' , arr[i][0]).path().by('value').toList()
                all_products.append((t[0][0]))
            
    return all_products

def get_product_dict(variant_name):
    try:
        dic = {}
        for i in variant_name:
            vertex = g.V().has('value' ,i).toList()
            ids = vertex[0].id
            dic[ids] = i
        return dic
    except:
        return 'Please enter a valid product name'
    
    
def get_products_from_graph(hier):
    
    product_group_name = GetCatandSubcat(new_dict)[0]
    product_type_name = GetCatandSubcat(new_dict)[1]
    
    cat_id = GetCatId(product_group_name)
    
    sub_cat_id = GetSubcatId(product_type_name)
    
    
    #getting attribute type and value from attribute tuple
    attributes_tup = GetAttributes(new_dict)
    all_attributes = attributes_tup[0]
    all_values = attributes_tup[1]
    
    
    variant_name = get_products(cat_id,sub_cat_id, all_attributes,all_values)
    
    prod_dict = get_product_dict(variant_name[:9])

    return prod_dict




get_products_from_graph(hier)