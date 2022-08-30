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
    
    
def GetAttributes():
    all_attributes = [k.lower() for k, v in new_dict.items() if k!='product_group_name' and k!='product_type_name']
    all_values = [v.lower() for k, v in new_dict.items() if k!='product_group_name' and k!='product_type_name']
    return all_attributes,all_values



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

    
def get_products_list(cat_id,sub_cat_id,all_attributes, all_values):
    list_prod = []
    all_products = []
    
    if sub_cat_id==0 and cat_id ==0:
        return "Please enter a valid product name"
    
    elif sub_cat_id ==0 and cat_id != 0 :
        if len(all_attributes) == 0:
            list_prod.append(g.V().has('~id' , cat_id).out().out().out().hasLabel('variant').path().by('value').toSet())
        for i in list_prod:
            for j in i:
                all_products.append(j[3])
            return all_products[:9]
        else:
            final_products = []
            for attr_val in all_values:
                final_products.append(list(g.V().has('~id' , sub_cat_id).out().out().hasLabel('variant').out().has('value' , attr_val).path().toSet()))
            for i in range(0,len(all_attributes)):
                for j in range(0,len(final_products[i])):
                    list_prod.append(final_products[i][j][1])
            count_dict = {}
            for i in range(len(list_prod)):
                count_dict[list_prod[i].id] = list_prod.count(list_prod[i])
            sorted_dict = sorted(count_dict.items(), key=lambda x:x[1],reverse = True)
            arr = []
            for k,v in sorted_dict:
                arr.append(g.V().has('~id' , k).toList())
            for i in range(len(arr)):
                t = g.V().has('~id' , arr[i][0]).path().by('value').toList()
                all_products.append((t[0][0]))
            return all_products[:9]
        
    else:
        if len(all_attributes) == 0:
            list_prod.append(list(g.V().has('~id' , sub_cat_id).out().out().hasLabel('variant').out().path().by('value').toSet()))
        for i in list_prod:
            for j in i:
                all_products.append(j[2])
            return all_products[:9]
        else:
            final_products = []
            for attr_val in all_values:
                final_products.append(list(g.V().has('~id' , sub_cat_id).out().out().hasLabel('variant').out().has('value' , attr_val).path().toSet()))
            for i in range(0,len(all_attributes)):
                for j in range(0,len(final_products[i])):
                    list_prod.append(final_products[i][j][2])
            count_dict = {}
            for i in range(len(list_prod)):
                count_dict[list_prod[i].id] = list_prod.count(list_prod[i])
            sorted_dict = sorted(count_dict.items(), key=lambda x:x[1],reverse = True)
            arr = []
            for k,v in sorted_dict:
                arr.append(g.V().has('~id' , k).toList())
            for i in range(len(arr)):
                t = g.V().has('~id' , arr[i][0]).path().by('value').toList()
                all_products.append((t[0][0]))
            return all_products[:9]

        
def get_dict(x):
    try:
        dic = {}
        for i in x:
            vertex = g.V().has('value' ,i).toList()
            ids = vertex[0].id
            dic[ids] = i
        return dic
    except:
        return 'Please enter a valid product name'
    

# function to find all possible nodes connected with the product
def matching_node_value(root):
    x_value = g.V().has('~id',root).out().values('value').toList()
    x_label = g.V().has('~id',root).out().label().toList()
    product = g.V().has('~id',root).both('prod_of').values('value').toList()
    sub_category = g.V().has('value',product[0]).both('sub_of').values().toList()
    x_label.append('subcategory')
    x_value.append(sub_category[0])
    return product[0], x_label, x_value


#function traverse the graph to find all the attributes of recommended product.
def product_attributes_dict(recomd_prod):
    product_dict = {}
    for prod_id in recomd_prod:
        main_product, prod_label, prod_value = matching_node_value(str(prod_id))
        prod_label.append('main_product')
        prod_value.append(main_product)
        product_dict[prod_id] = {prod_label[i]: prod_value[i] for i in range(len(prod_label))}
    return product_dict



def get_product(query, new_dict):
    
    #GetCatandSubcat
    product_group_name = GetCatandSubcat(new_dict)[0]
    product_type_name = GetCatandSubcat(new_dict)[1]
    
    #GetCatId
    cat_id = GetCatId(product_group_name)

    #GetsubcatId
    sub_cat_id = GetSubcatId(product_type_name)
    
    #GetAttributes
    attributes_tup = GetAttributes()
    all_attributes = attributes_tup[0]
    all_values = attributes_tup[1]
    
    #get_products
    x = get_products_list(cat_id,sub_cat_id,all_attributes, all_values)
    
    #get_dict
    prod_dict = get_dict(x)
    prod_dict = [x for x in prod_dict]
    
    
    product_attributes_values = product_attributes_dict(prod_dict)
    
    final_prod_dict = {}
    for p in prod_dict:
        final_prod_dict[p] = product_attributes_values[p]['main_product']
        
    return final_prod_dict     

    
    
# Using sample query to search products
query = 'T-shirt'
new_dict = {'product_group_name' : "garment upper body",
             'product_type_name' : 't-shirt',
            'colour_group_name' :'blue'
           }


get_product(query, new_dict)
        
    
    
