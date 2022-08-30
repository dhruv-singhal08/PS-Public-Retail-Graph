# import librairies
import networkx as nx
import pandas as pd
import numpy as np
import math as math
import time

#load data
df = pd.read_csv('articles.csv',
                 usecols=['article_id', 'prod_name','product_type_name','product_group_name',
                          'graphical_appearance_name','colour_group_name',
                          'department_name','index_name','index_group_name',
                          'section_name','garment_group_name','detail_desc'])
df.detail_desc=df.detail_desc.fillna(" ")
df = df.astype(str)

G = nx.Graph(label="Clothing Store")
start_time = time.time()
for i, rowi in df.iterrows():
    G.add_node(rowi['prod_name'],label="Product")
    for element in rowi['product_type_name']:
        G.add_node(element,label="sub_cat")
        G.add_edge(rowi['prod_name'], element, label="sub_cat_of")
    for element in rowi['product_group_name']:
        G.add_node(element,label="category")
        G.add_edge(rowi['prod_name'], element, label="cat_of")
    for element in rowi['department_name']:
        G.add_node(element,label="Deaprtment")
        G.add_edge(rowi['prod_name'], element, label="department_of")
    for element in rowi['index_name']:
        G.add_node(element,label="Gender")
        G.add_edge(rowi['prod_name'], element, label="gender_type")
        
def get_recommendation(root):
    root = df[df['article_id'] == root ].prod_name.tolist()[0]
    
    commons_dict = {}
    for e in G.neighbors(root):
        for e2 in G.neighbors(e):
            if e2==root:
                continue
            if G.nodes[e2]['label']=="Product":
                commons = commons_dict.get(e2)
                if commons==None:
                    commons_dict.update({e2 : [e]})
                else:
                    commons.append(e)
                    commons_dict.update({e2 : commons})
    product=[]
    weight=[]
    for key, values in commons_dict.items():
        w=0.0
        for e in values:
            w=w+1/math.log(G.degree(e))
        product.append(key) 
        weight.append(w)
    
    result = pd.Series(data=np.array(weight),index=product)
    result.sort_values(inplace=True,ascending=False)     
    k = result.index[:6];
    R ={}
    for i in k:
        R[df[df['prod_name'] == i ].article_id.tolist()[0]]  = i

    
    return R

root = '118458003'
get_recommendation(root)