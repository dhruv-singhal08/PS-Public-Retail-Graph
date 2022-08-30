import nest_asyncio
import time
nest_asyncio.apply()
import pandas as pd
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
%load_ext graph_notebook.magics
#pip install fuzzywuzzy


#pip install python-Levenshtein

# making a graph instance and connecting to database
graph = Graph()
remoteConn = DriverRemoteConnection('wss://database-prg-instance-1.cbtcet4rvkih.eu-central-1.neptune.amazonaws.com:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)


def get_attributes_keyword(query, hier):
    
    
    ##df =pd.read_csv("keyword_search_support.csv")
    
    def get_index_name_group(search_key):
        index_group_name_corpus = ['male','gents','gentlemen','men','man','mens','baby','babies','children','child','ladies','women','woman','female', 'sport','boy','girl']
        index_group_name_dict = {'boy': 'menswear', 'male': 'menswear','gents': 'menswear','gentlemen': 'menswear','men': 'menswear','man': 'menswear','mens': 'menswear','baby': 'baby/children','babies': 'baby/children','children': 'baby/children','child': 'baby/children','ladies': 'ladieswear','girl': 'ladieswear','women': 'ladieswear','woman': 'ladieswear','female': 'ladieswear','sport': 'sport'}
        index_group_name = None
        for i in search_key:
            x = process.extractOne(i , index_group_name_corpus)
            if(x[1] > 95):  
                index_group_name = index_group_name_dict[x[0]]
                search_key.remove(i)
        return search_key, index_group_name
    
    """def keyword_corpus(keywords):
        corpus = []
        for key in keywords:
            temp = key.split(',')
            for i in range(len(temp)):
                temp[i] = re.sub(r'[^\w\s]', '', temp[i]).strip()
            corpus = corpus + temp
    
        corpus = list(set(corpus))
        return corpus """

    
    def get_keyword(query, hier):
    
        query_list = query.split()
        dict_value = list(hier.values())
        search_key = []
        for i in query_list:
            if process.extractOne(i,dict_value )[1] > 91: continue
            search_key.append(i)
    
        search_key ,index_group_name =  get_index_name_group(search_key)
        hier['index_group_name']= index_group_name
    
        product_group_name = hier['product_group_name']
        product_type_name = hier['product_type_name']
        
        
        keyword = g.V().hasLabel('category').has('value',product_group_name).out().hasLabel('subcategory').has('value',product_type_name).out().hasLabel('product').out().hasLabel('keyword').values().toList() 
        corpus = list(set(keyword))

    
        #keywords = df[(df['product_group_name'] == product_group_name) & (df['product_type_name'] == product_type_name )]['final keyword'].tolist()
        #corpus = keyword_corpus(keywords)

        match_keyword = []
        for i in search_key:
            temp = {}
            for j in corpus:
                m_key = process.extractOne(i, [j])
                if m_key[1] >=75: temp[m_key[0]] = m_key[1]
        
            temp = sorted( temp.items(), key=lambda kv:(kv[1], kv[0]),reverse=True)
            temp = [x[0] for x in temp]
            if len(temp)>10: temp=temp[:10]
            match_keyword = match_keyword + temp
        

        
        for i in range(len(match_keyword)):
            key = 'keyword_'+str(i)
            hier[key] = match_keyword[i]
    
    
    
        return hier
    return get_keyword(query, hier)
    
    
    
    
        
    
        
    
hier = {'product_group_name': 'garment upper body', 'product_type_name': 'sweater', 'colour': 'black' , 'graph': 'stripped'}
query = "wool soft boy sweater"


get_attributes_keyword(query, hier)