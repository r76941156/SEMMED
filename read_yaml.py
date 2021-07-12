import yaml
import json
import pandas as pd

def parse_biolink_yaml():
   with open('biolink-model.yaml') as file:
    documents = yaml.full_load(file)
    inverse_pair={}
    SEMMED_PRED_MAPPING={}
    semmed_list=[]
    for item, doc in documents.items():
        if (item=='slots'):
        
         for doc_key in doc:
           biolink_key=doc_key.replace(" ","_")
           if ('inverse' in doc[doc_key]):
               inverse=doc[doc_key]['inverse'].strip()
               inverse_pair[doc_key]=inverse
 
           for item_key in doc[doc_key].keys():
            if ('mappings' in item_key): ### no matter what kind of mappings. E.g., exact, narrow...
             mappings=doc[doc_key][item_key]
             if (mappings!=None):
                for mapping in mappings:
                  if ('SEMMEDDB' in mapping):
                    semmed_list.append(["biolink:"+biolink_key,item_key,mapping.replace("SEMMEDDB:","")])
     
    for rec in semmed_list:
      check=rec[0].replace("biolink:"," ").replace("_"," ").strip()
      semmed_type=rec[2]
      found=0
      for key,value in inverse_pair.items():
       if (value.strip()==check): 
          found=1
          SEMMED_PRED_MAPPING[semmed_type]={'self':check.replace(' ','_'),'reverse':key.replace(' ','_')}
       elif (key.strip()==check): ###key check for part of only 07/2021
          found=1
          SEMMED_PRED_MAPPING[semmed_type]={'self':check.replace(' ','_'),'reverse':value.replace(' ','_')}
      if (found==0):
         SEMMED_PRED_MAPPING[semmed_type]={'self':check.replace(' ','_'),'reverse':check.replace(' ','_')}
