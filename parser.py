from collections import defaultdict
import csv
import os
import sys
import pandas as pd
import yaml
import json



def load_data(data_folder):
    def construct_rec(line):
      if (line[1]!='35392205'):
        sub_umls=line[4]
        obj_umls=line[8]
        predication_id=line[0]
        sub_semtype=line[6]
        sub_name=line[5]
        obj_semtype=line[10]
        obj_name=line[9]
        pmid=int(line[2])
        pred=line[3]
        sub_novelty=line[7]
        obj_novelty=line[11]
        sub_id_field="umls"
        obj_id_field="umls"
        
        sub_semtype_name=None
        obj_semtype_name=None
        if (sub_semtype in type_label):
         sub_semtype_name=type_label[sub_semtype]
       
        if (obj_semtype in type_label):
         obj_semtype_name=type_label[obj_semtype]
       
        if ("C" not in sub_umls): ###one or more gene ids
           sub_umls=sub_umls.split("|") 
           sub_name=sub_name.split("|")
           sub_id_field="ncbigene" 
        else:
           if ('|' in sub_umls):
            sub_umls=[sub_umls.split("|")[0]]
            sub_name=[sub_name.split("|")[0]]
           else:
            sub_umls=[sub_umls]
            sub_name=[sub_name]

        if ("C" not in obj_umls): ###one or more gene ids
           obj_umls=obj_umls.split("|")
           obj_name=obj_name.split("|")
           obj_id_field="ncbigene"  
        else:
           if ('|' in obj_umls):
              obj_umls=[obj_umls.split("|")[0]]
              obj_name=[obj_name.split("|")[0]]
           else:
              obj_umls=[obj_umls]
              obj_name=[obj_name]

        for sub_idx,sub_id in enumerate(sub_umls):
         for obj_idx,obj_id in enumerate(obj_umls):
           rec_related[predication_id]={
  
            "_id": "PREDICATION_ID:"+predication_id,
            "predicate": pred,
            "pmid": pmid,
            "subject":{
                    sub_id_field: sub_id,
                    "name": sub_name[sub_idx],
                    "semantic_type_abbreviation": sub_semtype,
                    "semantic_type_name": sub_semtype_name,
                    "novelty": sub_novelty
                  },
            "object":{
                   obj_id_field: obj_id,
                   "name": obj_name[obj_idx],
                   "semantic_type_abbreviation": obj_semtype,
                   "semantic_type_name": obj_semtype_name, 
                   "novelty": obj_novelty
                  }
           }
           if (not sub_semtype_name): del rec_related[predication_id]["subject"]["semantic_type_name"]
           if (not obj_semtype_name): del rec_related[predication_id]["object"]["semantic_type_name"]     

    edges_path = os.path.join(data_folder, "semmed_0821.csv")
    mapping_path = os.path.join(data_folder, "SemanticTypes_2018AB.txt")

    with open(edges_path) as f:
       next(f) 
       csv_total=sum(1 for line in f)

    names = pd.read_csv(mapping_path, sep="|",names=['abv', 'ID', 'label'])
    type_label = dict(zip(names.abv, names.label))

    rec_related = {}
    count=0
    with open(edges_path) as f:
        csv_reader = csv.reader(f, delimiter=';')
        next(csv_reader)
        for _item in csv_reader:
             count+=1
             print("Data Preparation Progess:",str(count)+"/"+str(csv_total))
             construct_rec(_item)
             print("=====")
        print("Data Generation is Done.")   
        count=0
        
    for rec in rec_related.values():
        count+=1
        print("Data Generation Progess:",str(count)+"/"+str(csv_total))
        yield rec    
