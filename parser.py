from collections import defaultdict
import csv
import os
import sys
import pandas as pd
import yaml
import json

maxInt = sys.maxsize


while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)


def load_data(data_folder):
       
    def construct_rec(sub_umls, obj_umls, line):

        if sub_umls not in rec_related:

            rec_related[sub_umls] = {
                '_id': sub_umls,
                'name': id_type_mapping[sub_umls]['name'],
                'semantic_type': id_type_mapping[sub_umls]['type'].replace(" ","") 
            }
        semmed_pred=line[1].lower()
        negation=line[4].lower()
        pmid_list=line[3].split(';')
        if (negation=='true'):
         pmid_list=[x+",negation" for x in pmid_list]
        pred = semmed_pred
      
        if pred not in rec_related[sub_umls]:
                rec_related[sub_umls][pred] = {}
        pmid=str(line[3])
        assoc = sub_umls + pred + pmid + obj_umls
          
        if assoc not in unique_assocs:
                unique_assocs.add(assoc)
                if obj_umls not in rec_related[sub_umls][pred]:
                    rec_related[sub_umls][pred][obj_umls] = {
                        'id': obj_umls,
                        'name': id_type_mapping[obj_umls]['name'],
                        'semantic_type': id_type_mapping[obj_umls]['type'].replace(" ",""),
                        "pmid": set()
                    }
                    rec_related[sub_umls][pred][obj_umls]["pmid"] = rec_related[sub_umls
                                                                              ][pred][obj_umls]["pmid"] | set(pmid_list) 
                    
                else:
                    rec_related[sub_umls][pred][obj_umls]["pmid"] |= set(pmid_list)

    nodes_path = os.path.join(data_folder, "nodes_filtered_new.csv")
    edges_path = os.path.join(data_folder, "edges_filtered_new.csv")
   
    id_type_mapping = {}
    unique_assocs = set()
    with open(nodes_path) as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        count=0
        for _item in csv_reader:
            sem_type=_item[3]
            umls_id=_item[0]
            name=_item[1]
            id_type_mapping[umls_id] = {'type':sem_type, 'name': name}

    with open(edges_path) as f:
       csv_total=sum(1 for line in f)

    rec_related = {}
    with open(edges_path) as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        count=0
        
        for _item in csv_reader:
             subject_id=_item[0]
             object_id=_item[2]
             count+=1
             print("Data Prep Progess:",str(count)+"/"+str(csv_total))
             construct_rec(subject_id, object_id, _item)
             print("=====")
        print("Data Prep is Done.")   

    print("Starting Data Generation Process...")
    rec_total=len(rec_related)         
    rec_count=0
    for v in rec_related.values():
        rec_count+=1
        print("Data Generation Progess:",str(rec_count)+"/"+str(rec_total))
        for m, n in v.items():
          if m not in ["_id", "name", "semantic_type"]: ###pred
                sem_tmp={}

                for item in n.values():
                    item['pmid'] = list(item['pmid'])
                    if (item['semantic_type'] in sem_tmp):
                       sem_tmp[item['semantic_type']].append(item)
                    else:
                       sem_tmp[item['semantic_type']]=[item]
                    del item['semantic_type']
                v[m] = sem_tmp
        yield v
    print("Data Generation is Done.")

