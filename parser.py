from collections import defaultdict
import csv
import os
import sys
import pandas as pd


def load_data(data_folder):

    def construct_rec(line):
        sub_umls=line[1]
        obj_umls=line[5]
        predication_id=line[0]
        sub_semtype=line[3]
        sub_name=line[2]
        obj_semtype=line[7]
        obj_name=line[6]
        pmid=line[8]
        pred=line[4].lower()

        rec_related[predication_id]={
  
         "_id": "PREDICATION_ID:"+predication_id,
         "association": {"edge_label":pred},
         "subject":{
                    '_id': sub_umls,
                    'name': sub_name,
                    'semantic_type': sub_semtype
                  },   
         "object":{
                   'id': obj_umls,
                   'name': obj_name,
                   'semantic_type': obj_semtype
                  },
          "pmid": pmid
         }
        yield rec_related[predication_id]
        del rec_related[predication_id]
   

    edges_path = os.path.join(data_folder, "SEMMED_source.csv")

    with open(edges_path) as f: ###total record count
       csv_total=sum(1 for line in f)

    rec_related = {}
    with open(edges_path) as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        count=0
        for _item in csv_reader:
             count+=1
             if (count>0):
                print("Data Generation Progess:",str(count)+"/"+str(csv_total))
                construct_rec(_item)
                print("=====")
        print("Data Generation is Finished.")   

