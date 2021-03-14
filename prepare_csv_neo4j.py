import gzip
import os
import json
import pandas as pd
FOLDER = './ni'
# with gzip.open('/home/joe/file.txt.gz', 'rb') as f:
#     file_content = f.read()

resources_csv_data = []

publisher_csv_data = []
publisher_set = set()
publisher_relation = []

collected_from_csv_data = []
collected_from_set = set()
collected_from_relation = []

subject_csv_data = []
subject_set = set()
subject_relation = []

author_csv_data = []
author_set = set()
author_relation = []


def prepare_csv_neo():
    for file in os.listdir(FOLDER):
           
        if not os.path.isfile(os.path.join(FOLDER, file)) or not file.endswith('.gz'):
            continue
        
        with gzip.open(os.path.join(FOLDER, file), 'rb') as f:
            # each file line contain a resource
            # variables ending with _n refer with element obtained by Neo4j insert
            resources = [json.loads(line) for line in f.readlines()]
            
            for resource in resources:
                # resource is a paper for instance
                # create resource
                if not resource.get('maintitle'):
                    continue

                resources_csv_data.append({
                    "id:ID": resource['id'],
                    "type": resource.get('type') if resource.get('type') is not None else "Undefined",
                    "main_title": resource['maintitle'],
                    ":LABEL": "Resource"
                })

                # reference to publisher
                if resource.get('publisher'):
                    if resource['publisher'] not in publisher_set:
                        publisher_csv_data.append({
                            "id:ID": resource['publisher'] + "_pub",
                            ":LABEL": "Publisher"
                        })
                        publisher_set.add(resource['publisher'])

                    publisher_relation.append({
                        ":START_ID": resource['id'],
                        ":END_ID": resource['publisher'] + "_pub",
                        ":TYPE": "PUBLISHED_IN"
                    })



                # collected from
                for collected_from in resource['collectedfrom']:
                    if collected_from['value'] not in collected_from_set:
                        collected_from_csv_data.append({
                            "id:ID": collected_from['value'] + "_coll",
                            ":LABEL": "Collector"
                        })
                        collected_from_set.add(collected_from['value'])

                    collected_from_relation.append({
                        ":START_ID": resource['id'],
                        ":END_ID": collected_from['value'] + "_coll",
                        ":TYPE": "COLLECTED_FROM"
                    })

                # connected keywords 
                for subject in resource['subjects']:
                    if subject['subject']['value'] not in subject_set:
                        subject_csv_data.append({
                            "id:ID": subject['subject']['value'] + "_sub",
                            ":LABEL": "Keyword"
                        })
                        subject_set.add(subject['subject']['value'])

                    subject_relation.append({
                        ":START_ID": resource['id'],
                        ":END_ID": subject['subject']['value'] + "_sub",
                        ":TYPE": "HAS_KEYWORD"
                    })

                # create author and link to resource
                for author in resource['author']:
                    if author['fullname'] == "":
                        continue
                    if author['fullname'] not in author_set:
                        author_csv_data.append({
                            "id:ID": author['fullname'],
                            ":LABEL": "Author"
                        })
                        author_set.add(author['fullname'])
                    
                    author_relation.append({
                        ":START_ID": author['fullname'],
                        ":END_ID": resource['id'],
                        ":TYPE": "CONTRIBUITED_IN",
                        "rank": author['rank']
                    })

    resources_csv = pd.DataFrame(resources_csv_data)
    resources_csv.to_csv("./import_files/resources.csv", index= False)

    publisher_csv = pd.DataFrame(publisher_csv_data)
    publisher_csv.to_csv("./import_files/publisher.csv", index= False)
    publisher_rel_csv = pd.DataFrame(publisher_relation)
    publisher_rel_csv.to_csv("./import_files/publisher_rel.csv", index= False)

    collected_from_csv = pd.DataFrame(collected_from_csv_data)
    collected_from_csv.to_csv("./import_files/collected_from.csv", index= False)
    collected_from_rel_csv = pd.DataFrame(collected_from_relation)
    collected_from_rel_csv.to_csv("./import_files/collected_from_rel.csv", index= False)

    subject_csv = pd.DataFrame(subject_csv_data)
    subject_csv.to_csv("./import_files/subject.csv", index= False)
    subject_rel_csv = pd.DataFrame(subject_relation)
    subject_rel_csv.to_csv("./import_files/subject_rel.csv", index= False)

    author_csv = pd.DataFrame(author_csv_data)
    author_csv.to_csv("./import_files/author.csv", index= False)    
    author_rel_csv = pd.DataFrame(author_relation)
    author_rel_csv.to_csv("./import_files/author_rel.csv", index= False)
# una volta nella cartella neo4j: (questa procedura funziona solo su un db mai usato, cioè che è stato startato una volta ma mai inserito niente)
# bin/neo4j-admin import --database=neo4j --nodes=import/resources.csv --nodes=import/publisher.csv --relationships=import/publisher_rel.csv --nodes=import/collected_from.csv --relationships=import/collected_from_rel.csv --nodes=import/subject.csv --relationships=import/subject_rel.csv --nodes=import/author.csv --relationships=import/author_rel.csv

prepare_csv_neo()