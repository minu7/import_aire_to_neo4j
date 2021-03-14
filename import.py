import gzip
import os
import json
from neo4j import GraphDatabase

# Init the connection to the database
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neuroinformatics"), encrypted=False)
RESET_DB = False
session = driver.session()

FOLDER = './ni'
# with gzip.open('/home/joe/file.txt.gz', 'rb') as f:
#     file_content = f.read()

def merge_elem(query, **kargs):
    res = session.run(query, **kargs)
    node = list(res)[0].values()[0]
    return node

def import_on_neo():
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
                resource_n = None
                if not resource.get('maintitle'):
                    print(json.dumps(resource))
                    continue
                resource['type'] = resource.get('type') if resource.get('type') is not None else "Undefined" # error if i not provide a type
                resource_n = merge_elem("""
                    MERGE (resource:Resource 
                    { 
                        id: $id,
                        main_title: $maintitle, 
                        // collection_date: $dateofcollection, sometimes miss 
                        // description: $description, 
                        type: $type 
                        // publication_date: $publicationdate sometimes miss
                    }) 
                    RETURN resource
                """, **resource)

                # reference to publisher
                if resource.get('publisher'): 
                    publisher = merge_elem("""
                            MERGE (p:Publisher { name: $name }) RETURN p
                        """, name = resource['publisher'])

                    merge_elem("""
                        MATCH (resource:Resource), (p:Publisher)
                        WHERE id(resource) = $resource_id AND id(p) = $publisher_id
                        MERGE (resource)-[r:PUBLISHED_IN]->(p) RETURN r
                    """, publisher_id = publisher.id, resource_id=resource_n.id)



                # collected from
                for collected_from in resource['collectedfrom']:
                    collected_from_n = merge_elem("""
                        MERGE (c:Collector { name: $name }) RETURN c
                    """, name = collected_from['value'])

                    merge_elem("""
                        MATCH (resource:Resource), (c:Collector)
                        WHERE id(resource) = $resource_id AND id(c) = $collected_from_id
                        MERGE (resource)-[r:COLLECTED_FROM]->(c) RETURN c
                    """, collected_from_id = collected_from_n.id, resource_id=resource_n.id)

                # connected keywords 
                for subject in resource['subjects']:
                    subject_n = merge_elem("""
                        MERGE (k:Keyword { name: $name }) RETURN k
                    """, name = subject['subject']['value'])

                    merge_elem("""
                        MATCH (resource:Resource), (k:Keyword)
                        WHERE id(resource) = $resource_id AND id(k) = $subject_id
                        MERGE (resource)-[r:HAS_KEYWORD]->(k) RETURN r
                    """, subject_id = subject_n.id, resource_id=resource_n.id)


                # create author and link to resource
                for author in resource['author']:
                    author_n = merge_elem("""
                        MERGE (author:Author { fullname: $fullname }) RETURN author
                    """, fullname = author['fullname'])

                    merge_elem("""
                        MATCH (resource:Resource), (author:Author)
                        WHERE id(resource) = $resource_id AND id(author) = $author_id
                        MERGE (author)-[r:CONTRIBUITED_IN { rank: $rank }]->(resource) RETURN author
                    """, author_id = author_n.id, resource_id=resource_n.id, rank=author['rank'])


import_on_neo()