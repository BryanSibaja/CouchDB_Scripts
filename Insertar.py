from itertools import islice
from ast import literal_eval
import couchdb as cdb
import gzip
import os

couch = cdb.Server('http://10.1.101.9:5984')
#couch = cdb.Server('http://127.0.0.1:5984')
meta = ['meta_Books.json.gz','meta_Baby.json.gz','meta_Office_Products.json.gz']
review = ['reviews_Baby_5.json.gz','reviews_Office_Products_5.json.gz','reviews_Books_5.json.gz']
db = couch['catalogo'] if 'catalogo' in couch else couch.create('catalogo')
products = set()
cantidad = 1000

for name in meta:
    with gzip.open(name, 'r') as file:
        print('Cargando {} '.format(name))
        for i,line in enumerate(islice(file, cantidad)):
            doc = literal_eval(line)
            doc['type'] = 'product'
            doc['_id'] = doc.pop('asin')
            products.add(doc['_id'])
            try:
                db.save(doc)
            except cdb.http.ResourceConflict:
                pass
print('Metadatos cargados')

for name in review:
    print('Cargando {} '.format(name))
    with gzip.open(name, 'r') as file:
        for line in file:
            doc = literal_eval(line)
            if db.get(doc['asin']):
                db.save(doc)
print('Reviews cargadas')
