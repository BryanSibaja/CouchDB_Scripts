import couchdb as cdb
couch = cdb.Server('http://10.1.101.9:5984')
db = couch['catalogo']

products = set(row.id for row in db.view('productos/productos'))
contador = 0

print('Limpiando referencias nulas...')
for docid in products:
    doc = db[docid]
    if 'related' in doc.keys():
        if 'also_bought' in doc['related'].keys():
            doc['related']['also_bought'] = list(set(doc['related']['also_bought']) & products)
            if not doc['related']['also_bought']:
                del doc['related']['also_bought']
        if 'also_viewed' in doc['related'].keys():
            doc['related']['also_viewed'] = list(set(doc['related']['also_viewed']) & products)
            if not doc['related']['also_viewed']:
                del doc['related']['also_viewed']
        if 'bought_together' in doc['related'].keys():
            doc['related']['bought_together'] = list(set(doc['related']['bought_together']) & products)
            if not doc['related']['bought_together']:
                del doc['related']['bought_together']
        if 'buy_after_viewing' in doc['related'].keys():
            doc['related']['buy_after_viewing'] = list(set(doc['related']['buy_after_viewing']) & products)
            if not doc['related']['buy_after_viewing']:
                del doc['related']['buy_after_viewing']
        if not doc['related']:
            del doc['related']
        contador = contador +1
        db[doc.id] = doc
        if contador % 5 == 0:
            print(contador)
print('Eliminadas referencias nulas')
