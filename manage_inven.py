import requests
from json import dumps, loads
import time
import sys
import codecs
from urllib2 import urlopen, Request
import elasticsearch
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

#logging.basicConfig(filename='/tmp/mymik/server_log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

ESS = 'http://localhost:9200'
ESS_INDEX = 'articles_price'
ESS_INDEX_TYPE = 'product'
ESS_FUNC_UPDATE = '_update'
ESS_FUNC_SEARCH = '_search'

SHOPS = 'shops'
ALTERNATE = 'alternate'

ALTERNATE_URL = ''

WAVE_URL = ''

DATA_DIR = '/tmp/mymik/data/'
SHOP_DATA_FILE = 'shop_data.json'

RESULT_DIR = '/tmp/mymik/inven/'
SHOP_RESULT_INAC = 'shop_inac.csv'
SHOP_RESULT_AC = 'shop_ac.csv'

ALTERNATE_RESULT_INAC = 'alternate_inac.csv'
ALTERNATE_RESULT_AC = 'alternate_ac.csv'

WAVE_RESULT_INAC = 'wave_inac.csv'
WAVE_RESULT_AC = 'wave_ac.csv'

ArtNumberID = 0

def get_spn_list(tag):
    try:
        es_client = elasticsearch.Elasticsearch(ESS)
        docs = es_client.search(index = ESS_INDEX,
                                doc_type = ESS_INDEX_TYPE,
                                body = { 'query' : { 'match' : { 'artnumber': tag} } },
                                scroll = '1m',
                                size = 5000
                            )
        num_docs = docs['hits']['total']
        scroll_id = docs['_scroll_id']
        spn_list = []
        for article in docs['hits']['hits']:
            spn_list.append({"spn":article['_source']["suppliernumber"], "variantid":str(article['_source']['variantid'])})
        while num_docs > 0:
            docs = es_client.scroll(scroll_id = scroll_id, scroll='1m')
            num_docs = len(docs['hits']['hits'])
            for article in docs['hits']['hits']:
                spn_list.append({"spn":article['_source']["suppliernumber"], "variantid":str(article['_source']['variantid'])})
        return spn_list
    except requests.exceptions.ConnectionError:
        time.sleep(15)
        return get_spn_list()

def get_supplierNumber(sn):
    result = sn
    result = result.replace('P','',1)
    while True:
        if result[0] == '0':
            result = result.replace('0', '', 1)
        else:
            break
    return result

def shop_process(shop):
    source_spn_list = []
    for url in shop['url']:
        articles = urlopen(url).read()
        for article in articles.split('\n'):
            try:
                source_spn_list.append(get_supplierNumber(str(article.split(';')[ArtNumberID]).replace('\"', '')))
            except:
                continue

    file_inac = open(RESULT_DIR + SHOP_RESULT_INAC ,  'a')
    file_ac = open(RESULT_DIR + SHOP_RESULT_AC , 'a')

    spn_list = get_spn_list(shop['tag'])
    for spn in spn_list:
        
        if source_spn_list.count(spn['spn']) == 0:
            file_inac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')
        else:
            file_ac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')
    file_inac.close()
    file_ac.close()

def config_shop():
    open(RESULT_DIR + SHOP_RESULT_INAC, 'w')
    open(RESULT_DIR + SHOP_RESULT_AC, 'w')

    temp = open(DATA_DIR + SHOP_DATA_FILE).read()
    shop_data = loads(temp)
    for shop in shop_data['shops']:
        shop_process(shop)

def config_alternate():
    import gzip
    from os import remove
    from csv import reader

    open(RESULT_DIR + ALTERNATE_RESULT_INAC, 'w')
    open(RESULT_DIR + ALTERNATE_RESULT_AC, 'w')

    zipped = urlopen(ALTERNATE_URL).read()
    with open('temp.gz', 'w') as f:
        f.write(zipped)
    
    with gzip.open('temp.gz', 'r') as f:
        with open('temp.csv', 'w') as temp:
            temp.write(f.read())

    source_ean_list = []
    source_spn_list = []
    with open('temp.csv', 'r') as f:
        temp = reader(f, delimiter=',')
        for i in temp:
            source_ean_list.append(i[13])
            source_spn_list.append(i[11])

    file_inac = open(RESULT_DIR + ALTERNATE_RESULT_INAC, 'a')
    file_ac = open(RESULT_DIR + ALTERNATE_RESULT_AC, 'a')
    spn_list = get_spn_list('ATN-')

    for spn in spn_list:
        if source_ean_list.count(spn['spn']) == 0 and source_spn_list.count(spn['spn']) == 0:
            file_inac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')
        else:
            file_ac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')

    file_inac.close()
    file_ac.close()
    remove('temp.gz')
    remove('temp.csv')

def config_wave():
    from base64 import encodestring

    open(RESULT_DIR + WAVE_RESULT_INAC, 'w')
    open(RESULT_DIR + WAVE_RESULT_AC, 'w')

    #logging.info('wave update start')
    auth_encoded = encodestring('%s:%s' % (id,pw))[:-1]
    req = Request(WAVE_URL)
    req.add_header('Authorization', 'Basic %s' % auth_encoded)

    source_spn_list=[]
    articles = urlopen(req).read()
    for article in articles.split('\n'):
        if len(article.split('	')) != 16:
            continue
        source_spn_list.append(article.split('	')[0].replace('\"', ''))

    spn_list = get_spn_list('WA-')
    file_inac = open(RESULT_DIR + WAVE_RESULT_INAC, 'a')
    file_ac = open(RESULT_DIR + WAVE_RESULT_AC, 'a')
    for spn in spn_list:
        if source_spn_list.count(spn['spn']) == 0:
            file_inac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')
        else:
            file_ac.write(str(spn['spn']) + ',' + str(spn['variantid']) + '\n')

    file_inac.close()
    file_ac.close()        

def comm_server(shop_kind, update_kind, file_inac, file_ac):
    import send_library
    send_library.send_file(file_inac)
    send_library.send_file(file_ac)
    send_library.send_signal(shop_kind, update_kind)

def main():
    shop_kind = sys.argv[1]
    update_kind = 'inven'

    if shop_kind == SHOPS:
        open(RESULT_DIR + SHOP_RESULT_INAC, 'w')
        open(RESULT_DIR + SHOP_RESULT_AC, 'w')
        config_shop()

        comm_server(shop_kind, update_kind, RESULT_DIR + SHOP_RESULT_INAC, RESULT_DIR + SHOP_RESULT_AC)
    elif shop_kind == ALTERNATE:
        open(RESULT_DIR + ALTERNATE_RESULT_INAC, 'w')
        open(RESULT_DIR + ALTERNATE_RESULT_AC, 'w')
        config_alternate()

if __name__=="__main__":
    #main()
    
    open(RESULT_DIR + SHOP_RESULT_INAC, 'w')
    open(RESULT_DIR + SHOP_RESULT_AC, 'w')
    config_shop()
    comm_server('shops', 'inven', RESULT_DIR + SHOP_RESULT_INAC, RESULT_DIR + SHOP_RESULT_AC)

    open(RESULT_DIR + ALTERNATE_RESULT_INAC, 'w')
    open(RESULT_DIR + ALTERNATE_RESULT_AC, 'w')
    config_alternate()
    comm_server('alternate', 'inven', RESULT_DIR + ALTERNATE_RESULT_INAC, RESULT_DIR + ALTERNATE_RESULT_AC)
    
    open(RESULT_DIR + WAVE_RESULT_INAC, 'w')
    open(RESULT_DIR + WAVE_RESULT_AC, 'w')
    config_wave()
    comm_server('wave', 'inven', RESULT_DIR + WAVE_RESULT_INAC, RESULT_DIR + WAVE_RESULT_AC)
    
