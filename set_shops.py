import json
import requests
from codecs import encode
import sys
import urllib2
import time
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(filename='/tmp/mymik/server_log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

HEADER = {'Content-Type': 'application/json'}

ESS = "http://localhost:9200"
ESS_INDEX = "articles_price"

WAVE = 'wave'
ALTERNATE = 'alternate'
SHOPS = 'shops'
RAKUTEN = 'rakuten'

AMAZON_URL = ""

ESS_FUNC_UPDATE= "_update"
ESS_FUNC_SEARCH = "_search"
ESS_INDEX_TYPE = "product"

AFFILINET_PRICE_ID = 1
AFFILINET_SPN_ID = 0
AFFILINET_SEPARATOR = ';'
WAVE_PRICE_ID = 4
WAVE_SPN_ID = 0
WAVE_SEPARATOR = '	'

DATA_DIR = '/tmp/mymik/data/'
RESULT_DIR = '/tmp/mymik/results/'
SOURCING_DIR = '/tmp/mymik/sourcing/'

ALTERNATE_DATA_FILE = 'alternate.csv'
SHOP_DATA_FILE = "shop_data.json"

SHOP_RESULT_FILE = 'article_price.csv'
RAKUTEN_RESULT_FILE = 'rakuten_price.csv'
ALTERNATE_RESULT_FILE = 'alternate_price.csv'
WAVE_RESULT_FILE = 'wave_price.csv'

SHOP_NEW_PRODUCT = 'new_product_shops.csv'
ALTERNATE_NEW_PRODUCT = 'new_product_alternate.csv'
WAVE_NEW_PRODUCT = 'new_product_wave.csv'

def get_search_query(suppliernumber, tag):
    query=json.dumps({
      "query": { 
        "bool":{
          "must":[
            { "match" : { "suppliernumber":{"query":suppliernumber,"operator":"and"}}},
            { "match" : { "artnumber":tag} }
          ]
        }
      }
    })
    return query

def get_update_query(Price):
    query = json.dumps({
        "doc": { "price":Price } })
    return query

def article_check(query):
    article_check_url = [ESS, ESS_INDEX, ESS_FUNC_SEARCH]
    try:
        response = requests.get('/'.join(article_check_url), data = query, headers = HEADER)
        result = json.loads(encode(response.text, 'utf-8'))
        if result['hits']['total'] == 0:
            return False
        else:
            variantList = []
            for variant in result['hits']['hits']:
                variantList.append(str(variant['_source']['variantid']))

            return variantList
    except requests.exceptions.ConnectionError:
        time.sleep(30)
        return article_check(query)
    except KeyError:
        print response.text
        exit(1)


def price_update(variantID, query):
    price_update_url = [ESS, ESS_INDEX, ESS_INDEX_TYPE, str(variantID), ESS_FUNC_UPDATE]
    try:
        response = requests.post('/'.join(price_update_url) , data=query, headers = HEADER)
        result = json.loads(encode(response.text, 'utf-8'))
        if result['_shards']['successful'] == 1:
            return True
        else:
            return False
    except requests.exceptions.ConnectionError:
        time.sleep(30)
        price_update(variantID, query)
    except KeyError as ke:
        pass

def get_supplierNumber(sn):
    result = sn
    while True:
        if result[0] == '0':
            result = result.replace('0','', 1)
        else:
            break

    return result

def shop_process(article, tag, result_file):
    attr = article.split(AFFILINET_SEPARATOR)
    f = open(SOURCING_DIR+SHOP_NEW_PRODUCT, 'a')
    try: 
        price = float( attr[AFFILINET_PRICE_ID] )
        supplierNumber = get_supplierNumber(attr[AFFILINET_SPN_ID].replace('\"',"")).replace('P','',1)
        variantList = article_check(get_search_query(supplierNumber, tag))

        if variantList is False:
            if tag != 'RKT-' and tag != 'KHF-':
                f.write(article+'\n')
            return 0

        for variantID in variantList:
            update_result = price_update(variantID, get_update_query(str(price)))
            if update_result == True:
                with open(result_file, 'a') as f:
                    article_price = [supplierNumber, str(variantID), str(price)]
                    f.write(','.join(article_price)+'\n')
        return 1
    except IndexError :
        return 0
    except ValueError as ve:
        return 0

def wave_process(article, tag, result_file):
    attr = article.split(WAVE_SEPARATOR)
    f = open(SOURCING_DIR+SHOP_NEW_PRODUCT, 'a')
    try:
        price = float( attr[WAVE_PRICE_ID] )*1.19
        supplierNumber = get_supplierNumber(attr[WAVE_SPN_ID].replace('\"',""))
        variantList = article_check(get_search_query(supplierNumber, tag))

        if variantList is False:
            f.write(article+'\n')
            return 0
        for variantID in variantList:
            update_result = price_update(variantID, get_update_query(str(price)))
            if update_result == True:
                with open(result_file, 'a') as f:
                    article_price = [supplierNumber, str(variantID), str(price)]
                    f.write(','.join(article_price)+'\n')
        return 1
    except IndexError :
        return 0
    except ValueError as ve:
        return 0

def config_shops():
    from os import listdir

    temp = open(DATA_DIR+SHOP_DATA_FILE).read()
    shop_data = json.loads(temp)

    for shop in shop_data["shops"]:
        logging.info(shop['shop'] + ' update start')
        for url in shop['url']:
            articles = urllib2.urlopen(url).read()
            
            for article in articles.split('\n')[1:]:
                shop_process(article, shop['tag'], RESULT_DIR + SHOP_RESULT_FILE)

def config_alternate():
    import gzip
    from os import remove
    from csv import reader

    zipped = urllib2.urlopen(ALTERNATE_URL).read()
    with open('temp.gz', 'w') as f:
        f.write(zipped)

    with gzip.open('temp.gz', 'r') as f:
        with open('temp.csv', 'w') as temp:
            temp.write(f.read())

    logging.info('Alternate update start')

    data_file = open('temp.csv', 'r')
    articles = reader(data_file, delimiter=',')
    sf = open(SOURCING_DIR + ALTERNATE_NEW_PRODUCT, 'w')

    for article in articles:
        supplierNumber = get_supplierNumber(article[13])
        delTime = article[20]
        price = article[21]
        delCost = article[22]
        if supplierNumber=='ean':
            continue
        variantList = article_check(get_search_query(supplierNumber, "ATN-"))

        if variantList is False:
            sf.write(';'.join(article)+'\n')
            continue
        for variantID in variantList:
            update_result = price_update(variantID, get_update_query(str(price)))
            if update_result == True:
                with open(RESULT_DIR + ALTERNATE_RESULT_FILE, 'a') as rf:
                    article_price = [supplierNumber, str(variantID), str(price), delTime, delCost]
                    rf.write(','.join(article_price)+'\n')

    data_file.close()
    sf.close()
    remove('temp.gz')
    remove('temp.csv')

def config_wave():
    from base64 import encodestring
    wave_url = ''
    
    logging.info('Wave update start')
    auth_encoded = encodestring('%s:%s' % (id, pw))[:-1]
    req = urllib2.Request(wave_url)
    req.add_header('Authorization', 'Basic %s' % auth_encoded)

    try:
        articles = urllib2.urlopen(req).read()
        for article in articles.split('\n'):
            if len(article.split('	')) != 16:
                continue
            wave_process(article, 'WA-', RESULT_DIR + WAVE_RESULT_FILE)

    except urllib2.HTTPError as e:
        pass

def print_help():
    print 'Usage : python set_shops.py [OPTION]'
    print 'get price from shop and get variantID from elasticsearch server'
    print 'result files in folder shop_result'
    print '\nMandatory arguments to long options are mandatory for short options too.\n'
    print 'shops\t\t get price and variant for Apothekes'
    print 'rakuten\t\t get price and variant for rakuten'
    print 'alternate\t\t get price and variant for alternate'

def comm_server(shop_kind, update_kind, filename):
    import send_library
    send_library.send_file(filename)
    send_library.send_signal(shop_kind, update_kind)

def main():
    shop_kind = sys.argv[1]
    update_kind = 'price'
    filename = ''

    if sys.argv[1] == SHOPS:
        open(RESULT_DIR+SHOP_RESULT_FILE, 'w')
        open(SOURCING_DIR+SHOP_NEW_PRODUCT, 'w')

        config_shops()
        comm_server(SHOPS, 'price', RESULT_DIR + SHOP_RESULT_FILE)
    elif sys.argv[1] == ALTERNATE:
        open(RESULT_DIR + ALTERNATE_RESULT_FILE, 'w')
        open(SOURCING_DIR + ALTERNATE_NEW_PRODUCT, 'w')

        config_alternate()        
        comm_server(ALTERNATE, 'price', RESULT_DIR + ALTERNATE_RESULT_FILE)
    elif sys.argv[1] == WAVE:
        open(RESULT_DIR + WAVE_RESULT_FILE, 'w')
        open(SOURCING_DIR + WAVE_NEW_PRODUCT, 'w')

        config_wave()
        comm_server(WAVE, 'price', RESULT_DIR + WAVE_RESULT_FILE)
    elif sys.argv[1] == 'help':
        print_help()
    else:
        print 'Try \'python set_shops.py help\' for more information'

if __name__ == "__main__":
    #main()
    
    #open(RESULT_DIR+SHOP_RESULT_FILE, 'w')
    #open(SOURCING_DIR+SHOP_NEW_PRODUCT, 'w')
    #config_shops()
    #comm_server(SHOPS, 'price', RESULT_DIR + SHOP_RESULT_FILE)

    open(RESULT_DIR + ALTERNATE_RESULT_FILE, 'w')
    open(SOURCING_DIR + ALTERNATE_NEW_PRODUCT, 'w')
    config_alternate()
    comm_server(ALTERNATE, 'price', RESULT_DIR + ALTERNATE_RESULT_FILE)
    
    open(RESULT_DIR + WAVE_RESULT_FILE, 'w')
    open(SOURCING_DIR + WAVE_NEW_PRODUCT, 'w')
    config_wave()
    comm_server(WAVE, 'price', RESULT_DIR + WAVE_RESULT_FILE)

    logging.info('All of price update processes complete')

