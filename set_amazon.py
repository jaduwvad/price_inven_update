#from amazon.api import AmazonAPI
import amazon.api
import bottlenose.api
import codecs
import sys
import requests
import json
import time
import urllib2
import elasticsearch
import os
import socket
from lxml import etree

reload(sys)
sys.setdefaultencoding('utf-8')

RESULT_DIR = '/tmp/mymik/results/'
SOURCING_DIR = '/tmp/mymik/sourcing/'
INVEN_DIR= '/tmp/mymik/inven/'

RESULT_FILE = 'amazon_price.csv'
NEW_PRODUCT = 'new_product_amazon.csv'
INVEN_FILE_INAC = 'amazon_inac.csv'
INVEN_FILE_AC = 'amazon_ac.csv'

class query_factory:
    def get_update_query(self, price):
        query = json.dumps({ "doc" : { "price" : price } })
        return query

class es_process:
    HEADER = {'Content-Type': 'application/json'}
    ESS = "http://localhost:9200"
    ESS_INDEX = "articles_price"
    ESS_INDEX_TYPE = "product"
    ESS_FUNC_UPDATE = "_update"
    ESS_FUNC_SEARCH = "_search"

    def search_articles(self, query):
        spn_list = []
        search_url = [self.ESS, self.ESS_INDEX, self.ESS_FUNC_SEARCH]
        try:
            response = requests.get('/'.join(search_url), data = query, headers=self.HEADER)
            result = json.loads(codecs.encode(response.text, 'utf-8'))
            return spn_list
        except requests.exceptions.ConnectionError:
            time.sleep(15)
            return self.search_articles(query)

    def get_spn_list(self):
        try:
            es_client = elasticsearch.Elasticsearch(self.ESS)
            docs = es_client.search(index = self.ESS_INDEX,
                                    doc_type = self.ESS_INDEX_TYPE,
                                    body = { 'query' : { 'match' : { 'artnumber':'AMZ' } } },
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
            return self.get_spn_list()

    def update_articles(self, variantID, query):
        update_url = [self.ESS, self.ESS_INDEX, self.ESS_INDEX_TYPE, variantID, self.ESS_FUNC_UPDATE]
        try:
            response = requests.post('/'.join(update_url), data = query, headers=self.HEADER)
            result = json.loads(codecs.encode(response.text, 'utf-8'))
            if result['_shards']['successful'] == 1:
                return True
            else:
                return False
        except requests.exceptions.ConnectionError:
            time.sleep(15)
            return self.update_articles(variantID, query)
        except KeyError:
            time.sleep(300)
            return self.update_articles(variantID, query)

def search_amazon(keyword, interval):
    try:
        az = amazon.api.AmazonAPI(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOC_TAG, region='DE')
        product = az.lookup(ResponseGroup='OfferFull', ItemId=keyword)

        result = {
            'price':product.price_and_currency[0], 
            'shipping':product._safe_get_element('Offers.Offer.OfferListing.IsEligibleForSuperSaverShipping'),
            'prime':product._safe_get_element('Offers.Offer.OfferListing.IsEligibleForPrime'),
            'merchant':product._safe_get_element('Offers.Offer.Merchant.Name')
        }
        return result
    except :
        time.sleep(interval)
        return search_amazon(keyword)

def manage_inven(filename, spn, data):
    with open(filename, 'a') as f:
        f.write(spn['spn']+','+spn['variantid']+','+str(data['merchant'])+'\n')

def price_process(spn_list, call_interval, interval_on_error):
    process = es_process()
    qf = query_factory()

    for spn in spn_list:
        try:
            time.sleep(call_interval)
            data = search_amazon(spn['spn'], interval_on_error)
        except amazon.api.AsinNotFound as e:
            with open(INVEN_DIR + INVEN_FILE_INAC, 'a') as f:
                f.write(spn['spn']+','+spn['variantid']+'\n')
            continue
        except TypeError as e:
            continue

        if str(data['price']) == 'None' or data['price'] == 0:
            manage_inven(INVEN_DIR + INVEN_FILE_INAC, spn, data)
            continue
        elif (data['shipping'] == 0 and data['prime'] == 0) or (str(data['shipping'])=='None' and str(data['prime'])=='None'):
            manage_inven(INVEN_DIR + INVEN_FILE_INAC, spn, data)
            continue
        else:
            manage_inven(INVEN_DIR + INVEN_FILE_AC, spn, data)

        update_result = process.update_articles(spn['variantid'], qf.get_update_query(str(data['price'])))
        article_price = [spn['spn'], str(spn['variantid']), str(data['price'])]
        if update_result == True:
            with open(RESULT_DIR + RESULT_FILE, 'a') as f:
                f.write(','.join(article_price) + '\n')
        else:
            with open(SOURCING_DIR + NEW_PRODUCT,'a') as f:
                f.write(','.join(article_price) + '\n')

def comm_server(shop_kind, update_kind, files):
    import send_library
    for filename in files:
        send_library.send_file(filename)

    send_library.send_signal(shop_kind, update_kind)

def main():
    call_interval = int(sys.argv[1])
    interval_on_error = int(sys.argv[2])
    process = es_process()
    spn_list = process.get_spn_list()
    print len(spn_list)

    open(RESULT_DIR + RESULT_FILE, 'w')
    open(SOURCING_DIR + NEW_PRODUCT, 'w')
    open(INVEN_DIR + INVEN_FILE_INAC, 'w')
    open(INVEN_DIR + INVEN_FILE_AC, 'w')

    price_process(spn_list, call_interval, interval_on_error)

    comm_server('amazon', 'price', [RESULT_DIR + RESULT_FILE])
    comm_server('amazon', 'inven', [INVEN_DIR + INVEN_FILE_INAC, INVEN_DIR + INVEN_FILE_AC])

if __name__ == '__main__':
    main()

