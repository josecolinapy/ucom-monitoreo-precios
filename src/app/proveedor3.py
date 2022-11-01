import json
import logging
import time
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from scrapy import Item, Field, Selector
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

datetime_format_cb = "%Y-%m-%dT%H:%M:%S"
# sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-Precios | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()


def readData(url:str) -> List[any]:
    logger.info("Obteniendo datos de proveedor 3")
    #url = 'https://sancayetano.com.py/wp-json/openwoo/v1/products/?per_page=100&page='
    entities = []
    for i in range(0, 140):
        logger.info("Invocando a: " + url + str(i + 1))
        resp = requests.get(url + str(i + 1), verify=False,headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36"})

        if resp.text is None:
            logger.info("Esperando 5 seg")
            time.sleep(5)
            resp = requests.get(url + str(i + 1), verify=False)
        json_array = resp.json()
        resp.close()
        # logger.info(str(len("Cantidad: " + json_array)))
        for prod in json_array:
            try:
                entity = {
                    # "codigoBarra": prod['meta_data'][10]['value'],
                    "nombre": prod['name'],
                    "entidadComercial": 3,
                    "precio": prod['price'],
                    "fechaRegistro": datetime.utcnow().strftime(datetime_format_cb)
                }
                if prod['categories']:
                    entity['categoria'] = prod['categories'][0]['name']
                if prod['meta_data']:
                    for m in prod['meta_data']:
                        if m['key'] == 'codigosbarra':
                            if m['value']:
                                entity['codigoBarra'] = str(m['value'][0])
                entities.append(entity)
            except Exception:
                pass
        time.sleep(1)
    file = open('precios_proveedor3-' + str(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')) + '.json', 'w+')
    json.dump(entities, file)
    file.close()
    return entities
