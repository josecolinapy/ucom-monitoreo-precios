import json
import logging
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


class Articulo(Item):
    name = Field()
    precio = Field()
    descripcion = Field()


def getStartUrl():
    url = "https://stock.com.py/default.aspx"
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    categories_links = []
    categories = soup.findAll(name='ul',
                              attrs={"class": "wsmenu-submenu-sub wstitemright clearfix"})

    for link in categories:

        subcategory = (link.find_all('ul'))
        for subcat in subcategory:
            subc1 = subcat.findAll('a')
            category = {}
            for a in subc1:
                if 'href' in a.attrs:
                    categories_links.append(a['href'])
    return categories_links


class PreciosCrawler(CrawlSpider):
    name = 'comercio1'
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36',
        'CLOSESPIDER_PAGECOUNT': 1000
        # Numero maximo de paginas en las cuales voy a descargar items. Scrapy se cierra cuando alcanza este numero
    }

    # Utilizamos 2 dominios permitidos, ya que los articulos utilizan un dominio diferente
    allowed_domains = ['stock.com.py']

    start_urls = getStartUrl()

    download_delay = 1.5

    # Tupla de reglas
    rules = (
        Rule(  # REGLA #1 => HORIZONTALIDAD POR PAGINACION
            LinkExtractor(
                allow=r'/?pageindex=\d+'
                # Patron en donde se utiliza "\d+", expresion que puede tomar el valor de cualquier combinacion de numeros
            ), follow=True, callback='parse_items'),

    )

    def parse_items(self, response):
        sel = Selector(response)
        productos = sel.xpath('//div[@class="col-lg-2 col-md-3 col-sm-4 col-xs-6 producto"]')

        for producto in productos:
            item = ItemLoader(Articulo(), producto)
            item.add_xpath('name',
                           r'./div/div/h2/a/text()', MapCompose(lambda i: i.replace('[', '').replace(']', '')))

            item.add_xpath('precio',
                           r'./div/div/div/span/span[@class="price-label"]/text()',
                           MapCompose(lambda i: i.replace('[', '').replace(']', '').replace(' ', '').replace('.', '')))
            print(item.get_xpath('name'))
            yield item.load_item()
        print(len(productos))


def readData() -> List[any]:
    fileName = 'precios_proveedor1-scraping' + str(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')) + '.json'
    process = CrawlerProcess({
        'FEED_FORMAT': 'json',
        'FEED_URI': fileName
    })
    process.crawl(PreciosCrawler)
    process.start()
    f = open(fileName)
    # returns JSON object as
    # a dictionary
    data = json.load(f)
    productos = []
    for i in data:
        try:
            producto = {
                "nombre": str(i['name']).replace('[', '').replace(']', '').replace("'", ''),
                "precio": int(str(i['precio']).replace('[', '').replace(']', '').replace("'", '')),
                "entidadComercial": 1,
                "fechaRegistro": datetime.utcnow().strftime(datetime_format_cb),
                "categoria": "1"
            }
            producto['categoria'] = producto['nombre'].split(' ')[0]
        except Exception:
            pass
        productos.append(producto)
    # productos.append(data)
    f.close()
    file = open('precios_comercio1-' + str(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')) + '.json', 'w+')
    json.dump(productos, file)
    file.close()
    return productos
