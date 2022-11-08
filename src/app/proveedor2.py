import json
import logging
from datetime import datetime
from typing import List

import requests

datetime_format_cb = "%Y-%m-%dT%H:%M:%S"
# sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-UCOM-PRECIOS | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()


def readData(url: str) -> List[any]:
    logger.info("Obteniendo datos de proveedor 2")
    resp = requests.get(url, verify=False)
    fileName = 'precios_proveedor2-' + str(datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')) + '.json'
    f = open(fileName, 'w+')

    json_array = resp.json()
    entities = []
    for prod in json_array['items']:
        entity = {
            "codigoBarra": prod['code'],
            "nombre": prod['name'],
            "entidadComercial": 2,
            "precio": prod['price'],
            "fechaRegistro": datetime.utcnow().strftime(datetime_format_cb)
        }
        if prod['family']:
            entity['categoria'] = prod['family']['name']
        # entities.append(json_array['items'])
        entities.append(entity)
    json.dump(entities, f)
    f.close()
    return entities
