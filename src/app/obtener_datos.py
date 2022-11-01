import glob
import json
import logging
import os
from datetime import datetime
from typing import List, Any

import proveedor1
import proveedor2
import proveedor3

datetime_format_cb = "%Y-%m-%dT%H:%M:%S"
# sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-DataProcessing | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()


def proceso_obtener_datos_proveedor(proveedor: str, url: str = None, tipo: str = 'local') -> List[Any]:
    start_time = datetime.now()
    data = []
    separator = os.sep
    if tipo == 'local':
        jsonFilesProveedor1 = glob.glob(os.path.join(".."+separator+"data"+separator+"proveedor1"+separator, "*.json"))
        logger.info(jsonFilesProveedor1)
        for fprov1 in jsonFilesProveedor1:
            f1 = open(fprov1)
            data.extend(json.load(f1))
        jsonFilesProveedor2 = glob.glob(
            os.path.join(".." + separator + "data" + separator + "proveedor2" + separator, "*.json"))
        for fprov2 in jsonFilesProveedor2:
            f2 = open(fprov2)
            data.extend(json.load(f2))
        jsonFilesProveedor3 = glob.glob(
            os.path.join(".." + separator + "data" + separator + "proveedor3" + separator, "*.json"))
        for fprov3 in jsonFilesProveedor3:
            f3 = open(fprov3)
            data.extend(json.load(f3))
    else:
        if proveedor == '1':
            data.extend(proveedor1.readData())
        if proveedor == '2':
            data.extend(proveedor2.readData(url=url))
        if proveedor == '3':
            data.extend(proveedor3.readData(url))
    #logger.info("Datos obtenidos: ", data)
    end_time = datetime.now()
    tiempo_ejecucion = (end_time - start_time).total_seconds()
    logger.info("Tiempo total de ejecución obteniendo los datos: %s", tiempo_ejecucion)
    return data
