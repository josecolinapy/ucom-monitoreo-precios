import argparse
import configparser
import logging

import os
from datetime import datetime
from typing import Optional

import numpy as np
import pandas
import psycopg2
from matplotlib import pyplot as plt
from psycopg2._psycopg import connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import obtener_datos
import almacenar_datos
import pandas as pd
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


def read_config(config: configparser.ConfigParser, section: str, option: str, fallback: Optional[str] = None):
    """Read config value from environment or file"""
    env_name = f"ETL_UCOM_MONITOREO_PRECIOS_{section.upper()}_{option.upper()}"
    result = os.getenv(env_name, config.get(section, option, fallback=fallback))
    if result is None:
        # Checks if the config has a environment section
        raise ValueError(f'Missing config {section}:{option} ({env_name})')
    return result


def read_config_int(configParser: configparser.ConfigParser, section: str, option: str, fallback: Optional[str] = None):
    """Read config value as integer, from environment or file"""
    return int(read_config(configParser, section, option, fallback))


def get_connection(configParser: configparser) -> connection:
    start_time = datetime.now()
    host = read_config(configParser, 'environment', 'host')
    port = read_config(configParser, 'environment', 'port')
    user = read_config(configParser, 'environment', 'user')
    password = read_config(configParser, 'environment', 'password')
    database = read_config(configParser, 'environment', 'database')
    logger.info("Obteniendo conexión: host %s port %s user %s database %s", host, port, user, database)

    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )
    conn.autocommit = True
    end_time = datetime.now()
    tiempo_ejecucion = (end_time - start_time).total_seconds()
    logger.info("Tiempo obteniendo conexión: %s", tiempo_ejecucion)
    return conn


def lista_canasta(descripcion=" ", categoria=" "):
    # tuve que crea un data frame local ya que no me leia el global dentro de varios sub niveles

    df5 = pd.DataFrame()
    df5[df.columns] = None

    texto_union = ['DE',
                   'EN']
    descripcion0 = descripcion
    descripcion = list(descripcion.split())

    # saca las palabras 'en' y 'de'
    for i in range(len(descripcion) - 1):
        for j in texto_union:
            if descripcion[i] == j:
                descripcion.remove(j)
    #    print(descripcion)

    categoria = categoria.replace('\ufeff', '')
    lista_categoria = list(categoria.split(","))

    # recorre la lista de productos completa
    for i in range(df.shape[0]):
        cont = 0

        for x in lista_categoria:
            if df.loc[i, 'categoria'].upper() == x:
                # elimina palabras repetidas y pone en lista
                lista_palabras = list(set(df.loc[i, 'nombre'].upper().split()))
                # Valida si la cantidad de palabras en la descripcion es igual en la lista .
                for j in descripcion:

                    for z in lista_palabras:
                        if z == j:
                            cont += 1
                if cont == len(descripcion):
                    df5 = df5.append(df.loc[[i]], ignore_index=True)

    df5['categoria'] = descripcion0
    return df5


def generar_planilla():


    df4 = pd.read_excel('canasta familiar.xlsx', header=5, sheet_name="08_05_2020_base")
    df5 = pd.DataFrame()
    df5[df.columns] = None

    df4['PRODUCTOS'].replace({pd.NaT: '#vacio#'}, inplace=True)
    # buscar palabras dentro de la descripciones
    for z in range(df4.shape[0]):
        lista1 = lista_canasta(str(df4.loc[z, 'PRODUCTOS'].upper()), str(df4.loc[z, 'categoria'].upper()))
        df5 = df5.append(lista1, ignore_index=True)

    df6 = df[['entidad_comercial', 'id']]
    del df6['id']
    df6 = df6.drop_duplicates()

    df7 = pd.DataFrame()
    df7['PRODUCTOS'] = df4['PRODUCTOS']
    df7['Cantidad'] = df4['Cantidad']
    df7['TIPO/MEDIDA'] = df4['TIPO/MEDIDA']

    for i in df6['entidad_comercial']:
        df7.insert(len(df7.columns), i, '', allow_duplicates=False)
        ent_comercial = df5.loc[df5['entidad_comercial'] == i, df5.columns]
        cont = 0
        Dic_col = {
        }
        for x in df7.columns:
            Dic_col[x] = cont
            cont += 1

        cont = 0
        for j in df7['PRODUCTOS']:
            ent_comercial_j = ent_comercial.loc[ent_comercial['categoria'] == j.upper(), ent_comercial.columns]
            #        precio estandar -----------
            # precio_normalizado(ent_comercial_j,df7)
            df7.iat[cont, Dic_col[i]] = ent_comercial_j['precio'].min()
            cont += 1

    # ------------------Comercio 1--------------------------------------------------------------
    width = 0.25
    n = len(df7.index)
    x = np.arange(n)
    plt.bar(x, df7['1'], width=width, label='Comercio 1')
    plt.xticks(x, df7.index)
    plt.legend(loc='best')
    plt.show()
    plt.savefig('Comercio 1.png')

    # ------------------Comercio 2--------------------------------------------------------------
    n = len(df7.index)
    x = np.arange(n)
    plt.bar(x, df7['2'], width=width, label='Comercio 2')
    plt.xticks(x, df7.index)
    plt.legend(loc='best')
    plt.show()
    plt.savefig('Comercio 2.png')

    # ------------------Comercio 3--------------------------------------------------------------
    n = len(df7.index)
    x = np.arange(n)
    plt.bar(x, df7['3'], width=width, label='Comercio 3')
    plt.xticks(x, df7.index)
    plt.legend(loc='best')
    plt.show()
    plt.savefig('Comercio 3.png')

    # --------------------------------------------------------------------------------
    plt.bar(df7.index, df7['1'] + df7['2'] + df7['3'], label='Comercio 1')
    plt.bar(df7.index, df7['2'] + df7['3'], label='Comercio 2')
    plt.bar(df7.index, df7['3'], label='Comercio 3')
    plt.legend(loc='best')
    plt.show()
    plt.savefig('Resumen Comercio.png')

    # --------------------------------------------------------------------------------
    n = len(df7.index)
    x = np.arange(n)
    width = 0.25
    plt.bar(x - width, df7['1'], width=width, label='Comercio 1')
    plt.bar(x, df7['2'], width=width, label='Comercio 2')
    plt.bar(x + width, df7['3'], width=width, label='Comercio 3')
    plt.xticks(x, df7.index)
    plt.legend(loc='best')
    plt.show()
    plt.savefig('Resumen Comercio 2.png')

    # --------------------------------------------------------------------------------

    # asistencia = df7['PRODUCTOS']
    asistencia = df7.index
    # Obtenemos la posicion de cada etiqueta en el eje de X
    # x = np.arange(len(df7['PRODUCTOS']))
    n = len(df7.index)
    x = np.arange(n)
    # tamaño de cada barra
    width1 = 0.8

    fig, ax = plt.subplots()
    # Generamos las barras para el conjunto de Comercio1
    rects1 = ax.bar(x - width, df7['1'], width, label='Comercio1')
    # Generamos las barras para el conjunto de Comercio1
    rects2 = ax.bar(x, df7['2'], width, label='Comercio2')
    # Generamos las barras para el conjunto de Comercio3
    rects3 = ax.bar(x + width, df7['3'], width, label='Comercio3')
    # Añadimos las etiquetas de identificacion de valores en el grafico
    ax.set_ylabel('Productos')
    ax.set_title('Productos canasta familiar')
    ax.set_xticks(x)
    ax.set_xticklabels(asistencia)
    # Añadimos un legen() esto permite mmostrar con colores a que pertence cada valor.
    ax.legend()

    def autolabel(rects):
        """Funcion para agregar una etiqueta con el valor en cada barra"""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    # Añadimos las etiquetas para cada barra
    # autolabel(rects1)
    # autolabel(rects2)
    # autolabel(rects3)
    fig.tight_layout()
    plt.savefig('doble_barra.png')
    # Mostramos la grafica con el metodo show()
    plt.show()

    df.to_excel('Reporte_comercios_.xlsx')
    df7.to_excel('Reporte_comercios_canasta.xlsx')

if __name__ == "__main__":
    start_time = datetime.now()
    parser = argparse.ArgumentParser(description='Cargar configuraciones')
    parser.add_argument('-c', '--config', dest='config', help='Path del archivo de configuración')
    args = parser.parse_args()
    config = configparser.ConfigParser()
    if args.config:
        logging.info('Leyendo configuración desde %s', {args.config})
        config.read(args.config)
    logger.info("Configuramos la conexión a la base de datos")
    connPG = get_connection(config)
    data = []
    url_proveedor_2 = config.get("environment", "url_proveedor_2")
    url_proveedor_3 = config.get("environment", "url_proveedor_3")
    # data.extend(obtener_datos.proceso_obtener_datos_proveedor(proveedor='1', tipo='remoto'))
    #data.extend(obtener_datos.proceso_obtener_datos_proveedor(proveedor='2', tipo='remoto', url=url_proveedor_2))
    #data.extend(obtener_datos.proceso_obtener_datos_proveedor(proveedor='3', tipo='remoto', url=url_proveedor_3))
    # recorremos la data
    #almacenar_datos.almacenar_datos(connPG, data)
    host = read_config(config, 'environment', 'host')
    port = read_config(config, 'environment', 'port')
    user = read_config(config, 'environment', 'user')
    password = read_config(config, 'environment', 'password')
    database = read_config(config, 'environment', 'database')
    connString = 'postgresql://'+user+':'+password+'@'+host+':'+port+'/'+database
    engine = create_engine(connString)
    df = pandas.read_sql("select * \
           from productos_precios p1 \
       order by p1.id,p1.precio", engine)
    generar_planilla()
    end_time = datetime.now()
    tiempo_ejecucion = (end_time - start_time).total_seconds()
    logger.info("Tiempo total de ejecución: %s", tiempo_ejecucion)
