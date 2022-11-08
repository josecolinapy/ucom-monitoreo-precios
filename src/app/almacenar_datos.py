import logging
from datetime import datetime
from typing import Any, List

from psycopg2._psycopg import connection

datetime_format_cb = "%Y-%m-%dT%H:%M:%S"
# sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-UCOM-PRECIOS-ALMACENAR_DATOS | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()

def obtenerEtiquetas(nombre: str):
    SEPARATORS = '[,;\.]+'
    et = nombre.split()
    noseplist = [word for word in nombre if word not in SEPARATORS]
    etiquetas = ''
    temp = list(map(str, et))
    etiquetas = ",".join(temp)
    # print(etiquetas)
    return etiquetas


def validar_numero(text=" "):
    numero_validacion = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', ',']
    for j in numero_validacion:
        if j == text:
            return "SI"
    return "NO"


def Validar_tamaño_desscendente(descripcion=""):
    posicion = descripcion.find('(')
    cont = 0
    if posicion != -1:
        for z in descripcion[posicion + 1:]:
            if validar_numero(z) == "SI":
                cont += 1
            elif z == ')':
                descripcion = descripcion.replace(descripcion[posicion:(posicion + cont + 2)], '')

            else:
                break

    cont = 0
    pos = len(descripcion) - 1
    p_inicial = len(descripcion) - 1
    p_final = len(descripcion)
    val0 = descripcion.find(" ", 0)  # que no sea en la primera palabra

    for x in reversed(descripcion):

        if pos > val0:
            val2 = validar_numero(descripcion[pos - 1])
            val1 = validar_numero(descripcion[pos])

            if val2 == "SI" and val1 == "NO":
                # mi primer numero encontrado
                if p_final == len(descripcion) and descripcion[pos - 1] != "." and descripcion[pos - 1] != ",":
                    p_final = pos

                # el primer numero es punto no hace nada
                if descripcion[pos - 1] == "." and p_final == len(descripcion):
                    cont += 1

                # el primer numero es coma no hace nada
                if descripcion[pos - 1] == "," and p_final == len(descripcion):
                    cont += 1

                if p_final != len(descripcion):
                    p_final = pos

                # if p_final!=len(descripcion) and descripcion[pos-1]!="." and descripcion[pos]!="," :
                #     p_final=pos

            elif val2 == "SI" and val1 == "SI":
                if p_final == len(descripcion) and descripcion[pos - 1] != ".":
                    cont += 1

                if p_final == len(descripcion) and descripcion[pos] != ",":
                    cont += 1

                #  Errores en la escritura de dos caracteres seguidos no debe hacer nada
                if descripcion[pos - 1:pos + 1] == ".." or descripcion[pos - 1:pos + 1] == ".," or descripcion[
                                                                                                   pos - 1:pos + 1] == ",." or descripcion[
                                                                                                                               pos - 1:pos + 1] == ",,":
                    p_final = pos

                #  retorno el todo el numero siempre y cuando exista un numero anteriormente.
                if pos == val0 + 1 and p_inicial != len(descripcion) - 1:
                    return descripcion[val0 + 1:p_final]

            elif val2 == "NO" and val1 == "SI":

                # el primer caracter no debe ser un punto.
                if p_final == len(descripcion) and descripcion[pos] == ".":
                    p_final = pos + 1

                # el primer caracter no debe ser un coma.
                if p_final == len(descripcion) and descripcion[pos] == ",":
                    p_final = pos + 1

                #  si encuentra algun nuemro como punto y coma, el cual no vario el valor inicial no debe hacer nada
                if p_final == len(descripcion) and descripcion[pos] != "." and descripcion[pos] != ",":
                    p_inicial = pos
                    return descripcion[p_inicial: p_final]

                if descripcion[pos] == "," or descripcion[pos] == ".":
                    p_final = pos + 1

                    #  si ya existe un valor inicial y cierra el primer numero, devuelve el primer numero encontrado en la cadena.
                if descripcion[pos] != "," and descripcion[pos] != ".":
                    p_inicial = pos
                    return descripcion[p_inicial: p_final]

            elif val2 == "NO" and val1 == "NO":
                p_final = pos + 1

            #  el primer caracter que sea numero
            else:
                cont += 1

        pos -= 1
    return "0"


def validar_tamanho_descendente(descripcion=""):
    posicion = descripcion.find('(')
    cont = 0
    if posicion != -1:
        for z in descripcion[posicion + 1:]:
            if validar_numero(z) == "SI":
                cont += 1
            elif z == ')':
                descripcion = descripcion.replace(descripcion[posicion:(posicion + cont + 2)], '')

            else:
                break

    cont = 0
    pos = len(descripcion) - 1
    p_inicial = len(descripcion) - 1
    p_final = len(descripcion)
    val0 = descripcion.find(" ", 0)  # que no sea en la primera palabra

    for x in reversed(descripcion):

        if pos > val0:
            val2 = validar_numero(descripcion[pos - 1])
            val1 = validar_numero(descripcion[pos])

            if val2 == "SI" and val1 == "NO":
                # mi primer numero encontrado
                if p_final == len(descripcion) and descripcion[pos - 1] != "." and descripcion[pos - 1] != ",":
                    p_final = pos

                # el primer numero es punto no hace nada
                if descripcion[pos - 1] == "." and p_final == len(descripcion):
                    cont += 1

                # el primer numero es coma no hace nada
                if descripcion[pos - 1] == "," and p_final == len(descripcion):
                    cont += 1

                if p_final != len(descripcion):
                    p_final = pos

                # if p_final!=len(descripcion) and descripcion[pos-1]!="." and descripcion[pos]!="," :
                #     p_final=pos

            elif val2 == "SI" and val1 == "SI":
                if p_final == len(descripcion) and descripcion[pos - 1] != ".":
                    cont += 1

                if p_final == len(descripcion) and descripcion[pos] != ",":
                    cont += 1

                #  Errores en la escritura de dos caracteres seguidos no debe hacer nada
                if descripcion[pos - 1:pos + 1] == ".." or descripcion[pos - 1:pos + 1] == ".," or descripcion[
                                                                                                   pos - 1:pos + 1] == ",." or descripcion[
                                                                                                                               pos - 1:pos + 1] == ",,":
                    p_final = pos

                #  retorno el todo el numero siempre y cuando exista un numero anteriormente.
                if pos == val0 + 1 and p_inicial != len(descripcion) - 1:
                    return descripcion[val0 + 1:p_final]

            elif val2 == "NO" and val1 == "SI":

                # el primer caracter no debe ser un punto.
                if p_final == len(descripcion) and descripcion[pos] == ".":
                    p_final = pos + 1

                # el primer caracter no debe ser un coma.
                if p_final == len(descripcion) and descripcion[pos] == ",":
                    p_final = pos + 1

                #  si encuentra algun nuemro como punto y coma, el cual no vario el valor inicial no debe hacer nada
                if p_final == len(descripcion) and descripcion[pos] != "." and descripcion[pos] != ",":
                    p_inicial = pos
                    return descripcion[p_inicial: p_final]

                if descripcion[pos] == "," or descripcion[pos] == ".":
                    p_final = pos + 1

                    #  si ya existe un valor inicial y cierra el primer numero, devuelve el primer numero encontrado en la cadena.
                if descripcion[pos] != "," and descripcion[pos] != ".":
                    p_inicial = pos
                    return descripcion[p_inicial: p_final]

            elif val2 == "NO" and val1 == "NO":
                p_final = pos + 1

            #  el primer caracter que sea numero
            else:
                cont += 1

        pos -= 1
    return "0"


def validar_tamanho_ascendente(descripcion=""):
    posicion = descripcion.find('(')
    cont = 0
    if posicion != -1:
        for z in descripcion[posicion + 1:]:
            if validar_numero(z) == "SI":
                cont += 1
            elif z == ')':
                descripcion = descripcion.replace(descripcion[posicion:(posicion + cont + 2)], '')

            else:
                break

    cont = 0
    pos = 0
    p_inicial = 0
    p_final = 0
    val0 = descripcion.find(" ", 0)  # que no sea en la primera palabra

    for x in descripcion:

        if pos > val0:
            val1 = validar_numero(descripcion[pos - 1])
            val2 = validar_numero(descripcion[pos])

            #  el primer caracter que sea numero
            if val2 == "SI" and val1 == "NO":
                if p_inicial == 0 and descripcion[pos] != "." and descripcion[pos] != ",":
                    p_inicial = pos

                # el primer caracter no debe ser un punto.
                if p_inicial == 0 and descripcion[pos] == ".":
                    cont += 1

                # el primer caracter no debe ser un coma.
                if p_inicial == 0 and descripcion[pos] == ",":
                    cont += 1

                # el ultmo caracter no debe ser un punto.
                if pos == (len(descripcion) - 1) and descripcion[pos] == "." and p_inicial == 0:
                    return "0"

                    # el ultmo caracter no debe ser una coma.
                if pos == (len(descripcion) - 1) and descripcion[pos] == "," and p_inicial == 0:
                    return "0"

                    # el ultimo caracter puede ser un numero
                if pos == (len(descripcion) - 1):
                    p_inicial = pos
                    return descripcion[p_inicial:]

            elif val2 == "SI" and val1 == "SI":

                #  Errores en la escritura de dos caracteres seguidos no debe hacer nada
                if descripcion[pos - 1:pos + 1] == ".." or descripcion[pos - 1:pos + 1] == ".," or descripcion[
                                                                                                   pos - 1:pos + 1] == ",." or descripcion[
                                                                                                                               pos - 1:pos + 1] == ",,":
                    cont += 1

                #  retorno el todo el numero siempre y cuando exista un numero anteriormente.
                if pos == (len(descripcion) - 1) and p_inicial != 0:
                    return descripcion[p_inicial:]

            elif val2 == "NO" and val1 == "SI":

                #  si encuentra algun nuemro como punto y coma, el cual no vario el valor inicial no debe hacer nada
                if p_inicial == 0:
                    cont += 1
                else:
                    #  si ya existe un valor inicial y cierra el primer numero, devuelve el primer numero encontrado en la cadena.
                    p_final = pos
                    return descripcion[p_inicial: p_final]

            else:
                cont += 1

        pos += 1
    return "0"


def validar_punto(tamaño="01,000,"):
    # los numeros en python solo continen puntos no comas
    tamaño = tamaño.replace(',', '.')

    # elimina los puntos finales de cualquier numero.
    if tamaño[len(tamaño) - 1] == '.':
        tamaño = tamaño[:len(tamaño) - 1]

    # busca 3 lugares para eliminar el punto
    lugar_punto = tamaño.find('.')
    nume_red = tamaño[lugar_punto + 1:]
    if len(nume_red) == 3 and tamaño[0] != '0':
        tamaño = tamaño.replace('.', '')

    # contabiliza la cantidad de ceros consecutivos desde el inicio
    # elimina todos los ceros a la izquiera antes que un numero
    consecutivo = 0
    cont = 0
    for x in tamaño:
        if tamaño[0] == '0' and len(tamaño) != 1:
            if x == '0' and x != "." and consecutivo == cont:
                consecutivo += 1

        cont += 1

    if consecutivo > 0 and tamaño[1] != '.' and len(tamaño) != consecutivo:
        tamaño = tamaño[consecutivo:]

    elif consecutivo > 0 and tamaño[1] != '.' and len(tamaño) == consecutivo:
        tamaño = '0'

    else:
        cont = 0

    return tamaño


def unidad_medida(text="descripcion", text2="tamaño"):
    validacion_unidad = ['CC', 'C.C',
                         'GR', 'GRAMOS',
                         'GRANO',
                         'KG', 'KLG', 'X KG', 'X KILO', 'XKILO', 'POR KILO', 'XKG', 'KL',
                         'ML',
                         'BOT', 'BOTELLA',
                         'LT', 'L', 'LTS', 'LITROS', 'LITRO',
                         'UN', 'UNI', 'UNIDADA', 'X UN', 'U.',
                         'W',
                         'MTS',
                         'MAZO', 'X MZ']

    val0 = text.find(text2, 0)
    p_inicial = 0

    if text2 == "0":
        text2 = ''
        val0 = 0
        validacion_unidad = ['CC.', 'C.C',
                             'GR.', 'GRAMOS',
                             'GRANO',
                             'KG.', 'KLG.', 'X KG', 'X KILO', 'XKILO', 'POR KILO', 'XKG', 'KL',
                             'ML.',
                             'BOT.', 'BOTELLA',
                             'LT.', 'L.', 'LTS.', 'LITROS', 'LITRO',
                             'UN.', 'UNI.', 'UNIDAD', 'X UN', 'U.',
                             'W.',
                             'MTS.',
                             'MAZO', 'X MZ']

    text = text[val0:].upper()

    if len(text) > len(text2):
        if text[(0 + len(text2))] == 'G':
            return 'GR'
        if text[(0 + len(text2))] == 'K':
            return 'KG'

    for j in validacion_unidad:
        val0 = text.find(j, 0)
        if p_inicial <= val0:
            if j == 'CC.' or j == 'C.C':
                return 'CC'
            if j == 'GRAMOS' or j == 'GR.':
                return 'GR'
            if j == 'KLG' or j == 'KLG.' or j == 'KG.' or j == 'X KG' or j == 'X KILO' or j == 'XKILO' or j == 'POR KILO' or j == 'XKG' or j == 'KL':
                return 'KG'
            if j == 'KLG' or j == 'KLG.':
                return 'KG'
            if j == 'ML.':
                return 'ML'
            if j == 'LT.' or j == 'L.' or j == 'L' or j == 'LTS.' or j == 'LTS' or j == 'LITROS' or j == 'LITRO':
                return 'LT'
            if j == 'UNI.' or j == 'UNI' or j == 'UNIDAD' or j == 'UN.' or j == 'X UN' or j == 'U.':
                return 'UN'
            if j == 'W.':
                return 'W'
            if j == 'MTS.':
                return 'MTS'
            if j == 'X MZ':
                return 'MAZO'
            if j == 'BOT.' or j == 'BOTELLA':
                return 'BOT'

            return j

    return "No tiene UM"


def almacenar_datos(connPg: connection, data: List[Any]):
    start_time = datetime.now()
    cursor = connPg.cursor()
    for ent in data:
        try:
            etiquetas = obtenerEtiquetas(ent["nombre"])
            sqlGetProductoPrecioHistorico = "select id,nombre,precio,categoria,unidad_medida,tamanho,entidad_comercial,etiqueta,codigo_barra " \
                                            "from productos_precios_historico where upper(nombre)=upper('[nombre]') and entidad_comercial='[entidad]' order by fecha_registro desc limit 1"
            sqlGetProductoPrecioHistorico = sqlGetProductoPrecioHistorico.replace("[nombre]", ent["nombre"])
            sqlGetProductoPrecioHistorico = sqlGetProductoPrecioHistorico.replace("[entidad]",
                                                                                  str(ent["entidadComercial"]))

            cursor.execute(sqlGetProductoPrecioHistorico)
            id = 0
            if cursor.rowcount > 0:  # Existe el producto
                record = cursor.fetchone()
                id = record[0]
            else:
                sqlSeq = "select nextval('productos_precios_historico_id_seq')"
                id = cursor.execute(sqlSeq)
                id = cursor.fetchone()[0]
                # print("seq", id)
            # Insertamos el producto historico
            sqlInsertProdHist = "INSERT INTO public.productos_precios_historico (id, fecha_registro, fecha_recibido, nombre," \
                                "precio, categoria, unidad_medida, tamanho, entidad_comercial,etiqueta,codigo_barra) VALUES " \
                                "([id], '[fecha_registro]', '[fecha_recibido]', upper('[nombre]'), [precio], upper('[categoria]'), '[unidad]', '[tamanho]', '[entidad]',upper('[etiqueta]'),'[codigoBarra]')"
            sqlInsertProdHist = sqlInsertProdHist.replace("[id]", str(id))
            # print(datetime.now())
            sqlInsertProdHist = sqlInsertProdHist.replace("[fecha_registro]",
                                                          ent['fechaRegistro'])
            sqlInsertProdHist = sqlInsertProdHist.replace("[fecha_recibido]",
                                                          datetime.strftime(datetime.now(), datetime_format_cb))
            sqlInsertProdHist = sqlInsertProdHist.replace("[nombre]", ent["nombre"])
            sqlInsertProdHist = sqlInsertProdHist.replace("[precio]", str(ent["precio"]))
            sqlInsertProdHist = sqlInsertProdHist.replace("[entidad]", str(ent["entidadComercial"]))
            sqlInsertProdHist = sqlInsertProdHist.replace("[etiqueta]", etiquetas)

            if ent["entidadComercial"] == '1':
                print("No existe cod barra")
                sqlInsertProdHist = sqlInsertProdHist.replace("[codigoBarra]", '')

            else:
                try:
                    if ent['codigoBarra']:
                        codigoBarra = ent['codigoBarra']
                        sqlInsertProdHist = sqlInsertProdHist.replace("[codigoBarra]", codigoBarra)
                except Exception:
                    sqlInsertProdHist = sqlInsertProdHist.replace("[codigoBarra]", '')

            try:
                if ent['categoria']:
                    categoria = ent['categoria']
                    sqlInsertProdHist = sqlInsertProdHist.replace("[categoria]", categoria)

            except Exception:
                sqlInsertProdHist = sqlInsertProdHist.replace("[categoria]", '')

            # print(sqlInsertProdHist)
            tamanho = validar_tamanho_descendente(ent["nombre"])
            tamanho1 = validar_tamanho_ascendente(ent["nombre"])
            tamanhoFinal = ''
            variable0 = validar_punto(tamanho)
            variable1 = unidad_medida(ent["nombre"], tamanho)
            variable2 = validar_punto(tamanho)
            variable3 = unidad_medida(ent["nombre"], tamanho1)
            if variable1 == "No tiene UM" and variable3 != "No tiene UM":
                tamanhoFinal = variable2
                unidad = variable3
            elif variable1 == "No tiene UM" and variable3 == "No tiene UM":
                tamanhoFinal = variable0
                unidad = variable1
            else:
                tamanhoFinal = variable0
                unidad = variable1

            sqlInsertProdHist = sqlInsertProdHist.replace("[unidad]", unidad)
            sqlInsertProdHist = sqlInsertProdHist.replace("[tamanho]", tamanhoFinal)

            cursor.execute(sqlInsertProdHist)
            connPg.commit();
            # Verificamos si existe en productos_precios
            sqlProductoPrecio = 'select * from productos_precios where id=[id]'
            sqlProductoPrecio = sqlProductoPrecio.replace("[id]", str(id))
            cursor.execute(sqlProductoPrecio)
            if cursor.rowcount > 0:  # Eliminamos de productos precios para insertar el mas nuevo}
                sqlDeleteProductoPrecio = 'delete from productos_precios where id=[id]'
                sqlDeleteProductoPrecio = sqlDeleteProductoPrecio.replace("[id]", str(id))
                cursor.execute(sqlDeleteProductoPrecio)
                connPg.commit();
            sqlInsert = 'INSERT INTO public.productos_precios(id, nombre, precio, categoria, unidad_medida, tamanho, entidad_comercial,etiqueta,codigo_barra) ' \
                        'select id,nombre,precio,categoria,unidad_medida,tamanho,entidad_comercial,upper(etiqueta),codigo_barra ' \
                        'from productos_precios_historico where id=[id] order by fecha_registro desc limit 1';
            sqlInsert = sqlInsert.replace("[id]", str(id))
            cursor.execute(sqlInsert)
            connPg.commit()
        except Exception as inst:
            print(inst)
            connPg.rollback()
            pass
    end_time = datetime.now()
    tiempo_ejecucion = (end_time - start_time).total_seconds()
    logger.info("Tiempo total de ejecución almacenando datos: %s", tiempo_ejecucion)