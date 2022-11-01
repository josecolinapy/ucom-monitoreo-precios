# UCOM MONITOREO DE PRECIOS DE PRODUCTOS
## TEMA:
Monitoreo de precios de productos de la canasta familiar en comercios método scraping
Trabajo Final para el Diplomado de Big data.    
## INTEGRANTES
- Gustavo Rodas
- José Colina

# MOTIVACION
La pandemia nos forzó a explorar nuevas formas de comprar con lo cual la mayoría de las empresas empezaron a promover sus productos de forma online. Eso nos permitió poder encontrar de forma online todos los productos de la canasta familiar de la mayoría de los comercios principales.
La crisis mundial producida por la guerra de ucrania y el crecimiento de la inflación tanto en Paraguay como a nivel mundial hace que tengamos que tener en cuenta los costos de los productos de la canasta familiar para poder mantener la misma calidad de vida.
Este proyecto plantea el monitoreo continuo de los costos de los productos de la canasta familiar para tener como ayuda y una recomendación sencilla.

## Presentación del trabajo práctico
- [Presentación del proyecto](doc/Poster-GustavoRodas-JoseColina.pptx)
## Tabla de contenidos
- [Software](#Software)


### Software
Para poder ejecutar el software en un equipo es necesario disponer
del siguiente software:

| Software | Versión |
| :------: |:-------:|
|  Python  | => 3.8  |
|   pip    |   \*    |

Dentro del directorio con el código del proceso (`src/app`) se encuentra un fichero `requerimientos.txt` que contiene las dependencias que necesita el script para
funcionar adecuadamente. Es recomendable crear un entorno virtual de Python[¹ ²](#referencias) (`virtualenv`), acceder al mismo e instalar dichas  dependencias mediante el comando `pip install -r requerimientos.txt` (recuerde ubicarse en su shell dentro del directorio `src/app`).
- Linux/MAC
```bash
  $ cd src/app
  $ python3 -m venv venv
  $ source venv/bin/activate
  (venv)$ pip install -r requerimientos.txt
```
- Windows
```bash
  $ cd src/app
  $ virtualenv venv
  $ venv/Scripts/activate
  (venv)$ pip install -r requerimientos.txt
```
- Motor de base de datos

|  Software  | Versión |
|:----------:|:-------:|
| Postgresql | => 9.6  |



## Referencias

1. Tutorial de virtualenv, en inglés
   https://docs.python-guide.org/dev/virtualenvs/#lower-level-virtualenv

2. Tutorial de virtualenv, en español
   https://rukbottoland.com/blog/tutorial-de-python-virtualenv/
