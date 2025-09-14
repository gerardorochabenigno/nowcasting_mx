import pandas as pd
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import json
from src.connector import FREDConnector, INEGIConnector, BanxicoConnector
from typing import Any
import logging

# Leemos variables del .env
load_dotenv()

# Creamos carpeta del log
os.makedirs('logs', exist_ok=True)

# Creamos carpetas de datos
os.makedirs('data/raw/banxico/csv', exist_ok=True)
os.makedirs('data/raw/banxico/json', exist_ok=True)

os.makedirs('data/raw/inegi/csv', exist_ok=True)
os.makedirs('data/raw/inegi/json', exist_ok=True)

os.makedirs('data/raw/fred/csv', exist_ok=True)
os.makedirs('data/raw/fred/json', exist_ok=True)

# Configuramos el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/macro_logs.log', encoding='utf-8'),
        logging.StreamHandler()  # Para imprimir en pantalla los logs
    ]
)


# Series de INEGI
inegi_series =  {
    '736181':'pib_inegi',  # Variable objetivo, desestacionalizada inegi
    '737219':'igae_inegi', # desestaconalizada inegi
    '740987':'consumo_privado_inegi', # desestacionalizada-inegi
    '736885':'actividad_industrial_inegi', # desestacionalizada-inegi
    '736969':'ind_manufacturera_inegi', # desestacionalizada-inegi
    '736941':'construccion_inegi', # desestacionalizada-inegi
    '214307':'indicador_adelantado_inegi', # sin estacionalizar
    '718508':'emec_inegi', #comercio al por mayor - sin desestacionalizar
    '214313':'bolsa_mexicana_inegi', # sin estacionalizar
    '736472':'cemento_inegi', # sin estacionalizar
    '214317':'tiie_inegi', # sin estacionalizar
    '736478':'aluminio_inegi', # sin estacionalizar
    '736514':'refacciones_inegi', # sin estacionalizar
    '736412':'elec_agua_gas_ductos_inegi', # sin estacionalizar # Corroborar
    '736516':'equipo_ferroviario' # sin estacionalizar
}

# Series de la FED
fred_series = {
    'INDPRO':'produccion_industrial_fred', # desestacionalizada fred
}

# Series de Banxico
banxico_series = {
    'SE36595':'importaciones_banxico', # sin estacionalizar
    'SE35398':'exportaciones_manufac_banxico', # sin estacionalizar # https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do?accion=consultarCuadro&idCuadro=CE125&sector=1&locale=es
    'SE36593':'exportaciones_total_banxico', # sin estacionalizar
    'SF311438':'m4_banxico', # sin estacionalizar
}

fred_api_key = os.getenv('FRED_API_KEY')
inegi_banco_token = os.getenv('INEGI_BANCO_TOKEN')
banxico_token = os.getenv('BANXICO_API_TOKEN')

if __name__=='__main__':
    
    # Para tener referencia de donde comienza a correr el script
    logging.info("Comenzando rutina de descarga de datos")

    try:
        fred = FREDConnector(fred_api_key)
        inegi = INEGIConnector(inegi_banco_token)
        banxico = BanxicoConnector(banxico_token)

        # Series de Banxico
        for serie in banxico_series.keys():
            data, metadata = banxico.get_banxico_data(serie)
            if not data.empty:
                BanxicoConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")

        # Series de INEGI
        for serie in inegi_series.keys():
            data, metadata = inegi.get_inegi_data(serie)
            if not data.empty:
                INEGIConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")
        
        # Series de FRED
        for serie in fred_series.keys():
            data, metadata = fred.get_fred_data(serie)
            if not data.empty:
                FREDConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")

        logging.info("Rutina de obtención de datos realizada con éxito")

    except Exception as e:
        logging.error(f"Error en el código: {e}")
        raise

