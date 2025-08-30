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
os.makedirs('data/banxico/csv', exist_ok=True)
os.makedirs('data/banxico/json', exist_ok=True)

os.makedirs('data/inegi/csv', exist_ok=True)
os.makedirs('data/inegi/json', exist_ok=True)

os.makedirs('data/fred/csv', exist_ok=True)
os.makedirs('data/fred/json', exist_ok=True)

# Configuramos el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/macro_logs.log', encoding='utf-8'),
        logging.StreamHandler()  # Para imprimir en pantalla los logs
    ]
)


fred_api_key = os.getenv('FRED_API_KEY')
inegi_banco_token = os.getenv('INEGI_BANCO_TOKEN')
banxico_token = os.getenv('BANXICO_API_TOKEN')

banxico_series = ['SF43783']
inegi_series =  ['737121', '737219', '740933'] # IGAE serie original, IGAE desestacionalizada, Consumo privado
fred_series = ['FEDFUNDS']



if __name__=='__main__':
    
    # Para tener referencia de donde comienza a correr el script
    logging.info("Comenzando rutina de descarga de datos")

    try:
        fred = FREDConnector(fred_api_key)
        inegi = INEGIConnector(inegi_banco_token)
        banxico = BanxicoConnector(banxico_token)

        # Series de Banxico
        for serie in banxico_series:
            data, metadata = banxico.get_banxico_data(serie)
            if not data.empty:
                BanxicoConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")

        # Series de INEGI
        for serie in inegi_series:
            data, metadata = inegi.get_inegi_data(serie)
            if not data.empty:
                INEGIConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")
        
        # Series de FRED
        for serie in fred_series:
            data, metadata = fred.get_fred_data(serie)
            if not data.empty:
                FREDConnector.save_data(data[serie], metadata)
            else:
                logging.warning(f"No se obtuvieron datos para la serie {serie}")

        logging.info("Rutina de obtención de datos realizada con éxito")

    except Exception as e:
        logging.error(f"Error en el código: {e}")
        raise

