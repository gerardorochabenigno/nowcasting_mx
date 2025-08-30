# connector.py
"""Modulo con conectores que permiten descargar datos de Banxico, INEGI y FRED y guardar en la carpeta data"""

import pandas as pd
import requests
import os
from datetime import datetime
import json
from typing import Any
import logging

class INEGIConnector:
    """Clase para conectarse a la API del Banco de Indicadores del INEGI y obtener series de datos de este sitio"""

    def __init__(self, api_token):
        self.api_token = api_token

    def get_inegi_data(self, id_series)-> tuple[pd.DataFrame, dict[Any, Any]]:
        """Método para obtener datos de FRED
        
        Args:
            id_serie (str): ID de la serie de FRED a 
            
        Returns:
            tuple: tuple con (pd.DataFrame, dict[metadata])"""
        
        # Url para descargar los datos en un json
        url = f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/{id_series}/es/00/false/BIE/2.0/{self.api_token}?type=json"

        try:
            response = requests.get(url)
            data = response.json()['Series'][0]
            
            df = pd.DataFrame(data['OBSERVATIONS'])[['TIME_PERIOD', 'OBS_VALUE']]
            df = df.rename(columns={'TIME_PERIOD':'date', 'OBS_VALUE':'value'})
            df['date'] = pd.to_datetime(df['date'], format='%Y/%m')
            df['value'] = df['value'].str.replace(',', '')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.rename(columns={'value': id_series})
            df.set_index('date', inplace=True)
            df = df.sort_index()
            
            metadata = {k:v for k, v in data.items() if k not in ['OBSERVATIONS', 'INDICADOR', 'TOPIC', 'UNIT_MULT', 'NOTE', 'SOURCE', 'LASTUPDATE', 'STATUS']}
            metadata['observation_start'] = str(df.index[0])
            metadata['observation_end'] = str(df.index[-1])
            metadata['last_update'] = str(datetime.now())
            metadata['id_series'] = id_series
            metadata['frequency'] = metadata.pop('FREQ')
            metadata['unit'] = metadata.pop('UNIT')
            
            logging.info(f"Datos para serie {id_series} (INEGI) obtenidos con éxito")
            return (df, metadata)

        except Exception as e:
            logging.error(f"Problema al obtener los datos de {id_series} (INEGI): {e}")
            return (pd.DataFrame(), {})
    
    @staticmethod  
    def save_data(series:pd.Series, metadata:dict, path_csv:str='data/inegi/csv', path_json:str='data/inegi/json'):
        """Método para guardar los datos descargados del Banco de Indicadores del INEGI"""
        
        try:
            # Guardamos el df como csv
            series.to_csv(os.path.join(path_csv, f"{metadata['id_series']}.csv"))
            
            # Guardamos el JSON
            with open(os.path.join(path_json, f"{metadata['id_series']}.json"), 'w', encoding="utf-8") as file:
                json.dump(metadata, file, indent=4, ensure_ascii=False)

            logging.info(f"Datos de {metadata['id_series']} (INEGI) guardados con éxito")

        except Exception as e:
            logging.error(f"Error al guardar datos de serie {metadata['id_series']} (INEGI): {e}")

class FREDConnector:
    """Clase para conectarse a la API de FRED y obtener series de datos de este sitio"""

    def __init__(self, api_key):
        self.api_key = api_key

    def get_fred_data(self, id_series) -> tuple[pd.DataFrame, dict[Any, Any]]:
        """Método para obtener datos de FRED
        
        Args:
            id_serie (str): ID de la serie de FRED a 
            
        Returns:
            tuple: tuple con (pd.DataFrame, dict[metadata])"""
        
        # Url para descargar los datos en un json
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={id_series}&api_key={self.api_key}&file_type=json"

        try:
            response = requests.get(url)
            data = response.json()
            
            df = pd.DataFrame(data['observations'])[['date', 'value']]
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            df['value'] = df['value'].str.replace(',', '')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.rename(columns={'value': id_series})
            df.set_index('date', inplace=True)
            
            metadata = {k:v for k, v in data.items() if k in ['units', 'count']}
            metadata['observation_start'] = str(df.index[0])
            metadata['observation_end'] = str(df.index[-1])
            metadata['last_update'] = str(datetime.now())
            metadata['id_series'] = id_series

            logging.info(f"Datos para serie {id_series} (FRED) obtenidos con éxito")
            return (df, metadata)

        except Exception as e:
            logging.error(f"Problema al obtener los datos de {id_series} (FRED): {e}")
            return (pd.DataFrame(), {})
    
    @staticmethod  
    def save_data(series:pd.Series, metadata:dict, path_csv:str='data/fred/csv', path_json:str='data/fred/json'):
        """Método para guardar los datos descargados de FRED"""
        
        try:
            # Guardamos el df como csv
            series.to_csv(os.path.join(path_csv, f"{metadata['id_series']}.csv"))
            
            # Guardamos el JSON
            with open(os.path.join(path_json, f"{metadata['id_series']}.json"), 'w', encoding="utf-8") as file:
                json.dump(metadata, file, indent=4, ensure_ascii=False)

            logging.info(f"Datos de {metadata['id_series']} (FRED) guardados con éxito")
        
        except Exception as e:
            logging.error(f"Error al guardar datos de serie {metadata['id_series']} (FRED): {e}")

class BanxicoConnector:
    """Clase para conectarse a la API del SIE de Banxico y obtener series de datos de este sitio"""

    def __init__(self, api_token):
        self.api_token = api_token
        self.header = {
            'Bmx-Token': self.api_token,
        }
    def get_banxico_data(self, id_series:str) -> tuple[pd.DataFrame, dict[Any,Any]]:
            """Método para obtener una serie del SIE-Banxico
            
            Args:
                id_serie (str): ID de la serie a consultar
            
            Returns: 
                tuple: tupla con (df, metadata)
                """
            
            try:
                 # Nos conectamos con la API de Banxico para obtener los datos en un json
                banxico_url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{id_series}/datos"
                response = requests.get(banxico_url, headers=self.header)
                json = response.json()
                data = json['bmx']['series'][0]
                
                # Obtenemos el dataframe del json
                df = pd.DataFrame(data['datos'])
                df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
                df['dato'] = df['dato'].str.replace(',', '')
                df['dato'] = pd.to_numeric(df['dato'], errors='coerce')
                df.set_index('fecha', inplace=True)
                df = df.rename(columns={'dato':id_series})

                # obtenemos metadatos
                metadata = {k: v for k, v in data.items() if k != 'datos'}
                metadata['id_series'] = metadata.pop('idSerie')
                metadata['fecha_inicio'] = str(df.index[0])
                metadata['fecha_final'] = str(df.index[-1])
                metadata['ultima_actualizacion'] = str(datetime.now())

                logging.info(f"Datos para serie {id_series} (Banxico) obtenidos con éxito")
                return (df, metadata)
            
            except Exception as e:
                logging.error(f"Problema al obtener los datos de {id_series} (Banxico): {e}")
                return (pd.DataFrame(), {})
            
    @staticmethod  
    def save_data(series:pd.Series, metadata:dict, path_csv:str='data/banxico/csv', path_json:str='data/banxico/json'):
        """Método para guardar los datos descargados del Banco de Indicadores del INEGI"""
        
        try:
            # Guardamos el df como csv
            series.to_csv(os.path.join(path_csv, f"{metadata['id_series']}.csv"))
            
            # Guardamos el JSON
            with open(os.path.join(path_json, f"{metadata['id_series']}.json"), 'w', encoding="utf-8") as file:
                json.dump(metadata, file, indent=4, ensure_ascii=False)

            logging.info(f"Datos de {metadata['id_series']} (Banxico) guardados con éxito")
        
        except Exception as e:
         logging.error(f"Error al guardar datos de serie {metadata['id_series']} (Banxico): {e}")