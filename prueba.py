import pandas as pd
import matplotlib.pyplot as plt
import random
from sodapy import Socrata
from concurrent.futures import ThreadPoolExecutor
import os

class DataExtractor:
    def __init__(self, precision):
        opciones = ['alta', 'media', 'baja']
        if precision not in opciones:
            raise ValueError(f'Precisión inválida: {precision}, debe ser una de {opciones}')
        
        self.precision = precision
        self.client = Socrata("www.datos.gov.co", '4Pc5rfFKWib9BUGqYVGaulRaP')
        self.total_registros = int(self.client.get("sty3-c395", select="count(*)")[0]['count'])
        self.limit_size = 50
        self.num_muestras = self._determinar_muestras()
        self.numeros_aleatorios = random.sample(range(1, self.total_registros), self.num_muestras // self.limit_size)
        self.df = pd.DataFrame()
    
    def _determinar_muestras(self):
        if self.precision == 'alta':
            return 20000
        elif self.precision == 'media':
            return 10000
        else:
            return 5000
    
    def _get_data(self, offset):
        """Obtiene datos de la API en paralelo."""
        try:
            results = self.client.get("sty3-c395", limit=self.limit_size, offset=offset, order="anno_inf ASC")
            return pd.DataFrame.from_records(results)
        except Exception as e:
            print(f"Error en offset {offset}: {e}")
            return pd.DataFrame()
    
    def fetch_data(self):
        """Descarga los datos en paralelo y los almacena en un DataFrame."""
        max_hilos = os.cpu_count() * 2
        with ThreadPoolExecutor(max_workers=max_hilos) as executor:
            dfs = list(executor.map(self._get_data, self.numeros_aleatorios))
        
        self.df = pd.concat(dfs, ignore_index=True)
        print(f"Total de datos descargados: {len(self.df)}")
    
    def save_to_csv(self, filename='out_definitivo.csv'):
        """Guarda el DataFrame en un archivo CSV."""
        if self.df.empty:
            print("No hay datos para guardar. Ejecuta fetch_data() primero.")
        else:
            self.df.to_csv(filename, index=False)
            print(f"Datos guardados en {filename}")

''' Uso de la clase'''
extractor = DataExtractor(precision='baja')
#extractor.fetch_data()
extractor.save_to_csv()