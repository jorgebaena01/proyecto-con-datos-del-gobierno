import pandas as pd
import random
from sodapy import Socrata
from concurrent.futures import ThreadPoolExecutor
import random
import os


client = Socrata("www.datos.gov.co", '4Pc5rfFKWib9BUGqYVGaulRaP')

total_registros = int(client.get("sty3-c395", select="count(*)")[0]['count'])
num_muestras = 20000
limit_size = 50
numeros_aleatorios = random.sample(range(1, total_registros), num_muestras // limit_size)

df = pd.DataFrame()

# Función para obtener datos de la API en paralelo
def get_data(offset):
    try:
        results = client.get("sty3-c395", limit=limit_size, offset=offset, order="anno_inf ASC")
        return pd.DataFrame.from_records(results)
    except Exception as e:
        print(f"Error en offset {offset}: {e}")
        return pd.DataFrame()

# Ejecutar en paralelo con los hilos que tiene el pc multiplicados por 2

max_hilos=(os.cpu_count())*2
with ThreadPoolExecutor(max_workers=max_hilos) as executor:
    dfs = list(executor.map(get_data, numeros_aleatorios))

# Concatenar todos los DataFrames en uno solo
df = pd.concat(dfs, ignore_index=True)

print(f"Total de datos descargados: {len(df)}")

df.to_csv('out_definitivo.csv')