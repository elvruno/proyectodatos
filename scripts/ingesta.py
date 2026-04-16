import os
import shutil
import logging
import pandas as pd

if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    filename='logs/ingesta.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def realizar_ingesta():
    origen = "data/raw/riesgo_hipertension_dataset.csv" 
    destino = "data/raw/ingesta_final.csv"

    try:
        logging.info("Iniciando el proceso de ingesta automatizada")
        
        if os.path.exists(origen):
            df = pd.read_csv(origen)
            num_filas = len(df)
            shutil.copy(origen, destino)
            mensaje_exito = f"Ingesta exitosa se procesaron {num_filas} registros."
            logging.info(mensaje_exito)
            print(f"{mensaje_exito}")
        else:
            logging.error("No se encontró el archivo fuente en data/raw/")
            print("Archivo fuente no encontrado.")

    except Exception as e:
        logging.error(f"Error en el pipeline: {e}")

if __name__ == "__main__":
    realizar_ingesta()