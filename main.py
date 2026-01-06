# main.py
import os
import json
import pandas as pd
from src.services.data_processor import DataProcessor
from src.ui.gui_handler import AppGUI


def load_config(config_path: str) -> dict:
    with open(config_path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)


def load_mega_data(file_path: str) -> pd.DataFrame:
    if file_path.endswith(('.xlsx', '.xls')):
        engine = 'xlrd' if file_path.endswith('.xls') else None
        # Lemos sem converter tipos automaticamente para não perder precisão
        return pd.read_excel(file_path, engine=engine)


def run_integration(mega_path, manual_inputs, save_path):
    """
    Esta função é chamada pela GUI quando o usuário clica em 'Processar'.
    """
    config = load_config("config/mapping.json")
    processor = DataProcessor(config)

    df_mega = load_mega_data(mega_path)
    df_final = processor.process(df_mega, manual_inputs)

    processor.save_to_csv(df_final, save_path)


if __name__ == "__main__":
    try:
        config_data = load_config("config/mapping.json")
        app = AppGUI(config_data, run_integration)
        app.run()
    except Exception as e:
        print(f"Erro ao iniciar aplicação: {e}")
