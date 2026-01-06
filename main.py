# main.py

import os
import json
import pandas as pd
from src.services.data_processor import DataProcessor
from src.ui.input_handler import InputHandler


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado em: {config_path}")
    with open(config_path, 'r', encoding='utf-8-sig') as f:  # Adicionado 'sig' aqui
        return json.load(f)


def load_mega_data(file_path: str) -> pd.DataFrame:
    """
    Carrega o arquivo do MEGA, suportando Excel ou CSV.
    """
    if file_path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_path)
    elif file_path.endswith('.csv'):
        # Tenta ler com separador padrão brasileiro, caso falhe usa o do sistema
        try:
            return pd.read_csv(file_path, sep=';', encoding='latin1')
        except:
            return pd.read_csv(file_path, sep=',', encoding='utf-8')
    else:
        raise ValueError("Formato de arquivo não suportado. Use Excel ou CSV.")


def main():
    try:
        # 1. Setup inicial e carga de configuração
        config = load_config("config/mapping.json")
        ui = InputHandler(config)
        processor = DataProcessor(config)

        # 2. Coleta de caminhos e dados manuais
        mega_file_path, output_filename = ui.get_file_paths()
        manual_inputs = ui.collect_manual_inputs()

        # 3. Extração (Extract)
        print("\n[1/3] Lendo dados do MEGA ERP...")
        df_mega = load_mega_data(mega_file_path)

        # 4. Transformação (Transform)
        print("[2/3] Aplicando mapeamento e regras lógicas...")
        df_final = processor.process(df_mega, manual_inputs)

        # 5. Carga (Load)
        output_path = os.path.join("data/output", output_filename)
        os.makedirs("data/output", exist_ok=True)

        print(f"[3/3] Exportando CSV para: {output_path}")
        processor.save_to_csv(df_final, output_path)

        print("\n✅ Processo concluído com sucesso!")

    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {str(e)}")


if __name__ == "__main__":
    main()
