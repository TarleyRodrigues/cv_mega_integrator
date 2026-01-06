# src/ui/input_handler.py

import json


class InputHandler:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def collect_manual_inputs(self) -> dict:
        """
        Varre o mapping e solicita input apenas para colunas 'manual'.
        """
        print("\n=== COLETA DE DADOS ESTÁTICOS (CV CRM) ===")
        manual_data = {}

        # Filtra todas as colunas que possuem origem 'manual'
        manual_cols = {k: v for k, v in self.mapping['columns'].items() if v.get(
            'origin') == 'manual'}

        for col_name, config in manual_cols.items():
            default_val = config.get("default")
            prompt = f"Informe [{col_name}]"

            if default_val:
                prompt += f" (Padrão: {default_val})"

            user_input = input(f"{prompt}: ").strip()

            # Se o usuário der enter vazio e houver um default, usa o default
            if not user_input and default_val:
                manual_data[col_name] = default_val
            else:
                manual_data[col_name] = user_input

        return manual_data

    @staticmethod
    def get_file_paths():
        """
        Solicita os caminhos dos arquivos via CLI.
        """
        mega_path = input(
            "\nCaminho do arquivo exportado do MEGA (Excel/CSV): ").strip().replace('"', '')
        output_name = input(
            "Nome do arquivo de saída (ex: carga_cv_01.csv): ").strip()
        return mega_path, output_name
