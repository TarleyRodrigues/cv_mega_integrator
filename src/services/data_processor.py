# src/services/data_processor.py

import pandas as pd
import numpy as np


class DataProcessor:
    def __init__(self, mapping: dict):
        self.mapping = mapping
        self.columns_config = mapping.get("columns", {})
        self.settings = mapping.get("settings", {})

    def process(self, mega_df: pd.DataFrame, manual_inputs: dict) -> pd.DataFrame:
        """
        Executa a transformação central: Mega ERP -> CV CRM Layout.
        """
        # 1. Inicializa o DataFrame de destino com as colunas do mapping
        target_cols = list(self.columns_config.keys())
        cv_df = pd.DataFrame(columns=target_cols, index=range(len(mega_df)))

        # 2. Processamento por tipo de origem
        for col_name, config in self.columns_config.items():
            origin = config.get("origin")

            if origin == "manual":
                cv_df[col_name] = manual_inputs.get(
                    col_name, config.get("default"))

            elif origin == "mega":
                mega_col = config.get("mega_column")
                if mega_col in mega_df.columns:
                    cv_df[col_name] = self._clean_mega_data(mega_df[mega_col])
                else:
                    cv_df[col_name] = config.get("default")

            elif origin == "empty":
                cv_df[col_name] = config.get("default")

        # 3. Processamento de Camada Lógica (Andar e Coluna)
        # Deve ser feito APÓS o preenchimento das colunas 'mega' pois depende do 'Nome (Unidade)'
        cv_df = self._apply_logical_rules(cv_df)

        return cv_df

    def _clean_mega_data(self, series: pd.Series) -> pd.Series:
        """
        Trata tipagem: converte decimais brasileiros (,) para padrão system (.) 
        e limpa espaços em branco.
        """
        series = series.astype(str).str.strip()

        # Se for uma coluna numérica vinda do MEGA (contém vírgula como decimal)
        # Substitui vírgula por ponto apenas onde faz sentido para conversão posterior
        if self.settings.get("decimal_sep_source") == ",":
            # Regex básica para identificar se a string parece um número decimal BR
            # Remove separador de milhar se houver
            series = series.str.replace('.', '', regex=False)
            series = series.str.replace(',', '.', regex=False)

        return series

    def _apply_logical_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica as regras de slicing de string baseadas no mapping.json.
        """
        for col_name, config in self.columns_config.items():
            if config.get("origin") == "logical":
                source_col = config.get("source_col")
                rule = config.get("rule")

                if source_col in df.columns:
                    # Garantir que é string para o slice
                    series = df[source_col].astype(str)

                    if rule == "slice_0_2":
                        # Ex: 0401A -> 04 -> 4
                        df[col_name] = series.str[0:2].apply(
                            lambda x: str(int(x)) if x.isdigit() else x)

                    elif rule == "slice_2_4":
                        # Ex: 0401A -> 01 -> 1
                        df[col_name] = series.str[2:4].apply(
                            lambda x: str(int(x)) if x.isdigit() else x)

        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: str):
        """
        Exporta o resultado final seguindo os padrões do CV CRM.
        """
        df.to_csv(
            output_path,
            index=False,
            sep=self.settings.get("csv_delimiter", ";"),
            encoding=self.settings.get("encoding_target", "utf-8")
        )
