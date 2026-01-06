# path/to/src/services/data_processor.py
import pandas as pd
import numpy as np
import re


class DataProcessor:
    def __init__(self, mapping: dict):
        self.mapping = mapping
        self.columns_config = mapping.get("columns", {})
        self.settings = mapping.get("settings", {})

    def process(self, mega_df: pd.DataFrame, manual_inputs: dict) -> pd.DataFrame:
        target_cols = list(self.columns_config.keys())
        cv_df = pd.DataFrame(columns=target_cols, index=range(len(mega_df)))

        # Keywords para identificar colunas numéricas no destino
        numeric_keywords = ['Área', 'Fração',
                            'Valor', 'Vagas', 'm²', 'Andar', 'Coluna']

        for col_name, config in self.columns_config.items():
            origin = config.get("origin")

            if origin == "manual":
                cv_df[col_name] = manual_inputs.get(
                    col_name, config.get("default"))

            elif origin == "mega":
                mega_col = config.get("mega_column")
                if mega_col in mega_df.columns:
                    is_num_field = any(
                        kw in col_name for kw in numeric_keywords)
                    if is_num_field:
                        cv_df[col_name] = self._handle_numeric(
                            mega_df[mega_col])
                    else:
                        cv_df[col_name] = self._handle_text(mega_df[mega_col])
                else:
                    cv_df[col_name] = config.get("default")

            elif origin == "empty":
                cv_df[col_name] = config.get("default")

        # Aplica Andar e Coluna baseando-se no Nome (Unidade) já limpo
        cv_df = self._apply_logical_rules(cv_df)
        return cv_df

    def _handle_text(self, series: pd.Series) -> pd.Series:
        """Mantém o texto original (ex: 301A) sem conversões."""
        return series.astype(str).str.strip().replace('nan', '')

    def _handle_numeric(self, series: pd.Series) -> pd.Series:
        """Converte para float internamente para cálculos e limpeza."""
        def clean_val(val):
            if pd.isna(val) or val == "":
                return 0.0
            if isinstance(val, (int, float)):
                return float(val)

            val_str = str(val).strip()
            # Se vier do MEGA como "1.250,50" -> 1250.50
            if "." in val_str and "," in val_str:
                val_str = val_str.replace(".", "").replace(",", ".")
            # Se vier como "12,00" -> 12.00
            elif "," in val_str:
                val_str = val_str.replace(",", ".")

            try:
                return float(val_str)
            except:
                return 0.0

        return series.apply(clean_val)

    def _apply_logical_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        for col_name, config in self.columns_config.items():
            if config.get("origin") == "logical":
                source_col = config.get("source_col")
                rule = config.get("rule")

                if source_col in df.columns:
                    # Filtra apenas o que não é nulo e não é vazio
                    mask = df[source_col].notnull() & (df[source_col] != "")

                    # Extrai apenas os dígitos (301A -> 301)
                    series = df.loc[mask, source_col].astype(
                        str).str.extract(r'(\d+)')[0].fillna("")

                    if rule == "slice_andar":
                        # Se len > 2 (ex: 301), tira os dois últimos e sobra o andar
                        df.loc[mask, col_name] = series.apply(
                            lambda x: int(
                                x[:-2]) if len(x) > 2 else (int(x) if x else 0)
                        )
                    elif rule == "slice_coluna":
                        # Pega apenas os dois últimos
                        df.loc[mask, col_name] = series.apply(
                            lambda x: int(
                                x[-2:]) if len(x) >= 2 else (int(x) if x else 0)
                        )

                    # FORÇA O TIPO INTEIRO (Nullable)
                    df[col_name] = df[col_name].astype('Int64')

        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: str):
        """
        Exportação Final Inteligente:
        Inteiros ficam como inteiros (3), Floats ficam com vírgula e 4 casas (103,2200).
        """
        export_df = df.copy()

        # Antes de exportar, colunas que são Int64 e não tem nulos podem ser
        # convertidas para string para garantir que o float_format não as afete
        for col in export_df.columns:
            if str(export_df[col].dtype) == 'Int64':
                # Converte para string removendo o .0 caso o Pandas tente forçar
                export_df[col] = export_df[col].astype(str).replace('<NA>', '')

        export_df.to_csv(
            output_path,
            index=False,
            sep=self.settings.get("csv_delimiter", ";"),
            encoding=self.settings.get("encoding_target", "utf-8-sig"),
            decimal=',',
            float_format='%.4f'  # Isso agora só afetará as Áreas e Fração Ideal
        )
