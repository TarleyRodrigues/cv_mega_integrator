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
                    mask = df[source_col].notnull() & (df[source_col] != "")
                    # Extrai números de "301A" -> "301"
                    series = df.loc[mask, source_col].astype(
                        str).str.extract(r'(\d+)')[0].fillna("")

                    if rule == "slice_andar":
                        df.loc[mask, col_name] = series.apply(
                            lambda x: float(
                                x[:-2]) if len(x) > 2 else float(x if x else 0)
                        )
                    elif rule == "slice_coluna":
                        df.loc[mask, col_name] = series.apply(
                            lambda x: float(
                                x[-2:]) if len(x) >= 2 else float(x if x else 0)
                        )
        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: str):
        """
        Exportação Final: Decimal como VÍRGULA e sem separador de milhar.
        """
        # Criamos uma cópia para não formatar o DF original
        export_df = df.copy()

        # Garante que colunas numéricas não tenham .00000000001 (resíduos de float)
        # e formata para o CSV com vírgula usando os parâmetros do to_csv
        export_df.to_csv(
            output_path,
            index=False,
            sep=self.settings.get("csv_delimiter", ";"),
            encoding=self.settings.get("encoding_target", "utf-8-sig"),
            decimal=',',         # <--- AQUI: Transforma 103.22 em 103,22
            # <--- Mantém 4 casas decimais fixas (sem notação científica)
            float_format='%.4f'
        )
