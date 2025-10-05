import pandas as pd
import re
from modules.utils.logger import setup_logger

logger = setup_logger("Harmonizer")


class Harmonizer:
    """
    Harmoniza e normaliza dados após o mapeamento de schema.
    """
    
    def __init__(self):
        pass
    
    def _harmonize_cep(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza CEPs extraindo e formatando corretamente.
        """
        if 'cep' not in df.columns:
            return df
        
        logger.debug("Harmonizando CEPs...")
        
        # Processar em chunks
        chunk_size = 50000
        for start in range(0, len(df), chunk_size):
            end = min(start + chunk_size, len(df))
            
            # Converter para string primeiro, depois processar
            chunk = df.loc[start:end-1, 'cep'].astype(str)
            chunk = chunk.str.replace(r'\D', '', regex=True)  # Remove não-dígitos
            chunk = chunk.str.zfill(8)  # Preenche com zeros à esquerda
            
            # Validar CEPs (8 dígitos)
            chunk = chunk.where(chunk.str.len() == 8, None)
            
            # Manter como string (não converter para int)
            df.loc[start:end-1, 'cep'] = chunk
        
        return df
    
    def _harmonize_monetary_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza valores monetários removendo símbolos e convertendo para float.
        """
        monetary_columns = ['valor_m2_terreno', 'valor_m2_construcao', 'valor_total']
        
        for col in monetary_columns:
            if col in df.columns:
                logger.debug(f"Harmonizando valores monetários: {col}")
                
                # Processar em chunks para melhor performance
                chunk_size = 50000
                for start in range(0, len(df), chunk_size):
                    end = min(start + chunk_size, len(df))
                    
                    # Converter explicitamente para string primeiro
                    chunk = df.loc[start:end-1, col].astype(str).str.replace(r'[R$,\s]', '', regex=True)
                    chunk = pd.to_numeric(chunk, errors='coerce')
                    
                    # Usar .loc com valores já convertidos
                    df.loc[start:end-1, col] = chunk.astype('float64')
        
        return df
    
    def _harmonize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza campos de data para formato padrão.
        """
        date_columns = ['data_cadastramento']
        
        for col in date_columns:
            if col in df.columns:
                logger.debug(f"Harmonizando datas: {col}")
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                except Exception as e:
                    logger.warning(f"Erro ao converter {col} para data: {e}")
        
        return df
    
    def _harmonize_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza campos categóricos (padronização de valores).
        """
        categorical_mappings = {
            'tipo_contribuinte': {
                '1': 'Pessoa Física',
                '2': 'Pessoa Jurídica',
                '1.0': 'Pessoa Física',
                '2.0': 'Pessoa Jurídica'
            }
        }
        
        for col, mapping in categorical_mappings.items():
            if col in df.columns:
                logger.debug(f"Harmonizando categórico: {col}")
                # Converter para string primeiro
                df[col] = df[col].astype(str)
                # Aplicar mapeamento
                df[col] = df[col].replace(mapping)
        
        return df
    
    def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza campos de texto (uppercase, trim, etc).
        """
        text_columns = ['logradouro', 'bairro', 'cidade', 'complemento']
        
        for col in text_columns:
            if col in df.columns:
                logger.debug(f"Normalizando texto: {col}")
                # Converter para string, remover espaços extras, uppercase
                df[col] = df[col].astype(str).str.strip().str.upper()
                # Substituir múltiplos espaços por um único
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                # Substituir 'NAN' por vazio
                df[col] = df[col].replace('NAN', '')
        
        return df
    
    def _ensure_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Garante que todas as colunas tenham tipos consistentes para Parquet.
        """
        logger.info("Garantindo tipos consistentes para Parquet...")
        
        # Colunas que devem ser string
        string_columns = [
            'numero_contribuinte', 'tipo_contribuinte', 'cpf_cnpj',
            'logradouro', 'numero', 'complemento', 'bairro', 'cidade',
            'tipo_uso_imovel', 'tipo_padrao_construcao', 'tipo_construcao',
            'regime_tributacao_iptu', 'regime_tributacao_trsd', 'cep'
        ]
        
        # Colunas que devem ser numérico (float64)
        numeric_columns = [
            'ano_exercicio', 'fracao_ideal', 'area_terreno', 'area_construida',
            'valor_m2_terreno', 'valor_m2_construcao', 'ano_construcao',
            'fator_obsolescencia', 'valor_total'
        ]
        
        # Colunas que devem ser int64
        integer_columns = ['ano']
        
        # Converter strings (tratando NaN/None)
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                # Remove '.0' de números que viraram string
                df[col] = df[col].str.replace(r'\.0$', '', regex=True)
                # Substitui 'nan' por vazio
                df[col] = df[col].replace('nan', '')
        
        # Converter numéricos float
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        
        # Converter inteiros
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # Int64 permite NaN
        
        # Data de cadastramento
        if 'data_cadastramento' in df.columns:
            try:
                df['data_cadastramento'] = pd.to_datetime(df['data_cadastramento'], errors='coerce')
            except:
                pass
        
        logger.info("✓ Tipos de dados ajustados")
        
        return df
    
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executa todas as harmonizações no DataFrame.
        
        Args:
            df: DataFrame com schema mapeado
            
        Returns:
            DataFrame harmonizado
        """
        logger.info(f"Iniciando harmonização de {len(df)} registros")
        
        # Fazer cópia para não modificar original
        df = df.copy()
        
        # Executar harmonizações
        df = self._harmonize_cep(df)
        df = self._harmonize_monetary_values(df)
        df = self._harmonize_dates(df)
        df = self._harmonize_categorical(df)
        df = self._normalize_text_fields(df)
        
        # Garantir tipos consistentes
        df = self._ensure_types(df)
        
        logger.info(f"✓ Harmonização concluída: {len(df)} registros")
        
        return df