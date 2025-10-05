import pandas as pd
from modules.utils.logger import setup_logger

class SchemaMapper:
    """Mapeia diferentes schemas de IPTU para um schema padrão unificado"""
    
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        
        # Schema padrão unificado (normalizado)
        self.standard_schema = {
            "numero_contribuinte": "text",
            "ano_exercicio": "int",
            "data_cadastramento": "timestamp",
            "tipo_contribuinte": "text",
            "cpf_cnpj": "text",
            "logradouro": "text",
            "numero": "text",
            "complemento": "text",
            "bairro": "text",
            "cidade": "text",
            "estado": "text",
            "fracao_ideal": "text",
            "area_terreno": "float",
            "area_construida": "float",
            "area_ocupada": "float",
            "valor_m2_terreno": "float",
            "valor_m2_construcao": "float",
            "ano_construcao": "int",
            "qtd_pavimentos": "int",
            "tipo_uso_imovel": "text",
            "tipo_padrao_construcao": "text",
            "fator_obsolescencia": "float",
            "ano_mes_inicio_contribuicao": "text",
            "valor_total_imovel": "float",
            "valor_iptu": "float",
            "cep": "text",
            "regime_tributacao_iptu": "text",
            "regime_tributacao_trsd": "text",
            "tipo_construcao": "text",
            "tipo_empreendimento": "text",
            "tipo_estrutura": "text",
            "codigo_logradouro": "text"
        }
        
        # Mapeamento de colunas antigas para novas (normalizado para lowercase)
        self.column_mapping = {
            # IDs e identificadores
            "_id": None,
            "numero_do_contribuinte": "numero_contribuinte",
            "ano_do_exercicio": "ano_exercicio",
            "data_do_cadastramento": "data_cadastramento",
            "tipo_de_contribuinte": "tipo_contribuinte",
            "cpf/cnpj_mascarado_do_contribuinte": "cpf_cnpj",
            
            # Endereço
            "logradouro": "logradouro",
            "numero": "numero",
            "complemento": "complemento",
            "bairro": "bairro",
            "cidade": "cidade",
            "estado": "estado",
            "cep": "cep",
            "codigo_logradouro": "codigo_logradouro",
            
            # Áreas e medidas
            "fracao_ideal": "fracao_ideal",
            "area_terreno": "area_terreno",
            "area_construida": "area_construida",
            "area_ocupada": "area_ocupada",
            
            # Valores
            "valor_do_m2_do_terreno": "valor_m2_terreno",
            "valor_do_m2_de_construcao": "valor_m2_construcao",
            "valor_total_do_imovel_estimado": "valor_total_imovel",
            "valor_iptu": "valor_iptu",
            "valor_cobrado_de_iptu": "valor_iptu",
            
            # Características do imóvel
            "ano_da_construcao_corrigido": "ano_construcao",
            "quant_pavimentos": "qtd_pavimentos",
            "quantidade_de_pavimentos": "qtd_pavimentos",
            "tipo_de_uso_do_imovel": "tipo_uso_imovel",
            "tipo_de_padrao_da_construcao": "tipo_padrao_construcao",
            "tipo_de_construcao": "tipo_construcao",
            "tipo_de_empreendimento": "tipo_empreendimento",
            "tipo_de_estrutura": "tipo_estrutura",
            "fator_de_obsolescencia": "fator_obsolescencia",
            
            # Tributação
            "regime_de_tributacao_do_iptu": "regime_tributacao_iptu",
            "regime_de_tributacao_da_trsd": "regime_tributacao_trsd",
            "ano_e_mes_de_inicio_da_contribuicao": "ano_mes_inicio_contribuicao"
        }

    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mapeia colunas do DataFrame para o schema padrão
        """
        if df.empty:
            self.logger.warning("DataFrame vazio recebido")
            return df
        
        self.logger.info(f"Mapeando {len(df)} registros com {len(df.columns)} colunas")
        self.logger.info(f"Colunas originais: {list(df.columns)[:5]}...")
        
        # Criar novo DataFrame com schema padrão
        mapped_df = pd.DataFrame()
        
        # Mapear cada coluna
        for old_col in df.columns:
            # Converter coluna para string (pode ser int em JSONs)
            old_col_str = str(old_col)
            
            # Normalizar nome da coluna para busca
            normalized = old_col_str.lower().strip().replace(" ", "_")
            normalized = normalized.replace("ç", "c").replace("õ", "o")
            normalized = normalized.replace("é", "e").replace("á", "a")
            normalized = normalized.replace("ã", "a").replace("í", "i")
            normalized = normalized.replace("ú", "u").replace("ê", "e")
            normalized = normalized.replace("ô", "o")
            
            if normalized in self.column_mapping:
                new_col = self.column_mapping[normalized]
                if new_col is not None:
                    mapped_df[new_col] = df[old_col]
                else:
                    self.logger.debug(f"Coluna '{old_col_str}' ignorada (mapeada para None)")
            else:
                self.logger.warning(f"Coluna '{old_col_str}' não encontrada no mapeamento, mantendo nome normalizado")
                mapped_df[normalized] = df[old_col]
        
        # Adicionar colunas faltantes com valores nulos
        for col in self.standard_schema.keys():
            if col not in mapped_df.columns:
                mapped_df[col] = None
                self.logger.debug(f"Coluna '{col}' adicionada com valores nulos")
        
        # Reordenar colunas conforme schema padrão
        mapped_df = mapped_df[list(self.standard_schema.keys())]
        
        self.logger.info(f"✓ Mapeamento concluído: {len(mapped_df)} registros, {len(mapped_df.columns)} colunas padrão")
        
        return mapped_df
    
    def get_schema_info(self) -> dict:
        """Retorna informações sobre o schema padrão"""
        return {
            "total_columns": len(self.standard_schema),
            "columns": list(self.standard_schema.keys()),
            "types": self.standard_schema
        }