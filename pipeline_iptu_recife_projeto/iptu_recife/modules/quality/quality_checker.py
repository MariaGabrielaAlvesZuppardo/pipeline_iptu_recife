import pandas as pd
import numpy as np
from typing import Tuple, List, Dict
from modules.utils.logger import setup_logger

logger = setup_logger("quality_checker")


class QualityChecker:
    """
    Valida qualidade dos dados BRUTOS (antes de qualquer normalização/processamento).
    
    Verifica:
    - Completude: campos obrigatórios não podem ser nulos/vazios
    - Consistência: valores devem estar em ranges válidos
    - Formato: tipos de dados esperados
    """
    
    def __init__(self):
        # Campos obrigatórios (nomes originais dos CSVs/JSONs, antes do mapeamento)
        self.required_fields_patterns = {
            # Padrões baseados nos logs reais
            "contribuinte": [
                "número do contribuinte", "numero do contribuinte",
                "contribuinte", "num_contribuinte", "cod_contribuinte",
                "inscricao", "inscr"
            ],
            "ano": [
                "ano do exercício", "ano do exercicio",
                "ano", "exercicio", "ano_exercicio", "anoexercicio"
            ],
            "logradouro": [
                "logradouro", "endereco", "rua", "end", 
                "nom_logradouro", "tipo_logradouro", "lograd",
                "código logradouro", "codigo logradouro"
            ],
            "bairro": [
                "bairro", "distrito", "nom_bairro"
            ],
            "cidade": [
                "cidade", "municipio", "nom_municipio"
            ],
            "valor_total": [
                "valor total do imóvel estimado", "valor total do imovel estimado",
                "valor", "valor_total", "valor_venal", "valor_imovel",
                "vl_total", "vlr_total", "val_total"
            ],
            "valor_iptu": [
                "iptu", "valor_iptu", "imposto", "valor_devido",
                "vl_iptu", "vlr_iptu", "val_iptu",
                "regime de tributação do iptu", "regime de tributacao do iptu"
            ]
        }
        
        # Regras de validação
        self.validation_rules = {
            "ano_min": 2000,
            "ano_max": 2025,
            "valor_min": 0,
            "valor_max": 100_000_000  # 100 milhões (limite razoável para IPTU)
        }
    
    def _find_column(self, df: pd.DataFrame, field_patterns: List[str]) -> str:
        """
        Encontra a coluna correspondente no DataFrame baseado em padrões.
        Usa busca flexível: busca parcial e case-insensitive.
        """
        for pattern in field_patterns:
            # Normaliza pattern para lowercase
            pattern_lower = pattern.lower()
            
            # Busca exata primeiro (mais confiável)
            for col in df.columns:
                # Converter coluna para string (pode ser int em JSONs)
                col_str = str(col).lower()
                if col_str == pattern_lower:
                    return col
            
            # Busca parcial (contém o padrão)
            for col in df.columns:
                col_str = str(col).lower()
                # Remove underscores e espaços para comparação mais flexível
                col_normalized = col_str.replace('_', '').replace(' ', '')
                pattern_normalized = pattern_lower.replace('_', '').replace(' ', '')
                
                if pattern_normalized in col_normalized:
                    return col
        
        return None
    
    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Detecta automaticamente as colunas relevantes no DataFrame bruto.
        """
        column_mapping = {}
        
        for field_key, patterns in self.required_fields_patterns.items():
            found_col = self._find_column(df, patterns)
            if found_col:
                column_mapping[field_key] = found_col
                logger.debug(f"Campo '{field_key}' mapeado para coluna '{found_col}'")
            else:
                logger.warning(f"Campo '{field_key}' não encontrado no DataFrame")
        
        return column_mapping
    
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valida o DataFrame bruto e retorna dados válidos e inválidos separadamente.
        
        Args:
            df: DataFrame bruto (antes de qualquer transformação)
            
        Returns:
            Tuple[df_valid, df_invalid]
        """
        if df.empty:
            logger.warning("DataFrame vazio recebido para validação")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"Iniciando validação de {len(df)} registros")
        
        # Detectar colunas automaticamente
        col_map = self._detect_columns(df)
        
        # Criar cópia para trabalhar
        df_work = df.copy()
        
        # Adicionar coluna de motivos de invalidação
        df_work['_validation_errors'] = ''
        
        # Lista de máscaras booleanas para registros válidos
        valid_masks = []
        
        # 1. VALIDAÇÃO DE COMPLETUDE
        logger.info("Verificando completude dos campos obrigatórios...")
        for field_key, col_name in col_map.items():
            if col_name:
                # Verifica nulos e strings vazias
                null_mask = df_work[col_name].isnull()
                empty_mask = (df_work[col_name].astype(str).str.strip() == '')
                incomplete_mask = null_mask | empty_mask
                
                # Registra erros
                df_work.loc[incomplete_mask, '_validation_errors'] += f"{field_key}_missing;"
                
                # Adiciona máscara de válidos
                valid_masks.append(~incomplete_mask)
        
        # 2. VALIDAÇÃO DE CONSISTÊNCIA - ANO
        if 'ano' in col_map and col_map['ano']:
            logger.info("Verificando consistência do ano...")
            ano_col = col_map['ano']
            try:
                # Converte para numérico, tratando erros
                ano_numeric = pd.to_numeric(df_work[ano_col], errors='coerce')
                
                invalid_year = (ano_numeric < self.validation_rules['ano_min']) | \
                              (ano_numeric > self.validation_rules['ano_max']) | \
                              ano_numeric.isnull()
                
                df_work.loc[invalid_year, '_validation_errors'] += 'ano_invalido;'
                valid_masks.append(~invalid_year)
            except Exception as e:
                logger.error(f"Erro ao validar ano: {e}")
        
        # 3. VALIDAÇÃO DE CONSISTÊNCIA - VALORES MONETÁRIOS
        monetary_fields = ['valor_total', 'valor_iptu']
        for field in monetary_fields:
            if field in col_map and col_map[field]:
                logger.info(f"Verificando consistência de {field}...")
                val_col = col_map[field]
                try:
                    # Limpa e converte valores
                    # Remove símbolos comuns de moeda e espaços
                    val_cleaned = df_work[val_col].astype(str).str.replace(r'[R$,\s]', '', regex=True)
                    val_numeric = pd.to_numeric(val_cleaned, errors='coerce')
                    
                    invalid_value = (val_numeric < self.validation_rules['valor_min']) | \
                                   (val_numeric > self.validation_rules['valor_max']) | \
                                   val_numeric.isnull()
                    
                    df_work.loc[invalid_value, '_validation_errors'] += f'{field}_invalido;'
                    valid_masks.append(~invalid_value)
                except Exception as e:
                    logger.error(f"Erro ao validar {field}: {e}")
        
        # 4. APLICAR TODAS AS MÁSCARAS
        if valid_masks:
            # Registro é válido apenas se passar em TODAS as validações
            final_valid_mask = pd.Series(True, index=df_work.index)
            for mask in valid_masks:
                final_valid_mask &= mask
        else:
            logger.warning("Nenhuma validação pôde ser aplicada")
            final_valid_mask = pd.Series(True, index=df_work.index)
        
        # Separar válidos e inválidos
        df_valid = df.loc[final_valid_mask].copy()
        df_invalid = df.loc[~final_valid_mask].copy()
        
        # Adicionar motivos aos inválidos
        if not df_invalid.empty:
            df_invalid['validation_errors'] = df_work.loc[~final_valid_mask, '_validation_errors']
        
        # Estatísticas
        valid_count = len(df_valid)
        invalid_count = len(df_invalid)
        quality_rate = (valid_count / len(df)) * 100 if len(df) > 0 else 0
        
        logger.info(f"✓ Validação concluída:")
        logger.info(f"  - Registros válidos: {valid_count} ({quality_rate:.2f}%)")
        logger.info(f"  - Registros inválidos: {invalid_count} ({100-quality_rate:.2f}%)")
        
        if invalid_count > 0:
            # Estatísticas de erros
            error_counts = df_work.loc[~final_valid_mask, '_validation_errors'].str.split(';').explode()
            error_counts = error_counts[error_counts != ''].value_counts()
            logger.info(f"  - Tipos de erros mais comuns:")
            for error_type, count in error_counts.head(5).items():
                logger.info(f"    • {error_type}: {count} ocorrências")
        
        return df_valid, df_invalid
    
    def get_quality_metrics(self, df_valid: pd.DataFrame, df_invalid: pd.DataFrame) -> Dict:
        """
        Calcula métricas detalhadas de qualidade.
        """
        total = len(df_valid) + len(df_invalid)
        
        if total == 0:
            return {
                "total_records": 0,
                "valid_records": 0,
                "invalid_records": 0,
                "quality_rate": 0.0
            }
        
        metrics = {
            "total_records": total,
            "valid_records": len(df_valid),
            "invalid_records": len(df_invalid),
            "quality_rate": round((len(df_valid) / total) * 100, 2),
            "completeness": round((len(df_valid) / total) * 100, 2),
        }
        
        return metrics