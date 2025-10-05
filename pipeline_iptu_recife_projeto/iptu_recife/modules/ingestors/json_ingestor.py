import zipfile
import json
import pandas as pd
from io import BytesIO
from modules.utils.logger import setup_logger

# ❌ REMOVA ESTA LINHA GLOBAL:
# logger = setup_logger("json_ingestor")


class JSONIngestor:
    """
    Ingestor para arquivos JSON dentro de ZIPs.
    Apenas carrega os dados brutos, SEM validação.
    Suporta estruturas com 'fields' e 'records'.
    """
    
    def __init__(self, zip_buffer: BytesIO, json_filename: str):
        """
        Args:
            zip_buffer: Buffer em memória contendo o arquivo ZIP
            json_filename: Nome do arquivo JSON dentro do ZIP
        """
        self.zip_buffer = zip_buffer
        self.json_filename = json_filename
        # 🟢 CORREÇÃO CRÍTICA: Inicialize o logger como atributo de instância
        self.logger = setup_logger(self.__class__.__name__)
    
    def load(self) -> pd.DataFrame:
        """
        Carrega o JSON do ZIP e retorna DataFrame bruto.
        """
        try:
            # 🟢 USE self.logger
            self.logger.info(f"Lendo arquivo {self.json_filename} do ZIP...")
            
            with zipfile.ZipFile(self.zip_buffer, 'r') as z:
                file_list = z.namelist()
                # 🟢 USE self.logger
                self.logger.debug(f"Arquivos encontrados no ZIP: {file_list}")
                
                # Encontrar o JSON
                json_file = None
                for f in file_list:
                    # Melhoria: Se o nome do arquivo for relevante (como no CSV), 
                    # use self.json_filename na busca também. Por simplicidade, mantemos o que estava.
                    if f.lower().endswith('.json'):
                        json_file = f
                        break
                
                if not json_file:
                    raise FileNotFoundError(
                        f"Nenhum arquivo JSON encontrado no ZIP. "
                        f"Arquivos disponíveis: {file_list}"
                    )
                
                # 🟢 USE self.logger
                self.logger.info(f"Arquivo encontrado: {json_file}")
                
                # Ler JSON
                with z.open(json_file) as f:
                    json_data = json.load(f)
                
                # Converter para DataFrame
                df = self._parse_json_structure(json_data)
                
                # 🟢 USE self.logger
                self.logger.info(f"✓ JSON carregado: {len(df)} registros, {len(df.columns)} colunas")
                # 🟢 USE self.logger
                self.logger.debug(f"Primeiras colunas: {list(df.columns)[:5]}")
                
                return df
                
        except FileNotFoundError as e:
            # 🟢 USE self.logger
            self.logger.error(f"Erro: {e}")
            raise
        except json.JSONDecodeError as e:
            # 🟢 USE self.logger
            self.logger.error(f"Erro ao decodificar JSON: {e}")
            raise
        except Exception as e:
            # 🟢 USE self.logger
            self.logger.error(f"Erro ao ler JSON do ZIP: {e}")
            raise
    
    def _parse_json_structure(self, json_data: dict | list) -> pd.DataFrame:
        """
        Analisa a estrutura do JSON e retorna DataFrame.
        """
        # Caso 1: Lista direta
        if isinstance(json_data, list):
            # 🟢 USE self.logger
            self.logger.debug("Estrutura: lista de registros")
            return pd.DataFrame(json_data)
        
        # Caso 2: Dict com 'fields' e 'records' (formato API comum)
        if isinstance(json_data, dict) and 'fields' in json_data and 'records' in json_data:
            # 🟢 USE self.logger
            self.logger.info("Estrutura detectada: fields + records (formato API)")
            
            # Extrair nomes das colunas de 'fields'
            columns = [field['id'] for field in json_data['fields']]
            # 🟢 USE self.logger
            self.logger.debug(f"Colunas extraídas de 'fields': {len(columns)} colunas")
            
            # Extrair dados de 'records'
            records = json_data['records']
            
            # Criar DataFrame com colunas nomeadas
            df = pd.DataFrame(records, columns=columns)
            # 🟢 USE self.logger
            self.logger.info(f"✓ DataFrame criado com colunas nomeadas de 'fields'")
            
            return df
        
        # Caso 3: Dict com chave de dados conhecida
        if isinstance(json_data, dict):
            possible_keys = ['data', 'dados', 'registros', 'records', 'items', 'imoveis']
            
            for key in possible_keys:
                if key in json_data and isinstance(json_data[key], list):
                    # 🟢 USE self.logger
                    self.logger.info(f"Dados encontrados na chave '{key}'")
                    return pd.DataFrame(json_data[key])
            
            # Caso 4: Tentar normalização
            # 🟢 USE self.logger
            self.logger.warning("Estrutura JSON não reconhecida, tentando normalização")
            return pd.json_normalize(json_data)
        
        raise ValueError(f"Formato JSON não suportado: {type(json_data)}")