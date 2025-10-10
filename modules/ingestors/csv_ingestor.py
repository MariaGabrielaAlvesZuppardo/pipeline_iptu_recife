import zipfile
import pandas as pd
import unicodedata
from io import BytesIO
from modules.utils.logger import setup_logger


class CSVIngestor:
    """
    Ingestor para arquivos CSV dentro de ZIPs.
    Apenas carrega os dados brutos, SEM validação.
    """
    
    def __init__(self, zip_buffer: BytesIO, csv_filename: str):
        """
        Args:
            zip_buffer: Buffer em memória contendo o arquivo ZIP
            csv_filename: Nome do arquivo CSV dentro do ZIP
        """
        self.zip_buffer = zip_buffer
        self.csv_filename = csv_filename
        # 🟢 CORREÇÃO 1: Logger como atributo de instância
        self.logger = setup_logger(self.__class__.__name__)
        
    def _fix_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        # ... (seu código de correção de nomes de coluna) ...
        # ...
        
        # O restante da função _fix_column_names não foi incluído,
        # mas presumimos que está correto e usa 'self' se precisar.
        
        replacements = {
            'Ã': 'ã', 'Ã§': 'ç', 'Ã©': 'é', 'Ã³': 'ó', 'Ã­': 'í', 
            'Ãº': 'ú', 'Ã¡': 'á', 'Ã£': 'ã', 'Ãª': 'ê', 
        }
        
        new_columns = {}
        for col in df.columns:
            new_col = col
            for bad, good in replacements.items():
                new_col = new_col.replace(bad, good)
            
            new_col = ''.join(char for char in new_col if unicodedata.category(char)[0] != 'C')
            
            new_columns[col] = new_col
        
        return df.rename(columns=new_columns)

    
    def load(self) -> pd.DataFrame:
        """
        Carrega o CSV do ZIP em CHUNKS para evitar estouro de memória...
        """
        CHUNK_SIZE = 50000 
        
        # 💡 NÃO PRECISA MAIS DE all_chunks aqui, ele será definido dentro do try
        
        try:
            # 🟢 CORREÇÃO 1: Uso de self.logger
            self.logger.info(f"Lendo arquivo {self.csv_filename} do ZIP em chunks (tamanho: {CHUNK_SIZE})...")
            
            with zipfile.ZipFile(self.zip_buffer, 'r') as z:
                
                file_list = z.namelist()
                # 🟢 CORREÇÃO 1: Uso de self.logger
                self.logger.debug(f"Arquivos encontrados no ZIP: {file_list}")
                
                # --- Lógica de busca de arquivo (adaptada para um exemplo mais completo) ---
                csv_file = None
                for f in file_list:
                    if f.lower().endswith('.csv') and self.csv_filename.lower() in f.lower():
                        csv_file = f
                        break
                
                if not csv_file:
                    raise FileNotFoundError(
                        f"Arquivo CSV '{self.csv_filename}' não encontrado no ZIP. "
                        f"Arquivos disponíveis: {file_list}"
                    )
                
                # 🟢 CORREÇÃO 1: Uso de self.logger
                self.logger.info(f"Arquivo encontrado: {csv_file}")
                
                
                # 🟢 CORREÇÃO 2: A função interna agora aceita a lista de chunks como argumento
                def read_in_chunks(file_handle, encoding, chunk_list):
                    chunk_iterator = pd.read_csv(
                        file_handle,
                        encoding=encoding,
                        sep=';',
                        chunksize=CHUNK_SIZE, 
                        na_values=['', 'NULL', 'null', 'NA', 'N/A'],
                        skipinitialspace=True
                    )
                    
                    for i, chunk in enumerate(chunk_iterator):
                        # 🟢 CORREÇÃO 1: Uso de self.logger
                        self.logger.debug(f"Lido chunk #{i+1} com {len(chunk)} registros")
                        chunk_list.append(chunk)
                    
                    return pd.concat(chunk_list, ignore_index=True)

                
                with z.open(csv_file) as f:
                    # 1. Tentar UTF-8 com BOM
                    all_chunks_utf8 = [] # 🟢 CORREÇÃO 2: Nova lista de chunks
                    try:
                        self.logger.debug("Tentando encoding 'utf-8-sig'")
                        df = read_in_chunks(f, 'utf-8-sig', all_chunks_utf8)
                    except UnicodeDecodeError:
                        # 2. Fallback para latin1
                        # 🟢 CORREÇÃO 1: Uso de self.logger
                        self.logger.warning("UTF-8 falhou, tentando latin1...")
                        
                        # Reabrir o arquivo para garantir que a leitura comece do zero
                        with z.open(csv_file) as f2:
                             all_chunks_latin1 = [] # 🟢 CORREÇÃO 2: Nova lista de chunks
                             df = read_in_chunks(f2, 'latin1', all_chunks_latin1)

                
                # 🟢 CORREÇÃO 1: Uso de self.logger
                self.logger.info(f"✓ CSV carregado: {len(df)} registros, {len(df.columns)} colunas")
                
                # Corrigir nomes de colunas com problemas de encoding
                df = self._fix_column_names(df)
                
                # 🟢 CORREÇÃO 1: Uso de self.logger
                self.logger.debug(f"Colunas: {list(df.columns)}")
                
                return df
                
        except FileNotFoundError as e:
            # 🟢 CORREÇÃO 1: Uso de self.logger
            self.logger.error(f"Erro: {e}")
            raise
        except Exception as e:
            # 🟢 CORREÇÃO 1: Uso de self.logger
            self.logger.error(f"Erro ao ler CSV do ZIP: {e}")
            raise