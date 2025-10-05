import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import MagicMock, call

from modules.ingestors.csv_ingestor import CSVIngestor


# --- Fixtures de Dados ---

@pytest.fixture
def mock_zip_buffer():
    """Retorna um io.BytesIO vazio mockado para inicialização."""
    return BytesIO(b'dummy zip content')

# DataFrame simulado com problemas de encoding no nome da coluna
@pytest.fixture
def raw_df_with_encoding_issue():
    data = {
        'coluna_normal': [1, 2],
        'cpf/cnpj_mascarado_do_contribuinte': ['a', 'b'], # Coluna que será corrigida em _fix_column_names
        'FracÃ£o Ideal': [0.5, 0.5] # Coluna com erro de encoding
    }
    return pd.DataFrame(data)

def test_csv_ingestor_file_not_found(mocker, mock_zip_buffer):
    """Testa o cenário onde o CSV não é encontrado dentro do ZIP."""
    ingestor = CSVIngestor(mock_zip_buffer, 'arquivo_inexistente.csv')

    # Mockar ZipFile para retornar lista de arquivos vazia/diferente
    mock_zipfile_instance = MagicMock()
    mock_zipfile_instance.namelist.return_value = ['folder/outro_arquivo.txt']
    mocker.patch('modules.ingestors.csv_ingestor.zipfile.ZipFile', return_value=mock_zipfile_instance)
    # Execução e Assert de Exceção
    with pytest.raises(FileNotFoundError) as excinfo:
        ingestor.load()
        
    assert "não encontrado no ZIP" in str(excinfo.value)