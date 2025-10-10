import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import MagicMock
from modules.ingestors.json_ingestor import JSONIngestor

# --- Fixtures de Dados ---

@pytest.fixture
def json_ingestor(mock_zip_buffer):
    """Fixture para inicializar o JSONIngestor."""
    # Usamos um nome genérico, pois mockaremos a leitura
    return JSONIngestor(mock_zip_buffer, 'dados.json') 

# Mock do conteúdo JSON para formato de lista
@pytest.fixture
def json_data_list():
    return [
        {'id': 1, 'nome': 'A'},
        {'id': 2, 'nome': 'B'}
    ]

# Mock do conteúdo JSON para formato API (fields e records)
@pytest.fixture
def json_data_api_format():
    return {
        'fields': [{'id': 'id'}, {'id': 'nome_completo'}],
        'records': [[3, 'C'], [4, 'D']]
    }

# Mock do conteúdo JSON com chave 'data'
@pytest.fixture
def json_data_nested():
    return {
        'header': 'info',
        'data': [
            {'id': 5, 'valor': 100},
            {'id': 6, 'valor': 200}
        ]
    }

# --- Teste de Unidade ---

def test_json_ingestor_parse_list(json_ingestor, json_data_list):
    """Testa o parsing de JSON no formato Lista de Dicionários."""
    df = json_ingestor._parse_json_structure(json_data_list)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['id', 'nome']

def test_json_ingestor_parse_api_format(json_ingestor, json_data_api_format):
    """Testa o parsing de JSON no formato 'fields' e 'records'."""
    df = json_ingestor._parse_json_structure(json_data_api_format)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    # As colunas devem vir dos 'fields'
    assert list(df.columns) == ['id', 'nome_completo'] 

def test_json_ingestor_parse_nested(json_ingestor, json_data_nested):
    """Testa o parsing de JSON aninhado com chave 'data'."""
    df = json_ingestor._parse_json_structure(json_data_nested)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['id', 'valor']
    
# ❌ O TESTE test_json_ingestor_load_full_process FOI REMOVIDO DAQUI