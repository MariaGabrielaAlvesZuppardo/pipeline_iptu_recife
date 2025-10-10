import pytest
from io import BytesIO

@pytest.fixture
def mock_zip_buffer():
    """Retorna um buffer de bytes simulado (BytesIO) para testes."""
    return BytesIO(b"dummy zip content")