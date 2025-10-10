import requests
import io

class Downloader:
    """Baixa ZIP diretamente para memória"""
    def download_zip_in_memory(self, url: str) -> io.BytesIO:
        resp = requests.get(url)
        resp.raise_for_status()
        return io.BytesIO(resp.content)
