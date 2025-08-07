from typing import Optional

import voyageai
import requests

# Добавление прокси через обертку для основного класса
class VoyageClientWrapper:
    def __init__(self, api_key, proxy_url=None):
        self.api_key = api_key
        self.proxy_url = proxy_url
        self.client = voyageai.Client(api_key=api_key)

        # Создаем кастомный адаптер с прокси
        class VoyageProxyAdapter(requests.adapters.HTTPAdapter):
            def __init__(self, proxy_url):
                self.proxy_url = proxy_url
                super().__init__()

            def send(self, request, **kwargs):
                if 'api.voyageai.com' in request.url:
                    kwargs['proxies'] = {
                        'http': self.proxy_url,
                        'https': self.proxy_url
                    }
                return super().send(request, **kwargs)

        # Патчим только requests для voyageai
        original_session_init = requests.Session.__init__

        def patched_session_init(session_self):
            original_session_init(session_self)
            # Добавляем адаптер только если это для Voyage AI
            adapter = VoyageProxyAdapter(self.proxy_url)
            session_self.mount('https://api.voyageai.com', adapter)

        requests.Session.__init__ = patched_session_init

    def embed(self, texts, model='voyage-3', input_type=None, output_dimension=None):
        """Выполняет embed через прокси"""
        return self.client.embed(texts=texts, model=model, input_type=input_type, output_dimension=output_dimension)

class VoyageClient:
    def __init__(self, api_key: str, proxy_url: Optional[str] = None):
        self.voyage_client = self.get_voyage_client(api_key=api_key, proxy_url=proxy_url)

    def get_voyage_client(self, api_key: str, proxy_url: Optional[str]):
        if proxy_url:
            return VoyageClientWrapper(api_key=api_key, proxy_url=proxy_url)
        return voyageai.Client(api_key=api_key)
