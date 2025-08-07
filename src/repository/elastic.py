from typing import Optional

from src.core.logger import get_logger
from src.config.config import settings
from elasticsearch import Elasticsearch

logger = get_logger(name=__name__)

class ElasticSearch:
    """Репозиторий для работы с ElasticSearch"""

    def __init__(self):
        self.es_client = self._get_client
        self.connection_attempts = settings.ES_MAX_RETRIES

    @property
    def _get_client(self) -> Optional[Elasticsearch]:
        """Создает и конфигурирует клиент es"""
        try:
            es_client = Elasticsearch([settings.ES_HOSTS])
            logger.debug(f"Клиент ES был инициализирован!")
            return es_client
        except Exception as e:
            logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            return None

    def close(self):
        """Закрытие соединения с Elasticsearch"""
        if self.es_client:
            try:
                self.es_client.close()
                logger.info("Соединение с Elasticsearch закрыто")
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения с Elasticsearch: {e}")
                return False
        return True

    def is_connected(self) -> bool:
        """Проверка активного подключения"""
        if not self.es_client:
            return False
        try:
            return self.es_client.ping()
        except Exception:
            return False

    def create_index(self, index_name: str = settings.ELASTICSEARCH_INDEX) -> bool:
        """Создание индекса для товаров"""
        if not self.is_connected():
            logger.error("Не удается подключиться к ES!")
            return False

        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "russian_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding",
                                "russian_stop",
                                "russian_stemmer"
                            ]
                        },
                        "autocomplete_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding",
                                "edge_ngram_filter"
                            ]
                        },
                        "autocomplete_search": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding"
                            ]
                        }
                    },
                    "filter": {
                        "russian_stop": {
                            "type": "stop",
                            "stopwords": "_russian_"
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        },
                        "edge_ngram_filter": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 20
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "attr_name": {
                        "type": "text",
                        "analyzer": "russian_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            },
                            "autocomplete": {
                                "type": "text",
                                "analyzer": "autocomplete_analyzer"
                            }
                        }
                    },
                    "attr_value": {
                        "type": "text",
                        "analyzer": "russian_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            },
                            "autocomplete": {
                                "type": "text",
                                "analyzer": "autocomplete_analyzer"
                            },
                            "numeric": {
                                "type": "double",
                                "ignore_malformed": True
                            }
                        }
                    },
                    "entity_id": {
                        "type": "keyword"
                    },
                    "entity_type": {
                        "type": "keyword"
                    }
                }
            }
        }

        try:
            # Удаляем индекс если он существует
            if self.es_client.indices.exists(index=index_name):
                self.es_client.indices.delete(index=index_name)
                logger.info(f"Существующий es-индекс {index_name} удален")

            # Создаем новый индекс
            self.es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"es-индекс {index_name} успешно создан")
            return True

        except Exception as e:
            logger.error(f"Ошибка создания индекса {index_name}: {e}")
            return False

    def add_documents(self, index_name: str, docs: dict[str, str]):
        for doc in docs:
            self.es_client.index(index=index_name, body=doc)

        result = self.es_client.count(index=index_name)
        logger.debug(f"Всего документов после добавления: {result['count']}")

    def add_document(self, index_name: str, document: dict[str, str]):
        self.es_client.index(index=index_name, document=document)
        return True

    def is_index_exists(self, index_name: str) -> bool:
        return self.es_client.indices.exists(index=index_name)

    def search_document(self, index_name: str, query: dict[str, str]):
        query_body = {
              "query": {
                "simple_query_string": {
                  "query": f"{list(query.keys())[0]}",
                  "fields": ["*", "*.value"],
                  "default_operator": "or"
                }
              }
            }
        logger.info(f"query: {query_body}")
        response = self.es_client.search(index=index_name, body=query_body)
        logger.info(f"RESPONSE: {response}")
        return response

    def search_document_fuzzy(self, index_name: str, query: dict[str, str]):

        query_body = {
              "query": {
                "match": {
                  "attr_name": {
                    "query": f"{list(query.keys())[0]}",
                    "fuzziness": "AUTO",
                    "minimum_should_match": "30%"
                  }
                }
              }
            }
        logger.info(f"query: {query_body}")
        response = self.es_client.search(index=index_name, body=query_body)
        logger.info(f"RESPONSE: {response}")
        return response

    def count_documents(self, index_name) -> Optional[int]:
        """Подсчитываем документы в индексе"""
        try:
            result = self.es_client.count(index=index_name)
            return result['count']
        except Exception as e:
            logger.error(f"Ошибка при подсчете документов: {e}")
            return None

    def get_index_info(self, index_name: str):
        """Получить информацию об индексе"""
        try:
            if not self.es_client.indices.exists(index=index_name):
                logger.info(f"Индекс '{index_name}' не существует")
                return False

            count = self.count_documents(index_name=index_name)
            stats = self.es_client.indices.stats(index=index_name)

            return {
                'index_name': index_name,
                'document_count': count,
                'index_size': stats['indices'][index_name]['total']['store']['size_in_bytes']
            }
        except Exception as e:
            return f"Ошибка: {e}"
