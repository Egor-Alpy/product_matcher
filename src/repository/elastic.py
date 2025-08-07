from typing import Optional

from src.core.logger import get_logger
from src.config.config import settings
from elasticsearch import Elasticsearch

logger = get_logger(name=__name__)

class ElasticSearch:
    """Репозиторий для работы с ElasticSearch"""

    def __init__(self):
        self.es_client = self._get_client
        self.index_name = settings.ELASTICSEARCH_INDEX
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
                    "entity_id"
                    # Плоские атрибуты для быстрого поиска
                    "flat_attributes": {
                        "type": "flattened"
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

    def add_documents(self):
        pass

    def count_documents(self) -> Optional[int]:
        """Подсчитываем документы в индексе"""
        try:
            result = self.es_client.count(index=self.index_name)
            return result['count']
        except Exception as e:
            print(f"Ошибка при подсчете документов: {e}")
            return None

    def get_index_info(self):
        """Получить информацию об индексе"""
        try:
            index_name = self.index_name
            if not self.es_client.indices.exists(index=index_name):
                logger.info()
                return f"Индекс '{index_name}' не существует"

            count = self.count_documents()
            stats = self.es_client.indices.stats(index=index_name)

            return {
                'index_name': index_name,
                'document_count': count,
                'index_size': stats['indices'][index_name]['total']['store']['size_in_bytes']
            }
        except Exception as e:
            return f"Ошибка: {e}"

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

es = ElasticSearch()
print(f'connected: {es.is_connected()}')

print(f'Создаем индекс в эластике: {es.create_index(index_name="testik")}')
print(f'Количество документов в индексе {settings.ELASTICSEARCH_INDEX}: {es.count_documents()}')
print(f'Информация по индексу {es.get_index_info()}')


print(f'Закрытие соединения с эластиком: {es.close()}')

