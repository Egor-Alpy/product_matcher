from src.config.config import settings
from src.core.logger import get_logger
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk
from typing import Dict, List, Any, Optional, Union

from src.repository.mongodb import MongoDb

logger = get_logger(name=__name__, level=settings.LOG_LEVEL)


class ProductSearchEngine:
    """
    Класс для работы с Elasticsearch для поиска товаров по характеристикам
    """

    def __init__(self,
                 hosts=None,
                 username: str = None,
                 password: str = None,
                 verify_certs: bool = True,
                 ca_certs: str = None):
        """
        Инициализация подключения к Elasticsearch

        Args:
            hosts: Список хостов Elasticsearch (должны включать схему: http://localhost:9200)
            username: Имя пользователя для аутентификации
            password: Пароль для аутентификации
            verify_certs: Проверять SSL сертификаты
            ca_certs: Путь к CA сертификатам
        """
        if hosts is None:
            hosts = ['http://localhost:9200']

        auth = None
        if username and password:
            auth = (username, password)

        self.es = Elasticsearch(
            hosts,
            basic_auth=auth,
            verify_certs=verify_certs,
            ca_certs=ca_certs
        )

        # Проверка подключения
        try:
            if not self.es.ping():
                raise ConnectionError("Не удалось подключиться к Elasticsearch. Проверьте, что сервис запущен.")

            # Логируем версию для диагностики
            info = self.es.info()
            es_version = info['version']['number']
            logger.info(f"Подключение к Elasticsearch {es_version} успешно установлено")
        except ESConnectionError as e:
            logger.error(f"Ошибка подключения: {e}")
            logger.error("Убедитесь, что Elasticsearch запущен на указанном хосте")
            raise
        except Exception as e:
            logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            logger.error(f"Проверьте URL подключения: {hosts}")
            raise

    def create_product_index(self, index_name: str = "products") -> bool:
        """
        Создание индекса для товаров с оптимизированным маппингом

        Args:
            index_name: Название индекса

        Returns:
            bool: True если индекс создан успешно
        """
        # Маппинг для индекса товаров
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "product_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "asciifolding",
                                "stop",
                                "stemmer"
                            ]
                        },
                        "exact_analyzer": {
                            "type": "keyword",
                            "normalizer": "lowercase_normalizer"
                        }
                    },
                    "normalizer": {
                        "lowercase_normalizer": {
                            "type": "custom",
                            "char_filter": [],
                            "filter": ["lowercase", "asciifolding"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "product_analyzer",
                        "fields": {
                            "exact": {
                                "type": "keyword",
                                "normalizer": "lowercase_normalizer"
                            },
                            "suggest": {
                                "type": "completion",
                                "analyzer": "product_analyzer"
                            }
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "product_analyzer"
                    },
                    "category": {
                        "type": "keyword"
                    },
                    "brand": {
                        "type": "keyword"
                    },
                    "price": {
                        "type": "double"
                    },
                    "currency": {
                        "type": "keyword"
                    },
                    "availability": {
                        "type": "boolean"
                    },
                    "created_at": {
                        "type": "date"
                    },
                    "updated_at": {
                        "type": "date"
                    },
                    # Динамические характеристики товара
                    "characteristics": {
                        "type": "nested",
                        "properties": {
                            "name": {
                                "type": "keyword"
                            },
                            "value": {
                                "type": "text",
                                "analyzer": "product_analyzer",
                                "fields": {
                                    "exact": {
                                        "type": "keyword"
                                    },
                                    "numeric": {
                                        "type": "double",
                                        "ignore_malformed": True
                                    }
                                }
                            },
                            "type": {
                                "type": "keyword"  # string, numeric, boolean, range
                            },
                            "unit": {
                                "type": "keyword"  # кг, см, литр и т.д.
                            }
                        }
                    },
                    # Плоские характеристики для быстрого поиска
                    "flat_characteristics": {
                        "type": "flattened"
                    }
                }
            }
        }

        try:
            # Удаляем индекс если он существует
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                logger.info(f"Существующий индекс {index_name} удален")

            # Создаем новый индекс
            response = self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"Индекс {index_name} успешно создан")
            return True

        except Exception as e:
            # Более общая обработка для совместимости с разными версиями
            logger.error(f"Ошибка создания индекса {index_name}: {e}")
            return False

    def add_product(self, product_data: Dict[str, Any], index_name: str = "products") -> bool:
        """
        Добавление товара в индекс

        Args:
            product_data: Словарь с данными товара
            index_name: Название индекса

        Returns:
            bool: True если товар добавлен успешно
        """
        try:
            # Обработка характеристик
            if 'characteristics' in product_data:
                processed_characteristics = []
                flat_characteristics = {}

                for char in product_data['characteristics']:
                    # Определяем тип характеристики
                    char_type = self._determine_characteristic_type(char.get('value'))

                    processed_char = {
                        'name': char.get('name'),
                        'value': str(char.get('value')),
                        'type': char_type,
                        'unit': char.get('unit', '')
                    }

                    processed_characteristics.append(processed_char)
                    flat_characteristics[char.get('name')] = str(char.get('value'))

                product_data['characteristics'] = processed_characteristics
                product_data['flat_characteristics'] = flat_characteristics

            # Добавляем товар в индекс
            response = self.es.index(
                index=index_name,
                id=product_data.get('id'),
                document=product_data  # Изменено с body на document для новой версии
            )

            logger.info(f"Товар {product_data.get('id')} добавлен в индекс {index_name}")
            return True

        except Exception as e:
            logger.error(f"Ошибка добавления товара: {e}")
            return False

    def bulk_add_products(self, products: List[Dict[str, Any]], index_name: str = "products") -> Dict[str, Any]:
        """
        Массовое добавление товаров в индекс

        Args:
            products: Список товаров
            index_name: Название индекса

        Returns:
            Dict: Результат операции с детализацией
        """
        try:
            actions = []

            for product in products:
                # Обработка характеристик
                if 'characteristics' in product:
                    processed_characteristics = []
                    flat_characteristics = {}

                    for char in product['attributes']:
                        char_type = self._determine_characteristic_type(char.get('value'))

                        processed_char = {
                            'name': char.get('name'),
                            'value': str(char.get('value')),
                            'type': char_type,
                            'unit': char.get('unit', '')
                        }

                        processed_characteristics.append(processed_char)
                        flat_characteristics[char.get('name')] = str(char.get('value'))

                    product['attributes'] = processed_characteristics
                    product['flat_characteristics'] = flat_characteristics

                action = {
                    "_index": index_name,
                    "_id": product.get('id'),
                    "_source": product
                }
                actions.append(action)

            # Выполняем bulk операцию
            success, failed = bulk(self.es, actions, index=index_name, chunk_size=500)

            result = {
                "success": success,
                "failed": len(failed) if failed else 0,
                "total": len(products)
            }

            if failed:
                logger.warning(f"Ошибки при добавлении товаров: {failed}")

            logger.info(f"Добавлено товаров: {success}, ошибок: {result['failed']}")
            return result

        except Exception as e:
            logger.error(f"Ошибка массового добавления товаров: {e}")
            return {"success": 0, "failed": len(products), "total": len(products)}

    def search_products(self,
                        search_criteria: Dict[str, Any],
                        index_name: str = "products",
                        size: int = 10,
                        from_: int = 0) -> Dict[str, Any]:
        """
        Поиск товаров по критериям

        Args:
            search_criteria: Словарь с критериями поиска
            index_name: Название индекса
            size: Количество результатов
            from_: Смещение для пагинации

        Returns:
            Dict: Результаты поиска
        """
        try:
            query = self._build_search_query(search_criteria)

            response = self.es.search(
                index=index_name,
                query=query,  # Изменено с body на отдельные параметры
                size=size,
                from_=from_,
                sort=[
                    {"_score": {"order": "desc"}},
                    {"updated_at": {"order": "desc"}}
                ]
            )

            return {
                "total": response['hits']['total']['value'],
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"Ошибка поиска товаров: {e}")
            return {"total": 0, "products": [], "took": 0}

    def search_by_characteristics(self,
                                  characteristics: Dict[str, Any],
                                  index_name: str = "products",
                                  size: int = 10,
                                  exact_match: bool = False) -> Dict[str, Any]:
        """
        Поиск товаров по характеристикам

        Args:
            characteristics: Словарь характеристик {название: значение}
            index_name: Название индекса
            size: Количество результатов
            exact_match: Точное совпадение характеристик

        Returns:
            Dict: Результаты поиска
        """
        try:
            must_conditions = []

            for char_name, char_value in characteristics.items():
                if exact_match:
                    # Точное совпадение
                    must_conditions.append({
                        "nested": {
                            "path": "characteristics",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"characteristics.name": char_name}},
                                        {"term": {"characteristics.value.exact": str(char_value)}}
                                    ]
                                }
                            }
                        }
                    })
                else:
                    # Нечеткое совпадение
                    char_type = self._determine_characteristic_type(char_value)

                    if char_type == "numeric":
                        # Числовой поиск с диапазоном
                        numeric_value = float(char_value)
                        tolerance = numeric_value * 0.1  # 10% погрешность

                        must_conditions.append({
                            "nested": {
                                "path": "characteristics",
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"term": {"characteristics.name": char_name}},
                                            {
                                                "range": {
                                                    "characteristics.value.numeric": {
                                                        "gte": numeric_value - tolerance,
                                                        "lte": numeric_value + tolerance
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        })
                    else:
                        # Текстовый поиск
                        must_conditions.append({
                            "nested": {
                                "path": "characteristics",
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"term": {"characteristics.name": char_name}},
                                            {"match": {"characteristics.value": str(char_value)}}
                                        ]
                                    }
                                }
                            }
                        })

            query = {
                "bool": {
                    "must": must_conditions
                }
            }

            response = self.es.search(
                index=index_name,
                query=query,
                size=size,
                sort=[{"_score": {"order": "desc"}}]
            )

            return {
                "total": response['hits']['total']['value'],
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"Ошибка поиска по характеристикам: {e}")
            return {"total": 0, "products": [], "took": 0}

    def get_similar_products(self,
                             product_id: str,
                             index_name: str = "products",
                             size: int = 5) -> Dict[str, Any]:
        """
        Поиск похожих товаров на основе характеристик

        Args:
            product_id: ID товара для поиска похожих
            index_name: Название индекса
            size: Количество результатов

        Returns:
            Dict: Результаты поиска похожих товаров
        """
        try:
            # Получаем исходный товар
            source_product = self.es.get(index=index_name, id=product_id)
            source_data = source_product['_source']

            if 'characteristics' not in source_data:
                return {"total": 0, "products": [], "took": 0}

            # Строим запрос на основе характеристик исходного товара
            should_conditions = []

            for char in source_data['characteristics']:
                should_conditions.append({
                    "nested": {
                        "path": "characteristics",
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"characteristics.name": char['name']}},
                                    {"match": {"characteristics.value": char['value']}}
                                ]
                            }
                        },
                        "boost": 1.0
                    }
                })

            query = {
                "bool": {
                    "should": should_conditions,
                    "must_not": [
                        {"term": {"id": product_id}}  # Исключаем сам товар
                    ],
                    "minimum_should_match": 1
                }
            }

            response = self.es.search(
                index=index_name,
                query=query,
                size=size,
                sort=[{"_score": {"order": "desc"}}]
            )

            return {
                "total": response['hits']['total']['value'],
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"Ошибка поиска похожих товаров: {e}")
            return {"total": 0, "products": [], "took": 0}

    def _ensure_index_exists(self, index_name: str) -> bool:
        """Проверка и создание индекса при необходимости"""
        if not self.es.indices.exists(index=index_name):
            logger.warning(f"Индекс {index_name} не существует, создаем автоматически")
            return self.create_product_index(index_name)
        return True

    def _build_search_query(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Построение поискового запроса на основе критериев

        Args:
            criteria: Критерии поиска

        Returns:
            Dict: Elasticsearch query
        """
        must_conditions = []

        # Текстовый поиск по названию и описанию
        if 'text' in criteria:
            must_conditions.append({
                "multi_match": {
                    "query": criteria['text'],
                    "fields": ["name^2", "description"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })

        # Фильтр по категории
        if 'category' in criteria:
            must_conditions.append({
                "term": {"category": criteria['category']}
            })

        # Фильтр по бренду
        if 'brand' in criteria:
            must_conditions.append({
                "term": {"brand": criteria['brand']}
            })

        # Фильтр по цене
        if 'price_from' in criteria or 'price_to' in criteria:
            price_range = {}
            if 'price_from' in criteria:
                price_range['gte'] = criteria['price_from']
            if 'price_to' in criteria:
                price_range['lte'] = criteria['price_to']

            must_conditions.append({
                "range": {"price": price_range}
            })

        # Фильтр по доступности
        if 'availability' in criteria:
            must_conditions.append({
                "term": {"availability": criteria['availability']}
            })

        # Фильтры по характеристикам
        if 'characteristics' in criteria:
            for char_name, char_value in criteria['characteristics'].items():
                must_conditions.append({
                    "nested": {
                        "path": "characteristics",
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"characteristics.name": char_name}},
                                    {"match": {"characteristics.value": str(char_value)}}
                                ]
                            }
                        }
                    }
                })

        if not must_conditions:
            return {"match_all": {}}

        return {"bool": {"must": must_conditions}}

    def _determine_characteristic_type(self, value: Any) -> str:
        """
        Определение типа характеристики

        Args:
            value: Значение характеристики

        Returns:
            str: Тип характеристики
        """
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, (int, float)):
            return "numeric"
        else:
            # Пытаемся преобразовать строку в число
            try:
                float(str(value))
                return "numeric"
            except ValueError:
                return "string"

    def delete_product(self, product_id: str, index_name: str = "products") -> bool:
        """
        Удаление товара из индекса

        Args:
            product_id: ID товара
            index_name: Название индекса

        Returns:
            bool: True если товар удален успешно
        """
        try:
            self.es.delete(index=index_name, id=product_id)
            logger.info(f"Товар {product_id} удален из индекса {index_name}")
            return True
        except NotFoundError:
            logger.warning(f"Товар {product_id} не найден в индексе {index_name}")
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления товара {product_id}: {e}")
            return False

    def refresh_index(self, index_name: str = "products") -> bool:
        """
        Обновление индекса для немедленного поиска новых документов

        Args:
            index_name: Название индекса

        Returns:
            bool: True если индекс обновлен успешно
        """
        try:
            self.es.indices.refresh(index=index_name)
            logger.info(f"Индекс {index_name} обновлен")
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления индекса {index_name}: {e}")
            return False

    def get_index_stats(self, index_name: str = "products") -> Dict[str, Any]:
        """
        Получение статистики по индексу

        Args:
            index_name: Название индекса

        Returns:
            Dict: Статистика индекса
        """
        try:
            stats = self.es.indices.stats(index=index_name)
            return {
                "documents_count": stats['indices'][index_name]['total']['docs']['count'],
                "store_size_bytes": stats['indices'][index_name]['total']['store']['size_in_bytes'],
                "indexing_total": stats['indices'][index_name]['total']['indexing']['index_total'],
                "search_total": stats['indices'][index_name]['total']['search']['query_total']
            }
        except Exception as e:
            logger.error(f"Ошибка получения статистики индекса {index_name}: {e}")
            return {}



# Todo: Mock usage !!!!!!!!!!!!!!!!!!!!!!
if __name__ == "__main__":
    # Инициализация (для локального Elasticsearch без SSL)
    search_engine = ProductSearchEngine(hosts=['http://localhost:9200'])

    # Для продакшена с SSL:
    # search_engine = ProductSearchEngine(
    #     hosts=['https://your-elasticsearch-host:9200'],
    #     username='your_username',
    #     password='your_password',
    #     verify_certs=True
    # )

    # Создание индекса
    search_engine.create_product_index("products")

    # Пример товаров для добавления
    sample_products = [
        {
            "id": "1",
            "name": "iPhone 13",
            "description": "Смартфон Apple iPhone 13",
            "category": "smartphones",
            "brand": "Apple",
            "price": 79990.0,
            "currency": "RUB",
            "availability": True,
            "created_at": "2023-01-01T00:00:00",
            "updated_at": "2023-01-01T00:00:00",
            "characteristics": [
                {"name": "screen_size", "value": "6.1", "unit": "inch"},
                {"name": "memory", "value": "128", "unit": "GB"},
                {"name": "color", "value": "black"},
                {"name": "weight", "value": "174", "unit": "g"}
            ]
        },
        {
            "id": "2",
            "name": "Samsung Galaxy S21",
            "description": "Смартфон Samsung Galaxy S21",
            "category": "smartphones",
            "brand": "Samsung",
            "price": 69990.0,
            "currency": "RUB",
            "availability": True,
            "created_at": "2023-01-01T00:00:00",
            "updated_at": "2023-01-01T00:00:00",
            "characteristics": [
                {"name": "screen_size", "value": "6.2", "unit": "inch"},
                {"name": "memory", "value": "128", "unit": "GB"},
                {"name": "color", "value": "white"},
                {"name": "weight", "value": "171", "unit": "g"}
            ]
        }
    ]

    # Массовое добавление товаров
    mongo_client = MongoDb()
    import asyncio
    dataset_products = asyncio.run(mongo_client.get_all_products())
    result = search_engine.bulk_add_products(sample_products)
    print(f"Результат загрузки: {result}")

    # Обновление индекса
    search_engine.refresh_index()

    # Поиск по характеристикам
    results = search_engine.search_by_characteristics({
        "memory": "128",
        "screen_size": "6.1"
    })

    print(f"Найдено товаров: {results['total']}")
    for product in results['products']:
        print(f"- {product['name']} ({product['brand']})")

    # Поиск похожих товаров
    similar = search_engine.get_similar_products("1")
    print(f"\nПохожие товары: {similar['total']}")
    for product in similar['products']:
        print(f"- {product['name']} ({product['brand']})")

    # Статистика индекса
    stats = search_engine.get_index_stats()
    print(f"\nСтатистика индекса: {stats}")