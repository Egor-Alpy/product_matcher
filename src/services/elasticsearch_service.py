from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk
from typing import Dict, List, Any, Optional
from src.config.config import settings
from src.core.logger import get_logger

logger = get_logger(name=__name__)


class ElasticsearchService:
    """Сервис для работы с Elasticsearch для товаров"""

    def __init__(self):
        self.es = None
        self.index_name = "products"

    def connect(self, hosts: List[str] = None):
        """Подключение к Elasticsearch"""
        if hosts is None:
            es_host = getattr(settings, 'ELASTICSEARCH_HOST', 'localhost')
            es_port = getattr(settings, 'ELASTICSEARCH_PORT', '9200')
            hosts = [f'http://{es_host}:{es_port}']

        try:
            self.es = Elasticsearch(hosts)
            if self.es.ping():
                logger.info(f"✅ Подключение к Elasticsearch успешно: {hosts}")
                return True
            else:
                logger.error("❌ Не удалось ping Elasticsearch")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Elasticsearch: {e}")
            return False

    def create_index(self, index_name: str = "products") -> bool:
        """Создание индекса для товаров"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
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
                    "_id": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "russian_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "russian_analyzer"
                    },
                    "article": {
                        "type": "keyword"
                    },
                    "brand": {
                        "type": "keyword"
                    },
                    "country_of_origin": {
                        "type": "keyword"
                    },
                    "warranty_months": {
                        "type": "keyword"
                    },
                    "category": {
                        "type": "keyword"
                    },
                    "created_at": {
                        "type": "date",
                        "format": "dd.MM.yyyy HH:mm||strict_date_optional_time||epoch_millis"
                    },
                    # Динамические атрибуты товара
                    "attributes": {
                        "type": "nested",
                        "properties": {
                            "attr_name": {
                                "type": "keyword"
                            },
                            "attr_value": {
                                "type": "text",
                                "analyzer": "russian_analyzer",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "numeric": {
                                        "type": "double",
                                        "ignore_malformed": True
                                    }
                                }
                            }
                        }
                    },
                    # Поставщики
                    "suppliers": {
                        "type": "nested",
                        "properties": {
                            "dealer_id": {
                                "type": "keyword"
                            },
                            "supplier_name": {
                                "type": "keyword"
                            },
                            "supplier_tel": {
                                "type": "keyword"
                            },
                            "supplier_address": {
                                "type": "text"
                            },
                            "supplier_description": {
                                "type": "text"
                            },
                            "supplier_offers": {
                                "type": "nested",
                                "properties": {
                                    "price": {
                                        "type": "nested",
                                        "properties": {
                                            "qnt": {
                                                "type": "integer"
                                            },
                                            "discount": {
                                                "type": "double"
                                            },
                                            "price": {
                                                "type": "double"
                                            }
                                        }
                                    },
                                    "stock": {
                                        "type": "keyword"
                                    },
                                    "delivery_time": {
                                        "type": "keyword"
                                    },
                                    "package_info": {
                                        "type": "text"
                                    },
                                    "purchase_url": {
                                        "type": "keyword",
                                        "index": False
                                    }
                                }
                            }
                        }
                    },
                    # Плоские атрибуты для быстрого поиска
                    "flat_attributes": {
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
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"✅ Индекс {index_name} успешно создан")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка создания индекса {index_name}: {e}")
            return False

    def add_product(self, product_data: Dict[str, Any], index_name: str = "products") -> bool:
        """Добавление товара в индекс"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
            return False

        try:
            # Обрабатываем атрибуты для создания flat_attributes
            if 'attributes' in product_data:
                flat_attributes = {}
                for attr in product_data['attributes']:
                    attr_name = attr.get('attr_name')
                    attr_value = attr.get('attr_value')
                    if attr_name and attr_value:
                        flat_attributes[attr_name] = attr_value

                product_data['flat_attributes'] = flat_attributes

            # Добавляем товар в индекс
            response = self.es.index(
                index=index_name,
                id=product_data.get('_id', {}).get('$oid'),
                document=product_data
            )

            logger.debug(f"Товар добавлен в индекс: {response['result']}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка добавления товара: {e}")
            return False

    def bulk_add_products(self, products: List[Dict[str, Any]], index_name: str = "products") -> Dict[str, int]:
        """Массовое добавление товаров"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
            return {"success": 0, "failed": len(products), "total": len(products)}

        try:
            actions = []

            for product in products:
                # Обрабатываем атрибуты
                if 'attributes' in product:
                    flat_attributes = {}
                    for attr in product['attributes']:
                        attr_name = attr.get('attr_name')
                        attr_value = attr.get('attr_value')
                        if attr_name and attr_value:
                            flat_attributes[attr_name] = attr_value

                    product['flat_attributes'] = flat_attributes

                action = {
                    "_index": index_name,
                    "_id": product.get('_id', {}).get('$oid'),
                    "_source": product
                }
                actions.append(action)

            # Выполняем bulk операцию
            success, failed = bulk(self.es, actions, chunk_size=500)

            result = {
                "success": success,
                "failed": len(failed) if failed else 0,
                "total": len(products)
            }

            logger.info(f"✅ Bulk добавление: {success} успешно, {result['failed']} ошибок")
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка bulk добавления: {e}")
            return {"success": 0, "failed": len(products), "total": len(products)}

    def search_products(self,
                        query: str = None,
                        filters: Dict[str, Any] = None,
                        size: int = 10,
                        from_: int = 0,
                        index_name: str = "products") -> Dict[str, Any]:
        """Поиск товаров"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
            return {"total": 0, "products": [], "took": 0}

        try:
            search_query = {"match_all": {}}

            # Если есть текстовый запрос
            if query and query.strip():
                search_query = {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "title^3",
                            "description^2",
                            "brand^2",
                            "article",
                            "attributes.attr_value"
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                }

            # Строим полный запрос с фильтрами
            full_query = {"query": search_query}

            # Добавляем фильтры
            if filters:
                must_conditions = [search_query]

                for field, value in filters.items():
                    if field == "brand":
                        must_conditions.append({"term": {"brand": value}})
                    elif field == "category":
                        must_conditions.append({"term": {"category": value}})
                    elif field == "price_from":
                        must_conditions.append({"range": {"suppliers.supplier_offers.price.price": {"gte": value}}})
                    elif field == "price_to":
                        must_conditions.append({"range": {"suppliers.supplier_offers.price.price": {"lte": value}}})

                full_query["query"] = {"bool": {"must": must_conditions}}

            # Добавляем пагинацию
            full_query.update({
                "size": size,
                "from": from_,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"created_at": {"order": "desc"}}
                ]
            })

            response = self.es.search(index=index_name, body=full_query)

            return {
                "total": response['hits']['total']['value'],
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"❌ Ошибка поиска товаров: {e}")
            return {"total": 0, "products": [], "took": 0}

    def search_by_attributes(self,
                             attributes: Dict[str, Any],
                             size: int = 10,
                             exact_match: bool = False,
                             index_name: str = "products") -> Dict[str, Any]:
        """Поиск товаров по атрибутам"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
            return {"total": 0, "products": [], "took": 0}

        try:
            must_conditions = []

            for attr_name, attr_value in attributes.items():
                if exact_match:
                    # Точное совпадение
                    must_conditions.append({
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"attributes.attr_name": attr_name}},
                                        {"term": {"attributes.attr_value.keyword": str(attr_value)}}
                                    ]
                                }
                            }
                        }
                    })
                else:
                    # Нечеткое совпадение
                    must_conditions.append({
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"attributes.attr_name": attr_name}},
                                        {"match": {"attributes.attr_value": str(attr_value)}}
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
                body={
                    "query": query,
                    "size": size,
                    "sort": [{"_score": {"order": "desc"}}]
                }
            )

            return {
                "total": response['hits']['total']['value'],
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"❌ Ошибка поиска по атрибутам: {e}")
            return {"total": 0, "products": [], "took": 0}

    def get_product_by_id(self, product_id: str, index_name: str = "products") -> Optional[Dict[str, Any]]:
        """Получение товара по ID"""
        if not self.es:
            logger.error("Elasticsearch не подключен")
            return None

        try:
            response = self.es.get(index=index_name, id=product_id)
            return response['_source']
        except NotFoundError:
            logger.info(f"Товар {product_id} не найден")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка получения товара {product_id}: {e}")
            return None

    def get_stats(self, index_name: str = "products") -> Dict[str, Any]:
        """Получение статистики индекса"""
        if not self.es:
            return {"error": "Elasticsearch не подключен"}

        try:
            stats = self.es.indices.stats(index=index_name)
            return {
                "documents_count": stats['indices'][index_name]['total']['docs']['count'],
                "store_size_bytes": stats['indices'][index_name]['total']['store']['size_in_bytes'],
                "indexing_total": stats['indices'][index_name]['total']['indexing']['index_total'],
                "search_total": stats['indices'][index_name]['total']['search']['query_total']
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {"error": str(e)}


# Глобальный экземпляр
elasticsearch_service = ElasticsearchService()
