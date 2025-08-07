import time
import traceback
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk
from typing import Dict, List, Any, Optional
from bson import ObjectId
from src.config.config import settings
from src.core.logger import get_logger

logger = get_logger(name=__name__)


class ElasticsearchService:
    """Полный сервис для работы с Elasticsearch для товаров"""

    def __init__(self):
        self.es = None
        self.index_name = "products"
        self.connection_attempts = 0
        logger.info("ElasticsearchService initialized")

    def connect(self, hosts: List[str] = None, retry: bool = True) -> bool:
        """Подключение к Elasticsearch с retry логикой"""
        if hosts is None:
            es_host = settings.ELASTICSEARCH_HOST
            es_port = settings.ELASTICSEARCH_PORT
            hosts = [f'http://{es_host}:{es_port}']

        logger.info(f"Attempting to connect to Elasticsearch: {hosts}")

        max_retries = 5 if retry else 1
        retry_delay = 2

        for attempt in range(max_retries):
            self.connection_attempts += 1

            try:
                logger.info(f"Connection attempt {attempt + 1}/{max_retries} to {hosts}")

                # Создаем клиент с базовыми настройками
                self.es = Elasticsearch(
                    hosts,
                    timeout=10,
                    max_retries=2,
                    retry_on_timeout=True
                )

                # Проверяем подключение
                if self.es.ping():
                    logger.info(f"✅ Elasticsearch connection successful: {hosts}")
                    return True
                else:
                    logger.warning(f"Elasticsearch ping failed on attempt {attempt + 1}")

            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {type(e).__name__}: {e}")
                self.es = None

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 10)  # Max 10 секунд

        logger.error(f"❌ Failed to connect to Elasticsearch after {max_retries} attempts")
        self.es = None
        return False

    def is_connected(self) -> bool:
        """Проверка активного подключения"""
        if not self.es:
            return False
        try:
            return self.es.ping()
        except Exception:
            return False

    def create_index(self, index_name: str = "products") -> bool:
        """Создание индекса для товаров"""
        if not self.ensure_connection():
            logger.error("Cannot create index: Elasticsearch not connected")
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
                logger.info(f"Существующий es-индекс {index_name} удален")

            # Создаем новый индекс
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"es-индекс {index_name} успешно создан")
            return True

        except Exception as e:
            logger.error(f"Ошибка создания индекса {index_name}: {e}")
            return False

    def _extract_document_id(self, product_data: Dict[str, Any]) -> Optional[str]:
        """Извлечение ID документа из данных продукта"""
        if '_id' not in product_data:
            return None

        _id = product_data['_id']

        if isinstance(_id, ObjectId):
            return str(_id)
        elif isinstance(_id, dict) and '$oid' in _id:
            return _id['$oid']
        elif isinstance(_id, str):
            return _id
        else:
            logger.warning(f"Неизвестный тип _id: {type(_id)}, значение: {_id}")
            return str(_id)

    def _prepare_product_data(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Подготовка данных продукта для индексации"""
        # Создаем копию данных, чтобы не изменять оригинал
        prepared_data = product_data.copy()

        # Обрабатываем атрибуты для создания flat_attributes
        if 'attributes' in prepared_data and isinstance(prepared_data['attributes'], list):
            flat_attributes = {}
            for attr in prepared_data['attributes']:
                if isinstance(attr, dict):
                    attr_name = attr.get('attr_name')
                    attr_value = attr.get('attr_value')
                    if attr_name and attr_value is not None:
                        # Обрезаем слишком длинные значения
                        attr_value_str = str(attr_value)
                        if len(attr_value_str) > 1000:
                            attr_value_str = attr_value_str[:1000] + "..."
                        flat_attributes[attr_name] = attr_value_str

            if flat_attributes:
                prepared_data['flat_attributes'] = flat_attributes

        # Валидируем и очищаем проблемные поля
        prepared_data = self._validate_and_clean_data(prepared_data)

        # Преобразуем ObjectId в строку для всех вложенных объектов
        prepared_data = self._convert_objectids_to_strings(prepared_data)

        return prepared_data

    def _validate_and_clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Валидация и очистка данных перед индексацией"""
        cleaned_data = {}

        for key, value in data.items():
            try:
                if key == '_id':
                    continue  # ID обрабатывается отдельно

                # Очищаем текстовые поля от слишком больших значений
                if isinstance(value, str) and len(value) > 10000:
                    logger.warning(f"Обрезаем слишком длинное поле {key}: {len(value)} символов")
                    cleaned_data[key] = value[:10000] + "..."
                elif isinstance(value, list):
                    # Валидируем списки
                    cleaned_list = []
                    for item in value[:1000]:  # Ограничиваем количество элементов
                        if isinstance(item, dict):
                            cleaned_item = self._validate_and_clean_data(item)
                            cleaned_list.append(cleaned_item)
                        else:
                            cleaned_list.append(item)
                    cleaned_data[key] = cleaned_list
                elif isinstance(value, dict):
                    # Рекурсивно обрабатываем вложенные словари
                    cleaned_data[key] = self._validate_and_clean_data(value)
                else:
                    cleaned_data[key] = value

            except Exception as e:
                logger.warning(f"Ошибка обработки поля {key}: {e}, пропускаем")
                continue

        return cleaned_data

    def _convert_objectids_to_strings(self, data: Any) -> Any:
        """Рекурсивно преобразует ObjectId в строки"""
        if isinstance(data, ObjectId):
            return str(data)
        elif isinstance(data, dict):
            return {key: self._convert_objectids_to_strings(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._convert_objectids_to_strings(item) for item in data]
        else:
            return data

    def add_product(self, product_data: Dict[str, Any], index_name: str = "products") -> bool:
        """Добавление товара в индекс"""
        if not self.ensure_connection():
            logger.error("Cannot add product: Elasticsearch not connected")
            return False

        try:
            # Извлекаем ID документа
            doc_id = self._extract_document_id(product_data)

            # Подготавливаем данные для индексации
            prepared_data = self._prepare_product_data(product_data)

            # Добавляем товар в индекс
            response = self.es.index(
                index=index_name,
                id=doc_id,
                document=prepared_data
            )

            logger.debug(f"Товар добавлен в индекс: {response['result']}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка добавления товара: {e}")
            logger.error(f"Данные товара: {product_data}")
            return False

    def bulk_add_products(self, products: List[Dict[str, Any]], index_name: str = "products") -> Dict[str, int]:
        """Массовое добавление товаров"""
        if not self.ensure_connection():
            logger.error("Cannot bulk add products: Elasticsearch not connected")
            return {"success": 0, "failed": len(products), "total": len(products)}

        if not products:
            logger.warning("Список товаров пуст")
            return {"success": 0, "failed": 0, "total": 0}

        try:
            actions = []
            failed_products = 0

            for i, product in enumerate(products):
                try:
                    # Извлекаем ID документа
                    doc_id = self._extract_document_id(product)

                    # Подготавливаем данные для индексации
                    prepared_data = self._prepare_product_data(product)

                    action = {
                        "_index": index_name,
                        "_id": doc_id,
                        "_source": prepared_data
                    }
                    actions.append(action)

                except Exception as e:
                    logger.error(f"❌ Ошибка подготовки товара {i}: {e}")
                    logger.error(f"Проблемный товар: {product}")
                    failed_products += 1
                    continue

            if not actions:
                logger.error("Не удалось подготовить ни одного товара для индексации")
                return {"success": 0, "failed": len(products), "total": len(products)}

            logger.info(f"Подготовлено {len(actions)} товаров для bulk операции")

            # Выполняем bulk операцию с детальной обработкой ошибок
            try:
                success_count, failed_items = bulk(
                    self.es,
                    actions,
                    chunk_size=500,
                    request_timeout=60,
                    max_retries=3,
                    raise_on_error=False,  # Не прерываем выполнение при ошибках
                    raise_on_exception=False  # Не прерываем при исключениях
                )
            except Exception as bulk_error:
                logger.error(f"❌ Ошибка выполнения bulk операции: {bulk_error}")
                # Пробуем выполнить операцию по частям для диагностики
                return self._fallback_bulk_add(actions, index_name, failed_products, len(products))

            failed_count = len(failed_items) if failed_items else 0
            total_failed = failed_count + failed_products

            # Подробное логирование ошибок
            if failed_items:
                logger.error(f"❌ Детали неудачных операций ({len(failed_items)} из {len(actions)}):")
                for i, failed_item in enumerate(failed_items[:10]):  # Показываем первые 10
                    logger.error(f"  Ошибка {i + 1}: {failed_item}")
                if len(failed_items) > 10:
                    logger.error(f"  ... и еще {len(failed_items) - 10} ошибок")

            result = {
                "success": success_count,
                "failed": total_failed,
                "total": len(products)
            }

            if total_failed == 0:
                logger.info(f"✅ Все товары успешно добавлены: {success_count} из {len(products)}")
            else:
                logger.warning(
                    f"⚠️ Bulk добавление завершено: {success_count} успешно, {total_failed} ошибок из {len(products)} товаров")

            return result

        except Exception as e:
            logger.error(f"❌ Критическая ошибка bulk добавления: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"success": 0, "failed": len(products), "total": len(products)}

    def _fallback_bulk_add(self, actions: List[Dict], index_name: str, initial_failed: int, total_products: int) -> \
    Dict[str, int]:
        """Fallback метод для добавления товаров по одному при ошибке bulk операции"""
        logger.info("Пробуем добавить товары по одному для диагностики...")

        success_count = 0
        failed_count = initial_failed

        for i, action in enumerate(actions[:10]):  # Пробуем первые 10 для диагностики
            try:
                response = self.es.index(
                    index=action["_index"],
                    id=action["_id"],
                    document=action["_source"]
                )
                success_count += 1
                logger.debug(f"Товар {i + 1} добавлен успешно: {response['result']}")
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Ошибка добавления товара {i + 1}: {e}")
                logger.error(f"ID товара: {action.get('_id')}")
                logger.error(f"Данные товара: {str(action['_source'])[:200]}...")

        return {
            "success": success_count,
            "failed": failed_count + (len(actions) - 10),  # Остальные считаем неудачными
            "total": total_products
        }

    def search_products_by_title(self,
                                 query: str,
                                 size: int = 10,
                                 from_: int = 0,
                                 index_name: str = "products") -> Dict[str, Any]:
        """Поиск товаров только по названию, бренду, описанию и артикулу"""
        if not self.ensure_connection():
            logger.error("Cannot search products by title: Elasticsearch not connected")
            return {"total": 0, "products": [], "took": 0}

        if not query or not query.strip():
            logger.warning("Пустой поисковый запрос для поиска по названию")
            return {"total": 0, "products": [], "took": 0}

        try:
            search_query = {
                "multi_match": {
                    "query": query.strip(),
                    "fields": [
                        "title^3",  # Название товара - наивысший приоритет
                        "brand^2",  # Бренд - высокий приоритет
                        "description^1.5",  # Описание - средний приоритет
                        "article"  # Артикул - обычный приоритет
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            }

            full_query = {
                "query": search_query,
                "size": size,
                "from": from_,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"created_at": {"order": "desc"}}
                ]
            }

            response = self.es.search(index=index_name, body=full_query)

            total = response['hits']['total']
            if isinstance(total, dict):
                total_value = total.get('value', 0)
            else:
                total_value = total

            return {
                "total": total_value,
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"❌ Ошибка поиска товаров по названию: {e}")
            return {"total": 0, "products": [], "took": 0}

    def search_products_by_attributes_text(self,
                                           query: str,
                                           size: int = 10,
                                           from_: int = 0,
                                           index_name: str = "products") -> Dict[str, Any]:
        """Поиск товаров только по текстовым значениям характеристик"""
        if not self.ensure_connection():
            logger.error("Cannot search products by attributes: Elasticsearch not connected")
            return {"total": 0, "products": [], "took": 0}

        if not query or not query.strip():
            logger.warning("Пустой поисковый запрос для поиска по характеристикам")
            return {"total": 0, "products": [], "took": 0}

        try:
            search_query = {
                "nested": {
                    "path": "attributes",
                    "query": {
                        "match": {
                            "attributes.attr_value": {
                                "query": query.strip(),
                                "fuzziness": "AUTO"
                            }
                        }
                    },
                    "inner_hits": {}  # Показывать какие именно атрибуты совпали
                }
            }

            full_query = {
                "query": search_query,
                "size": size,
                "from": from_,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"created_at": {"order": "desc"}}
                ]
            }

            response = self.es.search(index=index_name, body=full_query)

            total = response['hits']['total']
            if isinstance(total, dict):
                total_value = total.get('value', 0)
            else:
                total_value = total

            # Добавляем информацию о совпавших атрибутах
            products = []
            for hit in response['hits']['hits']:
                product = hit['_source'].copy()

                # Добавляем информацию о совпавших атрибутах
                if 'inner_hits' in hit and 'attributes' in hit['inner_hits']:
                    matched_attributes = []
                    for inner_hit in hit['inner_hits']['attributes']['hits']['hits']:
                        matched_attributes.append(inner_hit['_source'])
                    product['_matched_attributes'] = matched_attributes

                products.append(product)

            return {
                "total": total_value,
                "products": products,
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"❌ Ошибка поиска товаров по характеристикам: {e}")
            return {"total": 0, "products": [], "took": 0}

    def search_products(self,
                        query: str = None,
                        filters: Dict[str, Any] = None,
                        size: int = 10,
                        from_: int = 0,
                        index_name: str = "products") -> Dict[str, Any]:
        """Обобщенный поиск товаров (для обратной совместимости)"""
        if not self.ensure_connection():
            logger.error("Cannot search products: Elasticsearch not connected")
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
                    if field == "brand" and value:
                        must_conditions.append({"term": {"brand": value}})
                    elif field == "category" and value:
                        must_conditions.append({"term": {"category": value}})
                    elif field == "price_from" and value is not None:
                        must_conditions.append({
                            "nested": {
                                "path": "suppliers.supplier_offers",
                                "query": {
                                    "range": {"suppliers.supplier_offers.price.price": {"gte": value}}
                                }
                            }
                        })
                    elif field == "price_to" and value is not None:
                        must_conditions.append({
                            "nested": {
                                "path": "suppliers.supplier_offers",
                                "query": {
                                    "range": {"suppliers.supplier_offers.price.price": {"lte": value}}
                                }
                            }
                        })

                if len(must_conditions) > 1:
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

            total = response['hits']['total']
            if isinstance(total, dict):
                total_value = total.get('value', 0)
            else:
                total_value = total

            return {
                "total": total_value,
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
        """Поиск товаров по конкретным атрибутам (ключ-значение)"""
        if not self.ensure_connection():
            logger.error("Cannot search by attributes: Elasticsearch not connected")
            return {"total": 0, "products": [], "took": 0}

        if not attributes:
            logger.warning("Атрибуты для поиска не указаны")
            return {"total": 0, "products": [], "took": 0}

        try:
            must_conditions = []

            for attr_name, attr_value in attributes.items():
                if attr_value is None:
                    continue

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

            if not must_conditions:
                logger.warning("Не найдено валидных условий для поиска")
                return {"total": 0, "products": [], "took": 0}

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

            total = response['hits']['total']
            if isinstance(total, dict):
                total_value = total.get('value', 0)
            else:
                total_value = total

            return {
                "total": total_value,
                "products": [hit['_source'] for hit in response['hits']['hits']],
                "took": response['took']
            }

        except Exception as e:
            logger.error(f"❌ Ошибка поиска по атрибутам: {e}")
            return {"total": 0, "products": [], "took": 0}

    def get_product_by_id(self, product_id: str, index_name: str = "products") -> Optional[Dict[str, Any]]:
        """Получение товара по ID"""
        if not self.ensure_connection():
            logger.error("Cannot get product by ID: Elasticsearch not connected")
            return None

        if not product_id:
            logger.error("Product ID не может быть пустым")
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
        if not self.ensure_connection():
            return {"error": "Elasticsearch not connected"}

        try:
            if not self.es.indices.exists(index=index_name):
                return {"error": f"Index {index_name} does not exist", "documents_count": 0}

            stats = self.es.indices.stats(index=index_name)
            index_stats = stats['indices'][index_name]['total']

            return {
                "documents_count": index_stats['docs']['count'],
                "store_size_bytes": index_stats['store']['size_in_bytes'],
                "indexing_total": index_stats['indexing']['index_total'],
                "search_total": index_stats['search']['query_total'],
                "connection_attempts": self.connection_attempts
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {"error": str(e)}

    def delete_product(self, product_id: str, index_name: str = "products") -> bool:
        """Удаление товара по ID"""
        if not self.ensure_connection():
            logger.error("Cannot delete product: Elasticsearch not connected")
            return False

        if not product_id:
            logger.error("Product ID не может быть пустым")
            return False

        try:
            response = self.es.delete(index=index_name, id=product_id)
            logger.info(f"✅ Товар {product_id} удален: {response['result']}")
            return True
        except NotFoundError:
            logger.warning(f"Товар {product_id} не найден для удаления")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка удаления товара {product_id}: {e}")
            return False

    def clear_index(self, index_name: str = "products") -> bool:
        """Очистка индекса"""
        if not self.ensure_connection():
            logger.error("Cannot clear index: Elasticsearch not connected")
            return False

        try:
            response = self.es.delete_by_query(
                index=index_name,
                body={"query": {"match_all": {}}}
            )

            deleted_count = response.get('deleted', 0)
            logger.info(f"✅ Индекс {index_name} очищен, удалено документов: {deleted_count}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка очистки индекса {index_name}: {e}")
            return False

    def diagnose_products_data(self, products: List[Dict[str, Any]], sample_size: int = 5) -> Dict[str, Any]:
        """Диагностика структуры данных товаров"""
        if not products:
            return {"error": "Список товаров пуст"}

        diagnosis = {
            "total_products": len(products),
            "sample_products": [],
            "common_fields": {},
            "problematic_fields": [],
            "data_types": {},
        }

        # Анализируем образцы товаров
        for i, product in enumerate(products[:sample_size]):
            sample_info = {
                "index": i,
                "fields_count": len(product),
                "has_id": "_id" in product,
                "id_type": str(type(product.get("_id", "None"))),
                "fields": list(product.keys())
            }

            # Проверяем ID
            if "_id" in product:
                _id = product["_id"]
                if isinstance(_id, ObjectId):
                    sample_info["id_value"] = f"ObjectId({str(_id)})"
                elif isinstance(_id, dict):
                    sample_info["id_value"] = str(_id)
                else:
                    sample_info["id_value"] = str(_id)

            # Проверяем размер данных
            try:
                import json
                data_size = len(json.dumps(product, default=str))
                sample_info["data_size_bytes"] = data_size
                if data_size > 1000000:  # 1MB
                    diagnosis["problematic_fields"].append(f"Товар {i}: слишком большой размер ({data_size} байт)")
            except Exception as e:
                sample_info["serialization_error"] = str(e)
                diagnosis["problematic_fields"].append(f"Товар {i}: ошибка сериализации - {e}")

            diagnosis["sample_products"].append(sample_info)

        # Собираем статистику по полям
        all_fields = set()
        for product in products[:100]:  # Анализируем первые 100
            all_fields.update(product.keys())

        for field in all_fields:
            field_types = set()
            field_count = 0
            for product in products[:100]:
                if field in product:
                    field_count += 1
                    field_types.add(str(type(product[field])))

            diagnosis["common_fields"][field] = {
                "presence_rate": f"{field_count}/100",
                "types": list(field_types)
            }

        return diagnosis

    def get_aggregations(self, field: str, size: int = 20, index_name: str = "products") -> Dict[str, Any]:
        """Получение агрегации по указанному полю"""
        if not self.ensure_connection():
            logger.error("Cannot get aggregations: Elasticsearch not connected")
            return {"error": "Elasticsearch not connected"}

        try:
            query = {
                "size": 0,
                "aggs": {
                    f"{field}_agg": {
                        "terms": {
                            "field": field,
                            "size": size
                        }
                    }
                }
            }

            response = self.es.search(index=index_name, body=query)

            aggregations = []
            for bucket in response['aggregations'][f'{field}_agg']['buckets']:
                aggregations.append({
                    "key": bucket['key'],
                    "count": bucket['doc_count']
                })

            return {
                "field": field,
                "aggregations": aggregations,
                "total_unique_values": len(aggregations)
            }

        except Exception as e:
            logger.error(f"❌ Ошибка получения агрегации для поля {field}: {e}")
            return {"error": str(e)}

    def get_price_range_stats(self, index_name: str = "products") -> Dict[str, Any]:
        """Получение статистики по ценам"""
        if not self.ensure_connection():
            logger.error("Cannot get price stats: Elasticsearch not connected")
            return {"error": "Elasticsearch not connected"}

        try:
            query = {
                "size": 0,
                "aggs": {
                    "price_stats": {
                        "nested": {
                            "path": "suppliers.supplier_offers"
                        },
                        "aggs": {
                            "price_range": {
                                "stats": {
                                    "field": "suppliers.supplier_offers.price.price"
                                }
                            }
                        }
                    }
                }
            }

            response = self.es.search(index=index_name, body=query)

            price_stats = response['aggregations']['price_stats']['price_range']

            return {
                "min": price_stats.get('min'),
                "max": price_stats.get('max'),
                "avg": price_stats.get('avg'),
                "count": price_stats.get('count', 0)
            }

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики цен: {e}")
            return {"error": str(e)}

    def update_product(self, product_id: str, update_data: Dict[str, Any], index_name: str = "products") -> bool:
        """Обновление товара по ID"""
        if not self.ensure_connection():
            logger.error("Cannot update product: Elasticsearch not connected")
            return False

        if not product_id:
            logger.error("Product ID не может быть пустым")
            return False

        try:
            # Подготавливаем данные для обновления
            prepared_data = self._prepare_product_data(update_data)

            response = self.es.update(
                index=index_name,
                id=product_id,
                body={"doc": prepared_data}
            )

            logger.info(f"✅ Товар {product_id} обновлен: {response['result']}")
            return True

        except NotFoundError:
            logger.warning(f"Товар {product_id} не найден для обновления")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка обновления товара {product_id}: {e}")
            return False

    def close(self):
        """Закрытие соединения с Elasticsearch"""
        if self.es:
            try:
                self.es.close()
                logger.info("✅ Соединение с Elasticsearch закрыто")
            except Exception as e:
                logger.error(f"❌ Ошибка при закрытии соединения с Elasticsearch: {e}")


# Глобальный экземпляр
elasticsearch_service = ElasticsearchService()