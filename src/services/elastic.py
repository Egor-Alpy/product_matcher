from src.repository.elastic import ElasticSearch
from src.repository.mongodb import MongoClient, mongo_client
from src.core.logger import get_logger
import logging

logger = get_logger(name=__name__, level=logging.INFO)


class ElasticService:
    def __init__(self):
        self.es_client = ElasticSearch()
        self.mongo_client = MongoClient()

    async def create_category_indexes(self):
        """Создать индексы под каждую категорию"""

        categories_data = await mongo_client.get_all_categories()

        for category_data in categories_data:
            category_id = category_data['result']['categoryId']
            index_name = str(category_id)
            self.es_client.create_index(index_name=index_name)
            self.insert_documents(index_name=index_name, category_data=category_data)
            logger.info(f"Успешно заполнена категория | id: {category_id}")

        logger.info(f"Индексы для всех категорий успешно созданы!")

    def insert_documents(self, index_name: str, category_data: dict):
        """Вставить данные в индекс ЭС"""

        category_attrs = category_data['result']['parameters']
        for attr in category_attrs:
            if attr['type'] != "ENUM" or attr.get('values') is None:
                continue

            attr_name = attr['name']
            for attr_value in attr['values']:
                document = {attr_name: attr_value}
                self.es_client.add_document(index_name=index_name, document=document)

    async def search_es(self, category_id: int, search_query):
        """Выполнить поиск в индексе ЭС по id категории"""

        try:
            index_name = str(category_id)
            if not self.es_client.is_index_exists(index_name=index_name):
                self.es_client.create_index(index_name=index_name)
                category_data = await self.mongo_client.get_category_by_id(category_id=category_id)
                logger.warning(f"category_data: {category_data}")
                self.insert_documents(index_name=index_name, category_data=category_data)

            response = self.es_client.search_document(index_name=str(category_id), query=search_query)
            response_list = []
            for hit in response['hits']['hits']:
                attr_name = list(hit['_source'].keys())[0]
                attr_value = list(hit['_source'].values())[0]['value']
                response_list.append({attr_name: attr_value})
            return response_list
        except Exception as e:
            logger.error(f"Не удалось выполнить поиск! | {e}")
        return None

    async def search_es_fuzzy(self, category_id: int, search_query):
        """Мягкий поиск документа в ЭС"""

        try:
            index_name = str(category_id)
            if not self.es_client.is_index_exists(index_name=index_name):
                self.es_client.create_index(index_name=index_name)
                category_data = await self.mongo_client.get_category_by_id(category_id=category_id)
                logger.warning(f"category_data: {category_data}")
                self.insert_documents(index_name=index_name, category_data=category_data)

            response = self.es_client.search_document_fuzzy(index_name=str(category_id), query=search_query)
            response_list = []
            for hit in response['hits']['hits']:
                attr_name = list(hit['_source'].keys())[0]
                attr_value = list(hit['_source'].values())[0]['value']
                response_list.append({attr_name: attr_value})
            return response_list
        except Exception as e:
            logger.error(f"Не удалось выполнить поиск! | {e}")
        return None

    async def delete_all_indexes(self):
        """Удалить все индексы эластика"""

        try:
            response = self.es_client.delete_all_indexes()
            return response
        except Exception as e:
            logger.error(f"Ошибка при удалении индексов эластика: {e}")
            return None

    async def create_index(self, index_name: str):
        """Создать индекс эластика"""

        try:
            response = self.es_client.create_index(index_name=index_name)
            return response
        except Exception as e:
            logger.error(f"Ошибка при создании индекса эластика [{index_name}]: {e}")
            return None
ё



elastic_search = ElasticService()
