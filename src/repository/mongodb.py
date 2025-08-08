from motor.motor_asyncio import AsyncIOMotorClient
from src.config.config import settings
from src.core.logger import get_logger

logger = get_logger(name=__name__)


class MongoClient:
    def __init__(self):
        self.client = None
        self.database = None
        self._collection = None
        self.connect()

    def connect(self):
        mongo_db_name = settings.MONGO_DB_NAME
        self.client = AsyncIOMotorClient(settings.get_mongo_connection_link)
        self.database = self.client[mongo_db_name]
        logger.info(f"MongoDB подключен: {mongo_db_name}")

    async def disconnect(self):
        if self.client:
            self.client.close()

    async def get_all_categories(self) -> list:
        """
        Получаем все атрибуты из коллекции, которая указана в project_config.py
        """
        cursor = self.database[settings.MONGO_COLLECTION_NAME_ATTRIBUTES].find()
        documents = await cursor.to_list()
        return documents

    async def get_category_by_id(self, category_id: int):
        cursor = await self.database[settings.MONGO_COLLECTION_NAME_ATTRIBUTES].find_one(
            {"result.categoryId": category_id}
        )
        return cursor

    async def get_all_products(self, collection_name: str):
        """Получаем все товары из конкретной коллекции монго"""
        pass

mongo_client = MongoClient()
