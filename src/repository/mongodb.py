import voyageai
from motor.motor_asyncio import AsyncIOMotorClient

from src.config.config import settings
from src.core.logger import get_logger
import logging

logger = get_logger(name='mongodb')


class MongoDb:
    def __init__(self):
        self.client = AsyncIOMotorClient(settings.MONGO_CONNECTION_LINK_ATLAS)
        self.db = self.client[settings.MONGO_DB_NAME]
        self.collection = self.db[settings.MONGO_COLLECTION_NAME_VECTOR_CATEGORIES]

    async def create_vector_index(self):
        """Создать векторный поисковой индекс в MongoDB"""
        try:
            # Check if index already exists
            existing_indexes = await self.collection.list_search_indexes()
            if any(idx.get('name') == 'vector_index' for idx in existing_indexes):
                logger.info("Vector index already exists")
                return

            # Create the index with correct MongoDB Atlas format
            search_index_model = {
                "name": "vector_index",
                "definition": {
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "_embedding": {
                                "type": "knnVector",
                                "dimensions": settings.OUTPUT_DIMENSION,
                                "similarity": "cosine"
                            }
                        }
                    }
                }
            }

            await self.collection.create_search_index(model=search_index_model)
            logger.info("Vector index creation initiated")

            # Wait for index to be created (async operation)
            import time
            MAX_WAIT_ITERATIONS = 30
            WAIT_INTERVAL_SECONDS = 2
            LOG_INTERVAL = 5

            for i in range(MAX_WAIT_ITERATIONS):
                time.sleep(WAIT_INTERVAL_SECONDS)
                indexes = list(self.collection.list_search_indexes())
                if any(idx.get('name') == 'vector_index' for idx in indexes):
                    logger.info(f"Vector index created successfully after {(i + 1) * WAIT_INTERVAL_SECONDS} seconds")
                    break
                if i % LOG_INTERVAL == 0:
                    logger.info(f"Waiting for index creation... ({i + 1}/{MAX_WAIT_ITERATIONS})")
            else:
                logger.warning("Vector index creation timed out - it may still be creating")

        except Exception as e:
            logger.error(f"Error creating vector index: {e}")

    async def clear_collection(self):
        """Удалить все документы из коллекции"""
        result = await self.collection.delete_many({})
        logger.debug(f"Deleted {result.deleted_count} documents from collection")

    async def insert_batch(self, documents):
        """Insert batch of documents"""
        if documents:
            result = await self.collection.insert_many(documents)
            return len(result.inserted_ids)
        return 0

    async def get_all_products(self):
        logger.debug(f'Getting all products, from collection: {self.collection.name}, {self.db.name}, {settings.get_mongo_connection_link}')
        cursor = self.collection.find()
        products = await cursor.to_list()
        logger.debug(f"products: {products}")
        logger.debug(f"Fetched {len(products)} products from collection")
        return products


# Global database instance
db = MongoDb()
