import asyncio
import uvicorn
import time

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config.config import settings
from src.core.logger import get_logger
from src.api.router import router
from src.services.elastic import elastic_search

logger = get_logger(name=__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.debug("🚀 Запуск приложения...")

    # Инициализируем Elasticsearch с задержкой для ожидания готовности контейнера
    elasticsearch_connected = False
    max_startup_retries = 10
    startup_delay = 3

    logger.info("⏳ Ожидание готовности Elasticsearch...")
    time.sleep(startup_delay)  # Даем время Elasticsearch полностью запуститься

    logger.info("🎉 Приложение запущено успешно!")

    try:
        yield
    except Exception as e:
        logger.error(f"❌ Ошибка во время работы приложения: {e}")
        raise
    finally:
        logger.info("🔄 Остановка приложения...")

        # Закрываем соединение с Elasticsearch
        if elastic_search.es:
            try:
                elastic_search.es.close()
                logger.info("✅ Соединение с Elasticsearch закрыто")
            except Exception as e:
                logger.error(f"❌ Ошибка при закрытии соединения с Elasticsearch: {e}")

        logger.info("👋 Приложение остановлено")


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESC,
    version=settings.PROJECT_VERSION,
    lifespan=lifespan
)

# Добавляем CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутеры
app.include_router(router)


def main():
    try:
        logger.info(f"🚀 {settings.PROJECT_NAME} запускается...")
        logger.info(f"🔧 Настройки:")
        logger.info(f"   - API: {settings.API_HOST}:{settings.API_PORT}")
        logger.info(f"   - Elasticsearch: http://{settings.ELASTICSEARCH_HOST}:{settings.ELASTICSEARCH_PORT}")
        logger.info(f"   - MongoDB: {settings.get_mongo_connection_link}")

        uvicorn.run(
            app=settings.API_APP,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level=settings.API_LOG_LEVEL
        )
    except Exception as e:
        logger.error(f'❌ {settings.PROJECT_NAME} завершился с ошибкой: {e}')


if __name__ == "__main__":
    main()