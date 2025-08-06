import asyncio
import uvicorn
import time

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config.config import settings
from src.core.logger import get_logger
from src.api.router import router
from src.services.elasticsearch_service import elasticsearch_service

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

    for attempt in range(max_startup_retries):
        logger.info(f"🔌 Попытка подключения к Elasticsearch ({attempt + 1}/{max_startup_retries})...")

        try:
            # Подключаемся к Elasticsearch
            es_url = f'http://{settings.ELASTICSEARCH_HOST}:{settings.ELASTICSEARCH_PORT}'
            es_connected = elasticsearch_service.connect([es_url])

            if es_connected:
                logger.info("✅ Elasticsearch подключен успешно!")
                elasticsearch_connected = True

                # Проверяем существование индекса
                index_exists = elasticsearch_service.es.indices.exists(index=settings.ELASTICSEARCH_INDEX)
                if not index_exists:
                    logger.info(f"📝 Создание индекса '{settings.ELASTICSEARCH_INDEX}'...")
                    index_created = elasticsearch_service.create_index(settings.ELASTICSEARCH_INDEX)
                    if index_created:
                        logger.info("✅ Индекс создан успешно")
                    else:
                        logger.warning("⚠️ Не удалось создать индекс")
                else:
                    logger.info(f"✅ Индекс '{settings.ELASTICSEARCH_INDEX}' уже существует")

                # Получаем статистику
                try:
                    stats = elasticsearch_service.get_stats()
                    logger.info(f"📊 Статистика Elasticsearch: {stats.get('documents_count', 0)} документов")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось получить статистику: {e}")

                break

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Elasticsearch (попытка {attempt + 1}): {e}")

        if attempt < max_startup_retries - 1:
            wait_time = min(5 + attempt, 15)  # Увеличиваем время ожидания
            logger.info(f"⏳ Повторная попытка через {wait_time} секунд...")
            time.sleep(wait_time)

    if not elasticsearch_connected:
        logger.error("❌ НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К ELASTICSEARCH!")
        logger.error("⚠️ Приложение будет работать, но функции поиска будут недоступны")

    logger.info("🎉 Приложение запущено успешно!")

    try:
        yield
    except Exception as e:
        logger.error(f"❌ Ошибка во время работы приложения: {e}")
        raise
    finally:
        logger.info("🔄 Остановка приложения...")

        # Закрываем соединение с Elasticsearch
        if elasticsearch_service.es:
            try:
                elasticsearch_service.es.close()
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