import asyncio
import uvicorn

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config.config import settings
from src.core.logger import get_logger
from src.api.router import router

logger = get_logger(name=__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.debug("Запуск приложения...")
    try:
        logger.debug("Приложение запущено успешно")
        yield
    except Exception as e:
        logger.error(f"Ошибка при запуске: {e}")
        raise
    finally:
        logger.debug("Остановка приложения...")
        # await engine.dispose()
        logger.info("Приложение остановлено")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESC,
    version=settings.PROJECT_VERSION,
    lifespan=lifespan
)

app.include_router(router)

def main():

    try:
        logger.info(f"{settings.PROJECT_NAME} has been started!")
        uvicorn.run(
            app=settings.API_APP,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level=settings.API_LOG_LEVEL
        )
    except Exception as e:
        logger.error(f'{settings.PROJECT_NAME} has been finished with error: {e}')


if __name__ == "__main__":
    main()
