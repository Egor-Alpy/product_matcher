from src.api.v1.endpoints import health

from fastapi import APIRouter

router = APIRouter(prefix="/v1")

# Подключаем маршруты к эндпоинтам
router.include_router(health.router)
