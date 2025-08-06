from src.api.v1.endpoints import health, data

from fastapi import APIRouter

router_modules = [health, data]

router = APIRouter(prefix="/v1")

# Подключаем маршруты к эндпоинтам

for router_module in router_modules:
    router.include_router(router_module.router)
