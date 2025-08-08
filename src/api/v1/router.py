from src.api.v1.endpoints import health, search, data

from fastapi import APIRouter

router_modules = [health, search, data]

router = APIRouter(prefix="/v1")

# Подключаем маршруты к эндпоинтам

for router_module in router_modules:
    router.include_router(router_module.router)
