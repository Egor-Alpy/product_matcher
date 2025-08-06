from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel

from src.services.elasticsearch_service import elasticsearch_service
from src.repository.mongodb import MongoDb

router = APIRouter(tags=["Data"], prefix="/data")


class BulkLoadRequest(BaseModel):
    """Модель запроса для массовой загрузки"""
    products: List[Dict[str, Any]]
    index_name: str = "products"


@router.post("/bulk-load")
async def bulk_load_products(request: BulkLoadRequest):
    """Массовая загрузка товаров в Elasticsearch"""

    if not elasticsearch_service.es:
        raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

    if not request.products:
        raise HTTPException(status_code=400, detail="Список товаров пуст")

    try:
        result = elasticsearch_service.bulk_add_products(
            products=request.products,
            index_name=request.index_name
        )

        return {
            "success": True,
            "message": f"Загружено товаров: {result['success']}, ошибок: {result['failed']}",
            "details": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")


@router.post("/load-from-mongodb")
async def load_from_mongodb():
    """Загрузка товаров из MongoDB в Elasticsearch"""

    if not elasticsearch_service.es:
        raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

    try:
        # Получаем данные из MongoDB
        mongo_client = MongoDb()
        products = await mongo_client.get_all_products()

        if not products:
            return {
                "success": True,
                "message": "В MongoDB нет товаров для загрузки",
                "count": 0
            }

        # Загружаем в Elasticsearch
        result = elasticsearch_service.bulk_add_products(products)

        return {
            "success": True,
            "message": f"Загружено из MongoDB: {result['success']} товаров, ошибок: {result['failed']}",
            "details": {
                "mongodb_count": len(products),
                "elasticsearch_result": result
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки из MongoDB: {str(e)}")


@router.get("/count")
async def get_data_count():
    """Получение количества документов в разных источниках"""

    result = {
        "elasticsearch": 0,
        "mongodb": 0,
        "elasticsearch_available": False,
        "mongodb_available": False
    }

    # Проверяем Elasticsearch
    try:
        if elasticsearch_service.es and elasticsearch_service.es.ping():
            result["elasticsearch_available"] = True
            stats = elasticsearch_service.get_stats()
            if "documents_count" in stats:
                result["elasticsearch"] = stats["documents_count"]
    except Exception as e:
        print(f"Elasticsearch error: {e}")

    # Проверяем MongoDB
    try:
        mongo_client = MongoDb()
        products = await mongo_client.get_all_products()
        result["mongodb"] = len(products)
        result["mongodb_available"] = True
    except Exception as e:
        print(f"MongoDB error: {e}")

    return {
        "success": True,
        "data": result
    }


@router.delete("/clear-index")
async def clear_elasticsearch_index():
    """Очистка индекса Elasticsearch (ОСТОРОЖНО!)"""

    if not elasticsearch_service.es:
        raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

    try:
        # Удаляем и пересоздаем индекс
        if elasticsearch_service.es.indices.exists(index="products"):
            elasticsearch_service.es.indices.delete(index="products")

        # Создаем новый пустой индекс
        success = elasticsearch_service.create_index()

        if success:
            return {
                "success": True,
                "message": "Индекс очищен и пересоздан"
            }
        else:
            raise HTTPException(status_code=500, detail="Ошибка пересоздания индекса")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка очистки индекса: {str(e)}")


# Простой эндпоинт для тестирования загрузки одного товара
@router.post("/add-single")
async def add_single_product(product: Dict[str, Any]):
    """Добавление одного товара (для тестирования)"""

    if not elasticsearch_service.es:
        raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

    try:
        success = elasticsearch_service.add_product(product)

        if success:
            return {
                "success": True,
                "message": "Товар добавлен успешно",
                "product_id": product.get('_id', {}).get('$oid', 'unknown')
            }
        else:
            raise HTTPException(status_code=500, detail="Ошибка добавления товара")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка добавления товара: {str(e)}")


@router.get("/sample-product")
async def get_sample_product():
    """Получение примера структуры товара для тестирования"""

    return {
        "sample_product": {
            "_id": {
                "$oid": "sample_product_id_123"
            },
            "title": "Подвесная люстра Ambiente Alicante 8888/3 AB Tear drop",
            "description": "Элегантная подвесная люстра в классическом стиле",
            "article": "8888/3 AB Tear drop",
            "brand": "Ambiente",
            "country_of_origin": "Китай",
            "warranty_months": "12 месяцев",
            "category": "Типы/Люстры/Подвесные",
            "created_at": "22.05.2025 00:51",
            "attributes": [
                {
                    "attr_name": "Высота, мм",
                    "attr_value": "550"
                },
                {
                    "attr_name": "Диаметр, мм",
                    "attr_value": "700"
                },
                {
                    "attr_name": "Количество ламп",
                    "attr_value": "3"
                },
                {
                    "attr_name": "Мощность лампы, W",
                    "attr_value": "4"
                }
            ],
            "suppliers": [
                {
                    "supplier_name": "test_supplier",
                    "supplier_offers": [
                        {
                            "price": [
                                {
                                    "qnt": 1,
                                    "discount": 0,
                                    "price": 104274
                                }
                            ],
                            "stock": "В наличии"
                        }
                    ]
                }
            ]
        },
        "usage": "Используйте этот пример для тестирования POST /api/v1/data/add-single"
    }