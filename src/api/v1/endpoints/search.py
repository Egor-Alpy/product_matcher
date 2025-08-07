from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Literal
from src.services.elasticsearch_service import elasticsearch_service

router = APIRouter(tags=["Search"], prefix="/search")


@router.get("/title")
async def search_by_title(
        q: str = Query(..., description="Поисковый запрос по названию товара, бренду, описанию, артикулу"),
        size: int = Query(10, ge=1, le=100, description="Количество результатов (1-100)"),
        from_: int = Query(0, ge=0, alias="from", description="Смещение для пагинации")
):
    """
    Поиск товаров только по названию и основным полям

    Ищет по полям:
    - Название товара (title) - приоритет x3
    - Бренд (brand) - приоритет x2
    - Описание (description) - приоритет x1.5
    - Артикул (article) - приоритет x1

    НЕ ищет в характеристиках товара

    Примеры запросов:
    - /api/v1/search/title?q=люстра подвесная
    - /api/v1/search/title?q=Ambiente
    - /api/v1/search/title?q=Alicante 8888
    """

    if not elasticsearch_service.ensure_connection():
        raise HTTPException(status_code=503, detail="Поиск недоступен - нет соединения с Elasticsearch")

    try:
        result = elasticsearch_service.search_products_by_title(
            query=q,
            size=size,
            from_=from_
        )

        return {
            "success": True,
            "search_type": "title",
            "query": q,
            "total": result["total"],
            "returned": len(result["products"]),
            "took": result["took"],
            "products": result["products"],
            "pagination": {
                "size": size,
                "from": from_,
                "has_more": (from_ + size) < result["total"]
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка поиска по названию: {str(e)}"
        )


@router.get("/attributes")
async def search_by_attributes(
        q: str = Query(..., description="Поисковый запрос по характеристикам товара"),
        size: int = Query(10, ge=1, le=100, description="Количество результатов (1-100)"),
        from_: int = Query(0, ge=0, alias="from", description="Смещение для пагинации")
):
    """
    Поиск товаров только по характеристикам (атрибутам)

    Ищет только в значениях характеристик товара (attributes.attr_value)

    НЕ ищет в названии, бренде, описании или артикуле

    Примеры запросов:
    - /api/v1/search/attributes?q=550 мм
    - /api/v1/search/attributes?q=3 лампы
    - /api/v1/search/attributes?q=высота 550
    - /api/v1/search/attributes?q=диаметр 700
    """

    if not elasticsearch_service.ensure_connection():
        raise HTTPException(status_code=503, detail="Поиск недоступен - нет соединения с Elasticsearch")

    try:
        result = elasticsearch_service.search_products_by_attributes_text(
            query=q,
            size=size,
            from_=from_
        )

        return {
            "success": True,
            "search_type": "attributes",
            "query": q,
            "total": result["total"],
            "returned": len(result["products"]),
            "took": result["took"],
            "products": result["products"],
            "pagination": {
                "size": size,
                "from": from_,
                "has_more": (from_ + size) < result["total"]
            },
            "note": "Поиск выполнен только по характеристикам товара. В результатах может присутствовать поле '_matched_attributes' с совпавшими атрибутами."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка поиска по характеристикам: {str(e)}"
        )


@router.get("/")
async def search_combined(
        q: str = Query(..., description="Поисковый запрос"),
        search_type: Literal["title", "attributes", "both"] = Query("both", description="Тип поиска"),
        size: int = Query(10, ge=1, le=100, description="Количество результатов (1-100)"),
        from_: int = Query(0, ge=0, alias="from", description="Смещение для пагинации")
):
    """
    Универсальный поиск с выбором типа

    search_type параметры:
    - "title": поиск только по названию, бренду, описанию, артикулу
    - "attributes": поиск только по характеристикам товара
    - "both": поиск и по названию, и по характеристикам (возвращает объединенные результаты)

    Примеры:
    - /api/v1/search/?q=люстра&search_type=title
    - /api/v1/search/?q=550 мм&search_type=attributes
    - /api/v1/search/?q=люстра высота 550&search_type=both
    """

    if not elasticsearch_service.ensure_connection():
        raise HTTPException(status_code=503, detail="Поиск недоступен - нет соединения с Elasticsearch")

    try:
        if search_type == "title":
            result = elasticsearch_service.search_products_by_title(
                query=q,
                size=size,
                from_=from_
            )
        elif search_type == "attributes":
            result = elasticsearch_service.search_products_by_attributes_text(
                query=q,
                size=size,
                from_=from_
            )
        else:  # both
            # Выполняем оба поиска и объединяем результаты
            title_result = elasticsearch_service.search_products_by_title(
                query=q,
                size=size // 2,  # Половина результатов из поиска по названию
                from_=from_ // 2
            )

            attr_result = elasticsearch_service.search_products_by_attributes_text(
                query=q,
                size=size // 2,  # Половина результатов из поиска по атрибутам
                from_=from_ // 2
            )

            # Объединяем результаты, избегая дубликатов
            combined_products = title_result["products"].copy()
            seen_ids = set()

            # Добавляем ID из товаров по названию
            for product in combined_products:
                if "_id" in product:
                    seen_ids.add(str(product["_id"]))

            # Добавляем товары из поиска по атрибутам, избегая дубликатов
            for product in attr_result["products"]:
                product_id = str(product.get("_id", ""))
                if product_id not in seen_ids:
                    combined_products.append(product)
                    seen_ids.add(product_id)

            # Ограничиваем общее количество результатов
            combined_products = combined_products[:size]

            result = {
                "total": title_result["total"] + attr_result["total"],
                "products": combined_products,
                "took": max(title_result["took"], attr_result["took"]),
                "title_results": title_result["total"],
                "attributes_results": attr_result["total"]
            }

        return {
            "success": True,
            "search_type": search_type,
            "query": q,
            "total": result["total"],
            "returned": len(result["products"]),
            "took": result["took"],
            "products": result["products"],
            "pagination": {
                "size": size,
                "from": from_,
                "has_more": (from_ + size) < result["total"]
            },
            **({"title_results": result["title_results"],
                "attributes_results": result["attributes_results"]} if search_type == "both" else {})
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка поиска ({search_type}): {str(e)}"
        )


@router.get("/help")
async def search_help():
    """Справка по использованию поисковых эндпоинтов"""

    return {
        "success": True,
        "endpoints": {
            "/api/v1/search/title": {
                "description": "Поиск только по названию, бренду, описанию и артикулу товара",
                "fields": ["title (x3)", "brand (x2)", "description (x1.5)", "article (x1)"],
                "examples": [
                    "/api/v1/search/title?q=люстра подвесная",
                    "/api/v1/search/title?q=Ambiente",
                    "/api/v1/search/title?q=Alicante"
                ]
            },
            "/api/v1/search/attributes": {
                "description": "Поиск только по характеристикам товара",
                "fields": ["attributes.attr_value"],
                "examples": [
                    "/api/v1/search/attributes?q=550 мм",
                    "/api/v1/search/attributes?q=3 лампы",
                    "/api/v1/search/attributes?q=высота 550"
                ]
            },
            "/api/v1/search/": {
                "description": "Универсальный поиск с выбором типа",
                "parameters": {
                    "search_type": ["title", "attributes", "both"]
                },
                "examples": [
                    "/api/v1/search/?q=люстра&search_type=title",
                    "/api/v1/search/?q=550 мм&search_type=attributes",
                    "/api/v1/search/?q=люстра 550&search_type=both"
                ]
            }
        },
        "tips": [
            "Для поиска конкретных товаров используйте /title",
            "Для поиска по размерам, количеству, материалам используйте /attributes",
            "Для комбинированного поиска используйте /?search_type=both",
            "Результаты сортируются по релевантности, затем по дате создания"
        ]
    }