from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Literal
from src.services.elastic import elastic_search

router = APIRouter(tags=["Search"], prefix="/search")


@router.post("/search_es")
async def search_es(category_id: int, search_query: dict[str, str], offset: int = 1):
    """Поиск attr and value в Elasticsearch"""
    try:
        if not elastic_search.es_client:
            raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

        response = await elastic_search.search_es(category_id=category_id, search_query=search_query)

        return {
            "success": True,
            "details": response[:offset]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")

@router.post("/search_es_fuzzy")
async def search_es(category_id: int, search_query: dict[str, str]):
    """Поиск attr and value в Elasticsearch"""
    try:
        if not elastic_search.es_client:
            raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

        response = await elastic_search.search_es_fuzzy(category_id=category_id, search_query=search_query)

        return {
            "success": True,
            "details": response
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")
