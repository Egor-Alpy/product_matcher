from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Literal, Optional
from src.services.elastic import elastic_search

router = APIRouter(tags=["Data"], prefix="/data")


@router.delete("/delete_all_indexes")
async def delete_all_indexes():
    """Поиск attr and value в Elasticsearch"""
    try:
        if not elastic_search.es_client:
            raise HTTPException(status_code=503, detail="Elasticsearch недоступен")

        response = await elastic_search.delete_all_indexes()

        return {
            "success": True,
            "details": response
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")


