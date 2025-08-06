from fastapi import APIRouter, HTTPException


router = APIRouter(tags=["Health"], prefix="/search")


@router.get("/es")
async def health_check():
    """Поиск через простой эластик"""

    products = service.search_products()

    return {"status": "ok"}
