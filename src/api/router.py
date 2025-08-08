from fastapi import APIRouter
from src.api.v1.router import router as v1_router


router = APIRouter()

# Корневой endpoint вызывающийся при переходе по адресу сервиса
@router.get("/", tags=["Root"])
async def root():
    return {"message": "Bool Attrs Normalizer Service"}

router.include_router(v1_router, prefix='/api')
