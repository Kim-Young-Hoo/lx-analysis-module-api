from fastapi import APIRouter

from apis.v1 import route_analysis

from apis.v1 import route_data

api_router = APIRouter()
api_router.include_router(route_data.router, prefix="/statistics/data", tags=["data"])
api_router.include_router(route_analysis.router, prefix="/statistics/analysis", tags=["analysis"])