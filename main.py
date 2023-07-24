from fastapi import FastAPI

from db.base import Base
from db.session import engine
from core.config import settings
from apis.base import api_router


def include_router(app):
    app.include_router(api_router)


# def create_tables():
#     Base.metadata.create_all(bind=engine)


def start_application():
    app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION, root_path="/statistics")
    include_router(app)
    # create_tables()
    return app


app = start_application()
