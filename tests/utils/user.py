from sqlalchemy.orm import Session
from db.repository.user import create_new_user
from schemas.user import CreateUser


def create_random_user(db: Session):
    user = CreateUser(email="ping@fastapitutorial.com",password="Hello!")
    user = create_new_user(user=user, db=db)
    return user