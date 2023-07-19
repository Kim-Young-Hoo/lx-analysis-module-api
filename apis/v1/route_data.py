from enum import Enum
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from requests import Session
from starlette import status
from schemas.data import *
from db.repository.data import *
from db.session import get_db


router = APIRouter()


class ChartType(str, Enum):
    PIE = "pie"
    HISTOGRAM = "histogram"
    BAR = "bar"



@router.get("/variable", status_code=status.HTTP_200_OK)
def get_variable_list(db: Session = Depends(get_db)):
    """
    통계업무지원 특화서비스 데이터 카탈로그 목록을 반환한다.
    :param db: db session
    :return: 1,2 depth 형태의 카테고리명 string value json
    """
    variable_list = retrieve_variable_list(db)
    return variable_list


@router.get("/variable/{id}", response_model=ShowVariableDetail, status_code=status.HTTP_200_OK)
def get_variable_detail(id: str, db: Session = Depends(get_db)):
    """
    통계업무지원 특화서비스에서 2depth의 상세보기 아이콘을 클릭할 시 데이터 성질에 대한 결과를 반환한다.
    :param id: variable의 아이디 ex) M010001
    :param db: db session
    :return: json 데이터
    """
    variable_detail = retrieve_variable_detail(id, db)

    if not variable_detail:
        raise HTTPException(detail=f"variable with ID {id} does not exist")

    return variable_detail


@router.get("/variable/{id}/chart-data", response_model=ShowVariableChartData, status_code=status.HTTP_200_OK)
def get_variable_chart_data(id: str,
                            year: str,
                            value_period_type: str,
                            chart_type: ChartType = Query(...),
                            db: Session = Depends(get_db)):

    variable_chart_data = retrieve_variable_chart_data(id, year, value_period_type, chart_type, db)

    if not variable_chart_data:
        raise HTTPException(detail=f"variable with ID {id} does not exist")

    return variable_chart_data


# @router.delete("/variable/{id}")
# def delete_blog(id: int, db: Session = Depends(get_db)):
#     id = delete_blog(id=id, db=db)
#     return id