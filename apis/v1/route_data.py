from enum import Enum
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from requests import Session
from starlette import status
from schemas.data import *
from db.repository.data import *
from db.session import get_db
from datetime import datetime, date

router = APIRouter()


class ChartType(str, Enum):
    PIE = "pie"
    HISTOGRAM = "histogram"
    BAR = "bar"


@router.get("/variable", status_code=status.HTTP_200_OK)
def get_variable_list(year: str, region: Literal["all", "gsbd"],
                      period_unit: Literal["year", "month", "quarter", "half"],
                      detail_period: Literal["all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
                      db: Session = Depends(get_db)):
    """
    통계업무지원 특화서비스 데이터 카탈로그 목록을 반환한다.
    :param db: db session
    :return: 1,2 depth 형태의 카테고리명 string value json
    """
    variable_list = retrieve_variable_list(year, region, period_unit, detail_period, db)
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"variable with ID {id} does not exist")

    return ShowVariableDetail(
        name=variable_detail.dat_nm,
        source=variable_detail.dat_src,
        category=variable_detail.rel_dat_list_nm,
        region_unit=variable_detail.rgn_nm,
        update_cycle=variable_detail.updt_cyle,
        last_update_date=variable_detail.last_mdfcn_dt,
        data_scope=variable_detail.dat_scop_bgng + "-" + variable_detail.dat_scop_end
    )


@router.get("/variable/{id}/chart-data", response_model=ShowVariableChartData, status_code=status.HTTP_200_OK)
def get_variable_chart_data(id: str,
                            year: str,
                            period_unit: str,
                            chart_type: ChartType = Query(...),
                            db: Session = Depends(get_db)):
    variable_chart_data = retrieve_variable_chart_data(id, year, period_unit, chart_type, db)

    if not variable_chart_data:
        raise HTTPException(detail=f"variable with ID {id} does not exist")

    return variable_chart_data

# @router.delete("/variable/{id}")
# def delete_blog(id: int, db: Session = Depends(get_db)):
#     id = delete_blog(id=id, db=db)
#     return id
