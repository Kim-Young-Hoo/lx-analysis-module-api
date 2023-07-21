from datetime import date, datetime
from typing import List

from pydantic import EmailStr, BaseModel, Field


class ShowVariable(BaseModel):
    """
    변수 목록 반환하기 위한 dto
    """
    id: int
    name: str
    children: List['ShowVariable'] = []


class ShowVariableDetail(BaseModel):
    """
    변수의 상세정보를 반환하기 위한 dto
    """
    name: str
    source: str
    category: str
    unit: str
    update_cycle: str
    last_update_date: datetime
    data_scope: str


class ShowVariableChartData(BaseModel):
    """
    변수의 기초적인 차트를 그리기 위한 data dto
    TODO: chart type에 대해서 enum화 필요
    """
    name: str
    char_type: str
    area_code: str
    data_value: float


