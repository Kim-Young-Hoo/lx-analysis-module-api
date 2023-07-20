from datetime import date
from typing import Optional, List, Union, Any

from pydantic import BaseModel, Field, root_validator


class AnalysisResult(BaseModel):
    title: str  # 결과물 이름
    result: Any  # 결과물 (bas64 이미지, html 등의 string)
    format: str  # 결과물 포맷


class ShowAnalysis(BaseModel):
    """
    상관분석 결과를 반환하는 dto
    모든 필드는 base64형 image
    """
    data: List[AnalysisResult]


class BaseAnalysisInput(BaseModel):
    year: str
    value_period_type: str


class CreateCorrelation(BaseAnalysisInput):
    """
    상관분석 시행하기 위한 parameter dto

    {
        "variable_list": ["M026006", "M026002"],
        "year": "2021",
        "value_period_type": "yr_vl",
        "testing_side": "both",
        "valid_pvalue_accent": true
    }
    """
    variable_list: List[str]
    testing_side: str
    valid_pvalue_accent: bool


class CreateRegression(BaseAnalysisInput):
    """
    회귀분석 시행하기 위한 parameter dto
    """
    dependent_variable: str
    independent_variable_list: List[str]


class CreateClustering(BaseAnalysisInput):
    """
    군집분석 시행하기 위한 parameter dto
    """
    variable_list: List[str]
    n_point: int
