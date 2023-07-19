import base64
import io
import os
import uuid
from typing import Union, List, Literal

import numpy as np
import pandas as pd
from statsmodels.stats.anova import anova_lm

from .logging_module import logger
import statsmodels.api as sm
from statsmodels.formula.api import ols
from scipy.stats import pearsonr
import dataframe_image as dfi

BASE_PATH = "./output/regression/"

"""
상관분석 기능: 데이터간 선형관계의 유의성을 분석
 - 상관분석 모형 개발 -> 불가능. 상관분석은 모형 없고 수치만 나옴.
 - 분석수행 영역 창 별도 구성(단계별 탭 구성) 
     - 데이터 입력 및 속성 선택 기능 구성(1단계) -> 완료.
      (데이터는 1개 이상, 속성은 데이터당 2개 이상 구성 -> 완료 (data point가 1이상, feature가 2 이상이라는 의미일 듯)
      (속성은 ‘키 값’과 1개 이상의 변수를 선택) -> 불가능. 독립변수, 종속변수를 나누라는 의미인 것 같은데 상관분석에 없는 개념.
     - 입력 데이터 확인 및 수정 기능 구성(2단계) -> 완료.
     - 분석 데이터셋 생성 및 분석 수행 기능 구성(3단계) -> 완료.
 - 분석 결과는 대시보드에 표출(산점도, 상관매트릭스, 상관계수 표 차트) -> 산점도, 상관계수 매트릭스 완료. 상관계수 표 차트라 건 없음.
 - 분산팽창요인 별도 산출(다중공선성 체크) -> 불가능. 다중공선성은 상관계수나 데이터 성질을 보고 분석자가 스스로 판단해서 제거하는 것.

• 회귀분석 기능
 - 변수간 다중선형회귀모형 추정을 위한 분석 프로세스 제공 -> 히스토그램 등을 통해 변수의 분포도를 확인. 다중공선성 체크 후 변수 선별. null값 제거, 이상치 제거, normalize 등의 preprocessing 등등 다양한 과정이 있는데 어디까지 제공할 건지
 - 분석수행 영역 창 별도 구성(단계별 탭 구성)
     - 데이터 내 설명·종속 변수 선택 기능 구성(1단계) -> 완료.
     - 분석 수행(2단계) -> 완료. 근데 어떤 회귀분석 알고리즘 종류가 다양해서 어떤 걸 제공할 건지 불분명.
     - 회귀모형 평가(3단계) -> 개발 중. 회귀모형의 평가척도는 R-squared value 인데 이 값이 작으면 작을 수록 좋긴 하나 결국 분석자의 이해 필요.
        및 설명변수 재선택 기능 구성(4단계) -> 개발 중.
       (회귀모형 계수 설명 자료 제공 기능 구성) -> 개발 중. 설명자료 제공 가능하나 분석자의 이해 필요.
     - 회귀모형 비교 및 평가(5단계) -> 개발 중.
       및 변수의 상대적 중요도 제공 -> 개발 중. Random Forest 알고리즘은 feature importance 기능을 제공. 그 외 알고리즘에는 그런 게 없고 순전히 분석자의 충분한 이해로 판단 필요. 
"""


class RegressionModule:

    def __init__(self, data: pd.DataFrame, target_column_id: str) -> object:
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))
        self.data = data.iloc[:, 2:]

        self.y: str = target_column_id
        self.X: List[str] = self.data.columns.to_list()
        self.X.remove(self.y)
        self.directory: str = None
        self.model: sm.OLS = None

    def save_descriptive_statistics_table(self):
        """
        기술 통계량
        """
        if self.data.empty:
            raise AttributeError("data must be initialized")
        # self._mkdir()

        # Compute the descriptive statistics
        statistics = self.data.describe().T
        formatted_df = statistics.applymap(lambda x: "{:.0f}".format(x) if isinstance(x, (int, float)) else x)

        buffer = io.BytesIO()
        dfi.export(formatted_df, buffer, table_conversion='chrome')
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("descriptive statistics table converted to base64 successfully")
        return base64_table

    def fit(self):
        # X = sm.add_constant(self.X)

        formula = self.y + " ~ " + " + ".join(self.X)
        print(formula)
        # model = sm.OLS.from_formula(formula, data=self.data)
        model = ols(formula, data=self.data)
        self.model = model.fit()

    def get_result_summary(self) -> str:
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")
        return self.model.summary()._repr_html_()

    def get_anova_lm(self):
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        anova_table = anova_lm(self.model)
        buffer = io.BytesIO()
        dfi.export(anova_table, buffer, table_conversion='chrome')
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        return base64_table

    def predict(self, x):
        pass

    def _mkdir(self):
        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


if __name__ == '__main__':
    data = pd.read_csv('./dataset/pivoted_2021.csv')
    data.dropna()

    regression_module = RegressionModule(data.iloc[:, 2:], 'M020014')
    # print(regression_module.get_covariance())
    # print(regression_module.get_pvalue_of_correlation())

    regression_module.fit()
    print(regression_module.get_result_summary())
    print(regression_module.get_anova_lm())
    regression_module.save_descriptive_statistics_table()
