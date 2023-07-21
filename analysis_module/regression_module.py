import base64
import io
import os
import uuid
from typing import List

import pandas as pd
from statsmodels.stats.anova import anova_lm

from utils.logging_module import logger
import statsmodels.api as sm
from statsmodels.formula.api import ols
import dataframe_image as dfi

BASE_PATH = "./output/regression/"


class RegressionModule:

    def __init__(self, data: pd.DataFrame, target_column_id: str, dat_no_dat_nm_dict: dict) -> object:
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))
        self.data = data

        self.y_column_id: str = target_column_id
        self.X_column_id_list: List[str] = self.data.columns.to_list()

        self.X_column_id_list.remove(self.y_column_id)
        self.directory: str = None
        self.model: sm.OLS = None
        self.name_dict: dict = dat_no_dat_nm_dict

    def save_descriptive_statistics_table(self):
        if self.data.empty:
            raise AttributeError("data must be initialized")

        statistics = self.data.describe()
        statistics = statistics.rename(columns=self.name_dict)
        statistics = statistics.T
        formatted_df = statistics.applymap(lambda x: "{:.0f}".format(x) if isinstance(x, (int, float)) else x)
        buffer = io.BytesIO()
        dfi.export(formatted_df, buffer, table_conversion='chrome')
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("descriptive statistics table converted to base64 successfully")
        return base64_table

    def fit(self):
        formula = self.y_column_id + " ~ " + " + ".join(self.X_column_id_list)
        model = ols(formula, data=self.data)
        self.model = model.fit()

    def get_result_summary(self) -> str:
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")
        # return self.model.summary()._repr_html_()

        summary_df = pd.read_html(self.model.summary().tables[0].as_html())[0]
        summary_df.iat[7, 2] = summary_df.iat[8, 0]
        summary_df.iat[7, 3] = summary_df.iat[8, 1]
        summary_df = summary_df.drop(8)

        # Custom index and header lists
        custom_index = ['', '', '', '', '', '', '', '']
        custom_header = ['속성', '값', '속성', '값']

        # Set the custom index and header
        summary_df.index = custom_index
        summary_df.columns = custom_header
        

        buffer = io.BytesIO()
        dfi.export(summary_df, buffer, table_conversion='chrome')
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("summary table converted to base64 successfully")

        return base64_table

    def get_anova_lm(self):
        if not self.model:
            raise AttributeError("A model hasn't been fitted yet")

        anova_table = anova_lm(self.model)
        anova_table = anova_table.rename(
            columns={"df": "자유도", "sum_sq": "제곱합", "mean_sq": "평균제곱", "F": "F-통계량"},
            index=self.name_dict
        )
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
