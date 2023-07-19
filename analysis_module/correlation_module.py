import matplotlib
from fastapi import Depends

from db.session import get_db

matplotlib.use('Agg')  # Set the backend to 'Agg'

import base64
import io
import itertools
import os
import uuid
import numpy as np
import pandas as pd
import statsmodels.api as sm
from matplotlib import pyplot as plt
from typing_extensions import Union, List, Literal
from .logging_module import logger
import seaborn as sns
import dataframe_image as dfi
from scipy.stats import stats, pearsonr

BASE_PATH = "./output/regression/"


class CorrelationModule:

    def __init__(self, data: Union[np.ndarray, pd.DataFrame]) -> object:
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))

        if isinstance(data, np.ndarray):
            data = pd.DataFrame(data=data)
        self.X: pd.DataFrame = data
        self.selected_columns: List[str] = self.X.columns
        self.directory: str = None

    @property
    def columns(self) -> List[str]:
        return self.X.columns

    def get_pvalue_of_correlation(self):
        num_variables = data.shape[1]
        X = self.X.values

        p_values = np.zeros((num_variables, num_variables))

        for i in range(num_variables):
            for j in range(num_variables):
                if i != j:
                    corr, p_value = pearsonr(X[i], X[j])
                    p_values[i, j] = p_value
        return p_values

    def save_correlation_matrix(self):

        plt.clf()
        if self.X.empty:
            raise AttributeError("data must be initialized")
        self._mkdir()

        # Compute the correlation matrix
        correlation_matrix = self.X.corr()

        # Calculate the p-value matrix
        # p_value_matrix = correlation_matrix.applymap(
        #     lambda x: stats.pearsonr(self.X[x], self.X[x.name])[1] if x != 1.0 else 0.0
        # )
        #
        # # Print the p-value matrix
        # print(p_value_matrix)
        #
        # Plot the correlation matrix
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap="RdYlBu")
        plt.title("Correlation Matrix")

        # Save the plot as a PNG file
        plt.savefig(self.directory + "/correlation_matrix.png", format="png", dpi=300)

    def save_heatmap_plot(self, method: Literal["pearson", "kendall", "spearman"] = "pearson") -> str:
        plt.clf()
        if self.X.empty:
            raise AttributeError("data must be initialized")
        # self._mkdir()
        data = self.X[self.selected_columns]
        corr = data.corr(method=method)
        sns.heatmap(corr, annot=True, cmap="coolwarm", square=True)
        # plt.savefig(self.directory + '/heatmap_plot.jpg')
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=300)
        buffer.seek(0)
        base64_image = base64.b64encode(buffer.read()).decode()
        logger.info("heatmap plot saved successfully")

        return base64_image

    def save_pair_plot(self, method: Literal["pearson", "kendall", "spearman"] = "pearson") -> str:
        plt.clf()
        if self.X.empty:
            raise AttributeError("data must be initialized")
        # self._mkdir()

        scatter_matrix = pd.plotting.scatter_matrix(self.X)
        # Save the pair plot as an image
        plt.savefig(self.directory + "/pair_plot.png")
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=300)
        buffer.seek(0)
        base64_image = base64.b64encode(buffer.read()).decode()
        plt.close()

        logger.info("pair plot saved successfully")

        return base64_image

    def save_descriptive_statistics_table(self):
        if self.X.empty:
            raise AttributeError("data must be initialized")
        # self._mkdir()

        # Compute the descriptive statistics
        statistics = self.X.describe().T
        formatted_df = statistics.applymap(lambda x: "{:.0f}".format(x) if isinstance(x, (int, float)) else x)

        # Style the table
        # dfi.export(formatted_df, self.directory + '/descriptive_statistics.png', table_conversion='chrome')
        # logger.info("discriptive plot saved successfully")

        buffer = io.BytesIO()
        dfi.export(formatted_df, buffer, table_conversion='chrome')
        buffer.seek(0)
        base64_table = base64.b64encode(buffer.read()).decode()

        logger.info("descriptive statistics table converted to base64 successfully")
        return base64_table

    def _mkdir(self):
        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


if __name__ == '__main__':
    data = pd.read_csv('./dataset/pivoted_2021.csv')
    correlation_module = CorrelationModule(data)
    correlation_module.save_pair_plot()
