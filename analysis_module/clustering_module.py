import os
from abc import abstractmethod, ABCMeta

import numpy
import numpy as np
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import uuid
import pickle

from sklearn.mixture import GaussianMixture

from .logging_module import logger

BASE_PATH = "./output/clustering/"


class BaseModule(metaclass=ABCMeta):
    def __init__(self, data: numpy.array):
        self.uuid = uuid.uuid4()
        logger.info("class uuid : " + str(self.uuid))
        self.directory: str = None
        self.data: np.array = data
        self.model: object = None
        self.db_connection: object = None

    @abstractmethod
    def fit(self, n_init=100, max_iter=300) -> None: pass

    @abstractmethod
    def predict(self, data): pass

    def save_model(self) -> None:

        if not self.model:
            raise AttributeError("model is not fitted yet")

        self._mkdir()

        with open(self.directory + "/model.pickle", "wb") as fw:
            pickle.dump(self, fw)

    def _mkdir(self) -> None:

        if not os.path.exists(BASE_PATH):
            os.mkdir(BASE_PATH)

        if not self.directory:
            self.directory = BASE_PATH + str(self.uuid)
            os.mkdir(self.directory)


class BaseClusteringModule(BaseModule):

    def __init__(self, data):
        super().__init__(data)

    @abstractmethod
    def set_optimal_k(self, method: str) -> None: pass

    @abstractmethod
    def save_k_method_output_plot(self) -> None: pass

    @abstractmethod
    def save_cluster_output_plot(self) -> None: pass

    @abstractmethod
    def save_data_scatter_plot(self) -> None: pass

    @abstractmethod
    def fit(self, n_init=100, max_iter=300) -> None: pass

    @abstractmethod
    def predict(self, data): pass


class GMMModule(BaseClusteringModule):
    optimal_k_methods = {"BIC", "AIC"}

    def __init__(self, data: numpy.array):
        super().__init__(data)

        self.optimal_k: int = 2
        self.k_method: str = None
        self.k_range: range = range(2, 10)
        self.bic_scores = []
        self.aic_scores = []

    def __str__(self):
        return """
        GMM model
        uuid : {uuid}
        k_method : {k_method}
        k : {k}
        """.format(uuid=self.uuid, k_method=self.k_method, k=self.optimal_k)

    def set_k_range(self, start, end) -> None:
        if start < 2:
            raise ValueError("start must be larger than 1")
        self.k_range = range(start, end)

    def set_optimal_k(self, method: str = "AIC", fixed_size=2) -> None:

        if method and method not in self.optimal_k_methods:
            raise ValueError("not supported method")

        for n in self.k_range:
            gmm = GaussianMixture(n_components=n)
            gmm.fit(self.data)
            self.bic_scores.append(gmm.bic(self.data))
            self.aic_scores.append(gmm.aic(self.data))

        if method == "BIC":
            self.optimal_k = list(self.k_range)[np.argmin(self.bic_scores)]
        elif method == "AIC":
            self.optimal_k = list(self.k_range)[np.argmin(self.aic_scores)]

        logger.info("optimal k is set as : " + str(self.optimal_k))

    def fit(self, n_init=100, max_iter=300) -> None:

        if not self.data.any():
            raise AttributeError("data must be initialized")

        self.model = GaussianMixture(
            n_components=self.optimal_k,
            n_init=n_init,
            max_iter=max_iter
        ).fit(self.data)

        logger.info("model is successfully fitted")

    def predict(self, data):
        return NotImplemented

    def save_k_method_output_plot(self) -> None:

        plt.clf()
        self._mkdir()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        plt.figure(figsize=(10, 6))
        plt.plot(self.k_range, self.bic_scores, label='BIC')
        plt.plot(self.k_range, self.aic_scores, label='AIC')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Score')
        plt.title('BIC and AIC Scores for GMM')
        plt.legend()

        # Find the index of minimum BIC and AIC scores
        min_bic_idx = np.argmin(self.bic_scores)
        min_aic_idx = np.argmin(self.aic_scores)

        # Add markers for minimum scores
        plt.scatter(list(self.k_range)[np.argmin(self.bic_scores)], self.bic_scores[min_bic_idx], color='blue',
                    marker='o', label='Min BIC')
        plt.scatter(list(self.k_range)[np.argmin(self.aic_scores)], self.aic_scores[min_aic_idx], color='red',
                    marker='o', label='Min AIC')
        plt.savefig(self.directory + '/aic_bic_scores.jpg')

    def save_cluster_output_plot(self) -> None:
        plt.clf()
        if not self.model:
            raise AttributeError("model is not fitted yet")

        if not self.data.any():
            raise AttributeError("data must be initialized")

        labels = self.model.predict(self.data)

        for label in range(self.optimal_k):
            plt.scatter(self.data[labels == label, 0], self.data[labels == label, 1], label=f'Cluster {label + 1}')

        plt.legend()

        self._mkdir()
        plt.savefig(self.directory + '/cluster_output.jpg')
        logger.info("clustering output plot saved successfully")

    def save_data_scatter_plot(self) -> None:
        plt.clf()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        self._mkdir()
        plt.scatter(self.data[:, 0], self.data[:, 1])
        plt.savefig(self.directory + '/data_scatter_plot.jpg')
        logger.info("data scatter plot saved successfully")


class KMeansModule(BaseClusteringModule):
    optimal_k_methods = {"wcss", "silhouette"}

    def __init__(self, data: numpy.array):
        super().__init__(data)

        self.optimal_k: int = 2
        self.k_method: str = None
        self.silhouette_scores: list = []
        self.wcss: list = []
        self.k_range: range = range(2, 10)

    def __str__(self):
        return """
        K-Means model
        uuid : {uuid}
        k_method : {k_method}
        k : {k}
        """.format(uuid=self.uuid, k_method=self.k_method, k=self.optimal_k)

    def set_k_range(self, start, end) -> None:
        if start < 2:
            raise ValueError("start must be larger than 1")
        self.k_range = range(start, end)

    def set_optimal_k(self, method: str = "silhouette", fixed_size=2) -> None:

        if method and method not in self.optimal_k_methods:
            raise ValueError("not supported method")

        if method == "silhouette":
            for _k in self.k_range:
                kmeans = KMeans(n_clusters=_k, n_init=30, max_iter=30).fit(self.data)
                self.silhouette_scores.append(silhouette_score(self.data, kmeans.labels_))
            self.optimal_k = self.k_range[np.argmax(self.silhouette_scores)]
            self.k_method = method

        elif method == "wcss":
            for _k in self.k_range:
                kmeans = KMeans(n_clusters=_k, n_init=30, max_iter=30).fit(self.data)
                self.wcss.append(kmeans.inertia_)
            elbow_index = np.argmin(np.diff(self.wcss)) + 1
            self.optimal_k = self.wcss[elbow_index]
            self.k_method = method

        else:
            self.optimal_k = fixed_size
            self.k_method = None

        logger.info("optimal k is set as : " + str(self.optimal_k))

    def fit(self, n_init=100, max_iter=300) -> None:

        if not self.data.any():
            raise AttributeError("data must be initialized")

        self.model = KMeans(
            n_clusters=self.optimal_k,
            n_init=n_init,
            max_iter=max_iter
        ).fit(self.data)

        logger.info("model is successfully fitted")

    def predict(self, data):
        return NotImplemented

    def save_k_method_output_plot(self) -> None:

        plt.clf()
        self._mkdir()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        if self.k_method == "silhouette":
            plt.bar(self.k_range, self.silhouette_scores)
            plt.xlabel('Number of clusters (k)')
            plt.ylabel('Silhouette Score')
            plt.title('Silhouette Scores for Different Number of Clusters')
            max_index = np.argmax(self.silhouette_scores)
            plt.bar(self.k_range[max_index], self.silhouette_scores[max_index], color='red')
            plt.savefig(self.directory + "/silhouette_scores.jpg")
            logger.info("silhouette scores plot saved successfully")

        elif self.k_method == "wcss":
            plt.plot(self.k_range, self.wcss, marker='o')
            plt.xlabel('Number of Clusters (k)')
            plt.ylabel('WCSS')
            plt.title('Elbow Point Plot')
            plt.axvline(x=self.optimal_k, color='r', linestyle='--', label='Elbow Point')
            plt.legend()
            plt.savefig(self.directory + "/wcss.jpg")
            logger.info("elbow point plot saved successfully")

        else:
            logger.warning("no screenshot to save")

    def save_cluster_output_plot(self) -> None:
        plt.clf()
        if not self.model:
            raise AttributeError("model is not fitted yet")

        if not self.data.any():
            raise AttributeError("data must be initialized")

        labels = self.model.labels_

        for label in range(self.optimal_k):
            plt.scatter(self.data[labels == label, 0], self.data[labels == label, 1], label=f'Cluster {label + 1}')

        plt.legend()

        self._mkdir()
        plt.savefig(self.directory + '/cluster_output.jpg')
        logger.info("clustering output plot saved successfully")

    def save_data_scatter_plot(self) -> None:
        plt.clf()
        if not self.data.any():
            raise AttributeError("data must be initialized")

        self._mkdir()
        plt.scatter(self.data[:, 0], self.data[:, 1])
        plt.savefig(self.directory + '/data_scatter_plot.jpg')
        logger.info("data scatter plot saved successfully")


if __name__ == '__main__':
    # 군집분석 샘플데이터 생성
    # n_samples : 샘플 수
    # centers : 군집 수
    # x : 데이터
    # y : 레이블
    x, y = make_blobs(n_samples=5000, cluster_std=1.0, centers=5)

    kmeans = KMeansModule(x)
    kmeans.save_data_scatter_plot()
    kmeans.set_k_range(2, 10)
    kmeans.set_optimal_k()
    kmeans.fit()
    kmeans.save_model()
    kmeans.save_k_method_output_plot()
    kmeans.save_cluster_output_plot()

    gmm = GMMModule(x)
    gmm.save_data_scatter_plot()
    gmm.set_k_range(2, 10)
    gmm.set_optimal_k()
    gmm.fit()
    gmm.save_model()
    gmm.save_k_method_output_plot()
    gmm.save_cluster_output_plot()

    # with open("./c603868d-4109-4216-ae82-f5055c05ee52/model.pickle", "rb") as fr:
    #     kmeans = pickle.load(fr)
    # print(kmeans)
