import numpy as np


class OvoidTooSmall(Exception):
    pass


class OvoidSingularCovariance(Exception):
    pass


class OvoidNegativeSquared(Exception):
    pass


class Ovoid:
    def __init__(self, category, embeddings):
        self.category = category
        if embeddings.shape[0] < (embeddings.shape[1] + 1):
            raise OvoidTooSmall
        cov = np.cov(embeddings.transpose())
        try:
            self.inv_cov = np.linalg.inv(cov)
        except np.linalg.LinAlgError as err:
            raise OvoidSingularCovariance
        self.mean = np.mean(embeddings, axis=0)
        count, width = np.shape(embeddings)
        # print(f"OVOID: {category} {count} {width} {self.mean} {self.inv_cov}")

    def distance(self, embedding):
        delta = np.array(embedding["embeddings"]) - self.mean
        squared_mhb = np.sum((delta * np.matmul(self.inv_cov, delta)), axis=0)
        if squared_mhb < 0:
            raise OvoidNegativeSquared
        return np.sqrt(squared_mhb)
