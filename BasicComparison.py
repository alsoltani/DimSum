# coding:latin-1

# Dimension Independent Matrix Square using MapReduce.
# ----------------------------------------------------
# A very basic comparison.

import numpy as np
import pandas as pd
import subprocess
import random
from scipy import sparse


def cosine_similarities(matrix):

    m, n = matrix.shape
    cosine_sim = np.zeros((n, n))

    for i in xrange(n):
        for j in xrange(n):
            cosine_sim[i, j] = matrix[:, i].dot(matrix[:, j]) / \
                               (np.linalg.norm(matrix[:, i]) * np.linalg.norm(matrix[:, j]))

    return cosine_sim

if __name__ == "__main__":

    # Create the original matrix.

    """A = sparse.lil_matrix((pow(10, 5), pow(10, 2)))
    for i in xrange(A.shape[0]):

        values = random.sample(range(A.shape[1]), 2)
        A[i, values] = np.random.uniform(-1, 1, 2)

    np.savetxt("DimSum/A_Matrix.txt", A.todense().astype(np.float16))"""

    # Perform Hadoop job by running bash code.
    # Do not forget to chmod u+wx DimSum.sh before.

    subprocess.call("bash DimSum/DimSum.sh", shell=True)

    # Load data.

    # A = pd.read_csv("DimSum/A_Matrix.txt", sep=" ", header=None).values
    A = pd.read_csv("A_Matrix.txt", sep=" ", header=None).values
    norm_A = np.sqrt(np.sum(A*A, axis=0))

    # Output DimSum results.

    # B = pd.read_csv("DimSum/B_Matrix.txt", sep="\t", header=None)
    B = pd.read_csv("B_Matrix.txt", sep="\t", header=None)
    B.columns = ["idx", "col", "value"]
    B = B.pivot(index='idx', columns='col', values='value')

    # Output naive cosine similarities.

    cos = cosine_similarities(A)

    # Compare the two results.

    error_rate = np.linalg.norm(B.values - cos) / np.linalg.norm(cos)
    print "Error rate :", error_rate
