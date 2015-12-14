# coding:latin-1

# Dimension Independent Matrix Square using MapReduce.
# ----------------------------------------------------
# A very basic comparison.


import numpy as np
import pandas as pd


def cosine_similarities(matrix):

    m, n = matrix.shape
    cosine_sim = np.zeros((n, n))

    for i in xrange(n):
        for j in xrange(n):
            cosine_sim[i, j] = matrix[:, i].dot(matrix[:, j]) / \
                               (np.linalg.norm(matrix[:, i]) * np.linalg.norm(matrix[:, j]))

    return cosine_sim

if __name__ == "__main__":

    # Create simple array.

    A = np.random.randint(10, size=(10 ^ 4, 10 ^ 2))
    np.savetxt("DimSum/A_Matrix.txt", A)

    # Output DimSum results via MapReduce.

    B = pd.read_csv("DimSum/B_Matrix.txt", sep="\t", header=None)
    B.columns = ["idx", "col", "value"]
    B = B.pivot(index='idx', columns='col', values='value')
    print B

    # Output naive cosine similarities.

    print cosine_similarities(A)

    # Compare the two results.

    print np.linalg.norm(B.values - cosine_similarities(A))
