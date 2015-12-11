## Dimension Independent Matrix Square using MapReduce (DIMSUM Algorithm).

### Advanced Data Analysis, ENSAE 2015.

A Hadoop implementation of the DimSum algorithm, used to compute the singular values of an 
m × n sparse matrix A in a distributed setting, without communication dependence on m. 
This relates to the the all-pairs similarity problem : when given a dataset of sparse vector data, 
we aim at finding all similar vector pairs according to a similarity function such as cosine similarity, 
and a given similarity score threshold.
