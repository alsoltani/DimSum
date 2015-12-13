## Dimension Independent Matrix Square using MapReduce (DIMSUM Algorithm).

#### Advanced Data Analysis, ENSAE 2015.

###### Abstract

A Hadoop implementation of the DimSum algorithm, used to compute the singular values of an 
m × n sparse matrix A in a distributed setting, without communication dependence on m. 
This relates to the the all-pairs similarity problem : when given a dataset of sparse vector data, 
we aim at finding all similar vector pairs according to a similarity function such as cosine similarity, 
and a given similarity score threshold.

###### Hadoop Cheat Sheet

_Ex. with L2Norm :_

Remove existing output folder.      
`hadoop fs -rm -r /user/hadoop/l2norm/output`

(If necessary, to load environment variables)    
`source ~/.bashrc`

Compile and create jar.    
`hadoop com.sun.tools.javac.Main L2Norm.java`    
`jar cf l2.jar L2Norm*.class`

Run MapReduce.    
`hadoop jar l2.jar L2Norm /user/hadoop/l2norm/input /user/hadoop/l2norm/output`

See results in output folder.    
`hadoop fs -ls /user/hadoop/l2norm/output/`


