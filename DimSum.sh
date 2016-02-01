#!/bin/bash
# 1. L2Norm MapReduce
# -----------------
# Change A_Matrix.txt with the matrix of your choice.

hadoop fs -rm -r /user/hadoop/l2norm/output
hadoop fs -rm /user/hadoop/l2norm/input/A_Matrix.txt
hadoop fs -put A_Matrix.txt /user/hadoop/l2norm/input/A_Matrix.txt

source ~/.bashrc
hadoop com.sun.tools.javac.Main L2Norm.java
jar cf l2.jar L2Norm*.class
hadoop jar l2.jar L2Norm /user/hadoop/l2norm/input /user/hadoop/l2norm/output

# 2. DimSum MapReduce
# ------------------

hadoop fs -rm -r /user/hadoop/dimsum/output
hadoop fs -rm /user/hadoop/dimsum/input/A_Matrix.txt
hadoop fs -put A_Matrix.txt /user/hadoop/dimsum/input/A_Matrix.txt

hadoop com.sun.tools.javac.Main DimSum.java
jar cf ds.jar DimSum*.class
hadoop jar ds.jar DimSum /user/hadoop/dimsum/input /user/hadoop/dimsum/output
rm B_Matrix.txt
hadoop fs -get /user/hadoop/dimsum/output/part-r-00000 B_Matrix.txt
