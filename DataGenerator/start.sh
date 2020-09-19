javac DataGenerator.java
java DataGenerator

hadoop dfs -mkdir /user/hadoop/Project1/data
hadoop dfs -copyFromLocal Transactions.txt ../data
hadoop dfs -copyFromLocal Customers.txt ../data
