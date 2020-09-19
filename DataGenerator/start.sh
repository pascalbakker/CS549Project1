javac DataGenerator.java
java DataGenerator

hadoop dfs -mkdir /user/hadoop/Project1/data
hadoop dfs -copyFromLocal Transactions.txt /user/hadoop/Project1/data
hadoop dfs -copyFromLocal Customers.txt /user/hadoop/Project1/data
