hadoop dfs -rmr /user/hadoop/output
pig -f Query2.pig -param customers=/user/hadoop/Project1/data/Customers.txt -param transactions=/user/hadoop/Project1/data/Transactions.txt -param output=/user/hadoop/output
