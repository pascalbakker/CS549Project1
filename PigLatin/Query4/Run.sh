hadoop dfs -rmr ../../data/result/pigquery4
pig -f Query4.pig -param customers=../../data/Customers.txt -param transactions=../../data/Transactions.txt -param output=../../data/result/pigquery4
