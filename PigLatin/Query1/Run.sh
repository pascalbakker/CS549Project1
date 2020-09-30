hadoop dfs -rmr ../../data/results/pigquery1/
pig -f Query1.pig -param customers=../../data/Customers.txt -param transactions=../../data/Transactions.txt -param output=../../data/results/pigquery1/
