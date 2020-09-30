hadoop dfs -rmr ../data/..results/pigquery2
pig -f Query2.pig -param customers=../../data/Customers.txt -param transactions=../../data/Transactions.txt -param output=../../data/results/pigquery2/
