hadoop dfs -rmr ../../data/result/pigquery3
pig -f Query3.pig -param customers=../../data/Customers.txt -param transactions=../../data/Transactions.txt -param output=../../data/result/pigquery3
