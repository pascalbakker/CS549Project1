Customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
Customers_ = FOREACH Customers GENERATE ID, Name, Salary;

Transactions = LOAD '$transactions' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
Transactions_ = FOREACH Transactions GENERATE CustID, TransID, TransTotal, TransNumItems;

Join_CT = JOIN Transactions_ by CustID, Customers_ by ID USING 'replicated';
Join_CT_Group =  GROUP Join_CT BY (ID, Name, Salary);

Out = FOREACH Join_CT_Group GENERATE group, COUNT(Join_CT.TransTotal), SUM(Join_CT.TransTotal), MIN(Join_CT.TransNumItems);
STORE Out INTO '$output';


















