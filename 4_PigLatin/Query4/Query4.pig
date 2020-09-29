Customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
Customers_ = FOREACH Customers GENERATE ID, Name, Salary;

Customers_ = FOREACH Customers GENERATE ID, Gender, (CASE Age / 10
                                                WHEN 1 THEN '[10-20)' 
                                                WHEN 2 THEN '[20-30)' 
                                                WHEN 3 THEN '[30-40)' 
                                                WHEN 4 THEN '[40-50)' 
                                                WHEN 5 THEN '[50-60)' 
                                                ELSE '[60-70)' END) AS Age_;

Transactions = LOAD '$transactions' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
Transactions_ = FOREACH Transactions GENERATE CustID, TransTotal;

Join_CT = JOIN Transactions_ by CustID, Customers_ by ID USING 'replicated';
Join_CT_Group =  GROUP Join_CT BY (Age_, Gender);

Out = FOREACH Join_CT_Group GENERATE group, MIN(Join_CT.TransTotal), MAX(Join_CT.TransTotal), AVG(Join_CT.TransTotal);
STORE Out INTO '$output';


















