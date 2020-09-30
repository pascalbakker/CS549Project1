Customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);

Customers_Group = GROUP Customers BY CountryCode;
CustomerNameAndCount = FOREACH Customers GENERATE group, COUNT(Customers.Name) as CustomerCount;
CustomerNameAndCount = FILTER CustomerNameAndCount by CustomerCount<2000 or CustomerCount>5000;
STORE res INTO '$output' USING PigStorage(',');
