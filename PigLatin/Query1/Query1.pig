transactions = LOAD '$transactions'  USING PigStorage (',') AS (TRANSID:int, CUSTID:int, TOTAL: int, NUMITEMS:int, DESCRIPTION:charArray);

customers = LOAD '$customers' USING PigStorage (',') AS (CUSTID: int, NAME: charArray, AGE:int, GENDER:charArray, COUNTRY: charArray, SALARY:double);

joinedTables_woGroup = JOIN transactions BY CUSTID, customers by CUSTID;
joinedTables = GROUP joinedTables_woGroup BY customers.NAME;

transaction_counts = FOREACH joinedTables GENERATE FLATTEN(joinedTables_woGroup.NAME) as NAME, COUNT(joinedTables_woGroup.TRANSID) AS trans_count;

transcount_minumum = ORDER transaction_counts BY trans_count;
min = LIMIT transcount_minumum 1;
filtered_limit = FILTER transcount_minumum BY trans_count == min.trans_count;
trans_distinct = DISTINCT filtered_limit;

STORE trans_distinct INTO '$output' USING PigStorage (',');
