Customer = load '/user/hadoop/input/customers.db' using PigStorage(',') as (ID, Name, Age, CountryCode, Salary);
transaction = load '/user/hadoop/input/transactions.db' using PigStorage(',') as (TransID,CustID, TransTotal,TransNumItems,TransDesc);
A = group transaction by CustID;
B = foreach A generate group, COUNT(transaction) as NumberOfTrans, SUM(transaction.TransTotal) as total,MIN(transaction.TransNumItems) as min;
C = foreach Customer generate ID, Name, Salary;
D = join C by ID,B by group;
E = foreach D generate ID, Name, Salary, NumberOfTrans, total,min;

store E into '/user/hadoop/output2/Query2';
