Customer = load '/user/hadoop/input/customers.db' using PigStorage(',') as (ID, Name, Age, CountryCode, Salary);
transaction = load '/user/hadoop/input/transactions.db' using PigStorage(',') as (TransID,CustID, TransTotal,TransNumItems,TransDesc);
A = foreach Customer generate ID,CountryCode;
Z = foreach transaction generate CustID, TransTotal;
B = join A by ID, Z by CustID;
C = group B by CountryCode;
D = foreach C generate group, MIN(B.TransTotal) AS min,MAX(B.TransTotal) as max;
E = group A by CountryCode;
F = foreach E generate group, COUNT(A) as count;
G = join F by group, D by group;
H = foreach G generate F::group,count, min, max; 
store H into '/user/hadoop/output2/Query3';
