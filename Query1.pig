cus = LOAD 'hdfs://localhost:8020/user/hadoop/cache/customer.csv' USING PigStorage(',') as (ID,name,age,cc,salary);
trans = LOAD 'hdfs://localhost:8020/user/hadoop/dft/transaction.csv' USING PigStorage(',') as (transID,cusID,transTotal,transNum,transDesc);
record = GROUP trans BY cusID;
num = FOREACH record GENERATE group,COUNT(trans);
group_all = GROUP num ALL;
least = FOREACH group_all GENERATE MIN(num.$1);
ct = JOIN cus BY ID, num BY $0 USING 'replicated';
result = FOREACH ct GENERATE $1, $6;
final = FILTER result BY $1==least.$0;
STORE final INTO 'pigresult1' USING PigStorage(',');

