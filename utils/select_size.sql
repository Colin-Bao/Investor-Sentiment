SELECT table_schema                                 as '数据库',
       sum(table_rows)                              as '记录数',
       sum(truncate(data_length / 1024 / 1024, 2))  as '数据容量(MB)',
       sum(truncate(index_length / 1024 / 1024, 2)) as '索引容量(MB)',
       sum(truncate(DATA_FREE / 1024 / 1024, 2))    as '碎片占用(MB)'
from information_schema.tables
group by table_schema
order by sum(data_length) desc, sum(index_length) desc;

# 查看 MySQL「指定库」中「所有表」的容量大小
SELECT table_schema                            as '数据库',
       table_name                              as '表名',
       table_rows                              as '记录数',
       truncate(data_length / 1024 / 1024, 2)  as '数据容量(MB)',
       truncate(index_length / 1024 / 1024, 2) as '索引容量(MB)',
       truncate(DATA_FREE / 1024 / 1024, 2)    as '碎片占用(MB)'
from information_schema.tables
where table_schema = 'FIN_DAILY_BASIC'
order by data_length desc, index_length desc;

# 查看 MySQL 数据库中，容量排名前 10 的表
USE information_schema;
SELECT TABLE_SCHEMA                            as '数据库',
       table_name                              as '表名',
       table_rows                              as '记录数',
       ENGINE                                  as '存储引擎',
       truncate(data_length / 1024 / 1024, 2)  as '数据容量(MB)',
       truncate(index_length / 1024 / 1024, 2) as '索引容量(MB)',
       truncate(DATA_FREE / 1024 / 1024, 2)    as '碎片占用(MB)'
from tables
order by table_rows desc
limit 10;