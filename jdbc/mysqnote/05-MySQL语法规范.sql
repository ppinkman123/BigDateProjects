 -- MySQL里的关键字和函数以及数据库名，表名，字段名等都是不区分大小写
USE BiGDATA0409;
SELECT AGE
from PERSON;

-- 数据库里的值要根据校对集来确定是否区分大小写
-- 如果校对集里包含 ci表示不区分大小，如果有  cs或者bin表示区分大小，默认不区分大小写
select AGE, gender, hobbies
from person
where name = 'Jack';
show create database bigdata0409;  -- 校对集使用的是 utf8mb4_0900_ai_ci 不区分大小写

-- 创建了一个数据库 demo,并指定 demo数据库的 字符串和校对集
create database demo charset = utf8mb4 collate utf8mb4_general_ci;
create database demo; -- 没有指定字符集和校对集，默认使用 utf8mb4字符集，和 utf8mb4_0900_ai_ci校对集
show create database demo;  -- 显示建库语句


create table `table`(id int);
