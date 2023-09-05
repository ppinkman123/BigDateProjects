/*
DDL 数据定义语言，不涉及到 数值的操作
只是用来建库建表，查看数据库和表，修改数据库和表结构，删除库和表
*/
show databases; -- 显示所有的数据库
create database demo charset utf8mb4 collate utf8mb4_bin; -- 创建一个数据库，并指定数据库字符集和校对集
show create database demo; -- 查看某个数据库的建库语句
use bigdata0409; -- 切换到指定的数据库
show tables; -- 显示这个数据库下所有的表
show tables from demo; -- 可以查看demo数据库里所有的表

use demo; -- 切换到demo数据库
create table t1
(
    id   int,
    name varchar(32)
); -- 在demo库里创建一个 t1表格

create table bigdata0409.tt
(
    id int,
    x  boolean
); -- 在 bigdata0409库里创建一个tt表格

show create table t1; -- 查看建表语句
describe t1; -- 查看一个表的结构
drop table t1; -- 删除表
drop database demo; -- 删库

alter table bigdata0409.person add column city varchar(32);  -- 在表格里新增一个字段

-- 删除字段，修改字段属性，修改字段名等操作都要尽量避免!!!
alter table bigdata0409.person drop column isPass;
alter table bigdata0409.t1 modify x bigint;
alter table bigdata0409.t3 modify x int;

alter table bigdata0409.t4 change gender sex varchar(32);  -- 修改字段名的同时修改字段的属性

-- 修改字段的位置没有实际意义
alter table bigdata0409.t2 modify z decimal(5,2) first;
alter table bigdata0409.t2 modify y double after z;

alter table bigdata0409.tt rename bigdata0409.t8;
rename table bigdata0409.t8 to bigdata0409.tt;