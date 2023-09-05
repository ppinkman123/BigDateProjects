/*
DDL 不涉及到数值本身
DML 是对数据进行增删改 操作
*/
create database bigdata0409 character set utf8;
use bigdata0409;

create table person
(
    id       int primary key auto_increment,
    name     varchar(32),
    gender   varchar(4),
    province varchar(16),
    height   double
);

-- 给指定字段插入数据
insert into person(name, province) value ('jack', '湖北');

-- 对于自增的主键，可以传入 0 或者 null 表示自增
insert into person(id, name, gender, province, height) value (null, 'jerry', '男', '北京', 1.8);

-- 如果给所有的字段都插入了值，字段名可以省略不写，字段值和字段名一一对应
insert into person value (null, 'merry', '女', '江苏', 1.65);

-- 通过  values  可以插入多个数据
insert into person
values (null, 'helen', '女', '浙江', 1.63),
       (null, 'jim', '男', '江西', 1.75);

insert into person(name, province, gender, id, height) value ('tony', '河南', '男', null, 1.78);

# update 语句通常要和  where语句 配合使用，添加条件查询
update person
set name='崔崔'
where name like '%e%';

delete from person;  -- 清空表里所有的数据，并没有删除过表，如果再添加数据，自增的键会接着自增
truncate person; -- truncate相当于将这个表删除以后又重建了
/*
truncate 清空表的特点:
1. 相当于把表删除以后重建了  (drop和create)
2. 执行速度快
3. 无法回滚
*/

delete from person where name='jack';

-- 复制一个表格
create table people like person;
desc people;
insert into people select * from person;
-- select 语句用来实现查询操作
select * from person;

