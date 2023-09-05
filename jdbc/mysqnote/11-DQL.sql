/*
DataQueryLanguage  数据查询语言，用来查询数据  select 语句
*/
use bigdata0409;

/*
select 字段1,字段2... from 数据源 [where 判断条件];
select * from 数据源 [where 判断条件];
数据源: 可以是一个表格，也可以是多个表格，还可以是一个查询结果(子查询)
from 这个语句在 MySQL里，某些情况下可以省略.但是在某些SQL类型里，from语句不能省略，此时可以考虑使用 dual虚拟表。

select 语句后面的子句:
    1. from: 指定数据源，可以是一个表，也可以是多个表，还可以是一个查询语句
    2. on: 多表查询
    3. where: 从表中筛选数据
    4. having: 从表中筛选数据。
    5. group by:对数据进行分组。
    6. order by:排序
    7. limit:分页
    8. union:合并多个表格
*/
select *
from student
where id = 5;

/*
where 和 having 的区别:
    都能使用:
        如果判断条件的字段在查询结果集里，可以使用where,也能使用having.推荐使用where

    只能使用where不能使用having的场景:
        如果判断条件的字段不再查询的结果集里，只能使用 where不能使用 having

    只能使用having不能使用where的场景:
        1. 判断条件里使用到了查询结果集字段别名，此时只能使用 having,不能使用where
        2. 如果使用到了分组语句 group by，只能使用 having不能使用where.

别名:  可以给字段，表，甚至 查询结果起一个别名
    别名可以使用 双引号或者单引号包裹，也可以不写引号。如果别名里有空格(不推荐)，引号不能省略
    as 也可以省略
*/

-- where和having都能使用的场景
select *
from student
where id = 5;
select *
from student
having id = 5;

-- 只能使用 where 不能使用having的场景: 判断条件字段如果不在查询的结果集里，只能使用where,不能使用having
select name, gender, birthday
from student
where id = 5;
-- select name,gender,birthday from student having id = 1;

-- 只能使用 having不能使用where的场景
-- 如果判断条件里使用到了别名，此时判断条件不能使用where,只能使用having
-- select name as "n",gender as "g",birthday as "b" from student where n='郭德纲';
select name as "n", gender as 'g', birthday as b, only_child as "o c"
from student
having n = '郭德纲';
select name n, gender g, birthday b
from student;

-- 使用了分组语句以后，只能使用having不能使用where
# select city from student group by city where city='北京';
select city, count(id) ci
from student
group by city
having ci > 3;

-- group by分组语句通常都需要配合聚合函数来使用
-- count 用来计数，注意事项:建议使用非空字段计数，推荐使用主键
select group_concat(name), sum(money) 和, avg(money) 平均值, std(money) 标准差, count(id) 总数, city
from student
group by city;

select group_concat(name), sum(money) 和, avg(money) 平均值, std(money) 标准差, count(id) 总数, city, gender
from student
group by city, gender;

create database day529;
use day529;
create table nig(id int ,name int,sc int);
insert nig(id, name, sc) values (1,2,3),
                                (1,2,3),
                                (4,5,6);
select name,count(name) from nig group by id,sc;

-- MySQL里的流程控制语句
select if(3 < 2, 'hello', 'good'); -- 相当于java里的三元表达式
select ifnull(null, 'ok'); -- 如果第一个值不是null,就取第一个值；否则就去第2个

select id,
       english,
       case
           when english < 60 and english >= 0 then '不及格'
           when english >= 60 and english < 80 then '及格'
           when english >= 80 and english <= 100 then '优秀'
           else '错误'
           end english_level
from score; -- 相当于java里的if...else if...else 语句

select name,
       gender,
       case gender
           when '男' then 'male'
           when '女' then 'female'
           else 'other'
           end sex
from student; -- 相当于 java里的switch...case语句

select 3 = null;
select 3 =1;
select null is  null ;