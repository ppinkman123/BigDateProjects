/*
有多个表联合查询:
1. from后面添加多个数据源
2. 使用 union 语句实现两个表格的拼接
3. 使用 join...on 语句进行多表查询(重点)
*/

-- from后面可以有多个数据源,如果不添加 where判断条件，是一个笛卡尔积，相当于内连接
select *
from student,
     score
where student.id = score.id;

/*
union要求两个表格的字段数是一样的!
字段名和字段类型不一致也可以拼接，拼接以后结果的字段名是前一个表的字段名，拼接以后的类型都是字符串类型
如果两个表里存在相同的数据，只会显示一次
*/
select *
from user1
union
select *
from user2;

-- select * from student join score; 不添加判断条件，得到的结果依然是一个笛卡尔积，等价于 select * from student,score;

-- join 等价于 inner join,关键字 inner 可以省略，注意使用 on 添加判断条件
-- inner join 是内连接，取两个表共有的部分
select *
from student
         inner join score on student.id = score.id;
-- 等价于 select * from student,score where student.id=score.id;

-- left outer join,关键字 outer可以省略，查询左右表共有的，以及左表特有，右表没有的是 null
select *
from student
         left join score on student.id = score.id;

-- right outer join,关键字 outer可以省略，查询左右表共有的，以及右表特有，左表没有的是 null
select *
from student
         right join score on student.id = score.id;

-- mySQL不支持全外连接，可以使用  左外连接 union 右外连接  实现
select * from student left join score on student.id = score.id -- 左外连接
union
select * from student right join score on student.id = score.id; -- 右外连接

-- 查找到 左表独有的数据
select * from student left join score on student.id = score.id where score.id is null;

-- 查找到 右表特有的数据
select * from student right join score on student.id = score.id where student.id is null;

-- 查询左表和右表 特有的数据
select * from student left join score on student.id = score.id where score.id is null
union
select * from student right join score on student.id = score.id where student.id is null;