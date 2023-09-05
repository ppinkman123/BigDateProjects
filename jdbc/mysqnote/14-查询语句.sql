/*
order by:  按照指定的顺序对数据进行排序,默认是升序asc排序
    还可以设置为 降序排序 desc
    还可以依据多个条件进行排序
limit: 用来限制取值的个数，配合 offset语句实现分页功能
*/
select *
from view_student_score
order by english, math desc; -- 先按照英语升序排序，如果英语值一样，再按照数学降序排序


select *
from student
limit 2 offset 5; -- limit 表示取值个数 offset表示偏移个数,先指定limit,再指定offset

select *
from student
limit 5,2;
-- 可以省略成为 limit m,n  m表示偏移的个数,n表示取值的个数

/*
通过  limit 和 offset 语句，可以实现 分页的功能
每一页显示  number 个数据，使用 page 表示页码(从1开始)，查询第 page 页的数据的SQL语句
select * from student limit (page-1)*number,number;
*/

select city, avg(money) am, avg(math) aa, avg(english) ae
from (
         select student.*, score.math, english
         from student
                  join score on student.id = score.id
         where student.id > 5) as ss
group by city
having am > 10
order by ae
limit 0,2;

/*
SQL 语句的书写顺序:
select ... from ...join ...on... where ... group by... having..order by...limit...offset

SQL 语句的执行顺序:
from > on > join > where > group by > having > select > distinct > order by > limit
*/

/*
 distinct 用来去重
 */
select distinct gender
from student;

select count(distinct city)
from student;