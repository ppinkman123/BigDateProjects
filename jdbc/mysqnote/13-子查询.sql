/*
在查询语句里再嵌套一个查询语句  子查询都必须要添加 ()

查找到数学最高分学生的信息
where 后面的判断条件来源一个子查询
*/
select *
from student
where id in (select id from score where math = (select max(math) from score));

select *
from student
         join score on student.id = score.id
where student.id in (
    select id
    from score
    where math = (select max(math) from score));

/*
from 的数据源来自于一个查询结果,from后面的子查询，必须要起一个别名
查找学生数学成绩大于80的学生信息
*/
select *
from student
         join score on student.id = score.id
where math > 80;


select *
from (select student.*, score.math, score.english
      from student
               join score on student.id = score.id) ss
where math > 80;

/*
    视图表：把查询结果保存到一个view视图表格里,可以使用 show  tables查看表格以及视图表
    语法:  create view <viewName> as 查询语句
    视图表依赖于一个查询结果，如果数据源发生了改变，视图表里的数据也会跟着改变
    显示视图表的建表语句: show create view <viewName>
    删除视图表: drop view <viewName>
*/
CREATE VIEW view_student_score AS
select student.*,
       score.english,
       score.math
from (student
         join score on ((student.id = score.id)));

drop view view_student_score;

/*
查询每个城市里，数学的最高分学生信息
*/
select city, max(math)
from student
         join score on student.id = score.id
group by city;
-- 获取到每个城市的最高分

-- 获取到每个城市的最高分信息
select city, max(math)
from view_student_score
group by city;

select *
from view_student_score
         join (select city, max(math) as mm
               from view_student_score
               group by city) as ss on view_student_score.city = ss.city and view_student_score.math = ss.mm;

select gender, round(avg(english), 2) as avg_english, round(avg(math), 2) as avg_math
from view_student_score
group by gender;

select max(birthday)
from student;
select min(birthday)
from student;
select datediff('2020-02-05', '2010-10-03');

select datediff(max(birthday), min(birthday))
from student;