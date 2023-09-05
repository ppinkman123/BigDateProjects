use day3;
show create table student;

select name,gender from student where id = 5;

select name  n,gender  g,city c
from student having n = '马云';

select group_concat(name), sum(money) 和, money,avg(money) 平均值, std(money) 标准差, count(id) 总数, city
from student
group by city,money;

select *,group_concat(name)
from student
group by city;

select *
from student,
     score
;

select *from student
union
select *from score;

create table t1(id int,
name varchar(2));

create table t2(cd int,
                name int);

select *from t1
union
select *from t2;

select *from student inner join score;

delete  from score where id=8 or id = 12;
delete from student where id=5 or id=9;

select *from student;
select *from score;

select *
from student
         left join score on student.id=score.id;

select *
from student
        right join score on student.id=score.id;

select * from student left join score on student.id = score.id where score.id is null
union
select *from student right join score on student.id=score.id where student.id is null;

select *
from student
where id in (select id from score where math = (select max(math) from score));

select *
from student
where id in(select id from score where math = (select max(math)from score));

-- 查找数学最高分的学生姓名和性别
select name,gender
from student
where id = (select id from score where math = (select max(math)from score));

select name,gender from student where id =(
select id from score where math=(
select max(math)from score));

select name,gender,math,english
from (select student.*, score.math,english
from student,score where student.id = score.id) as s where math>60 and english>80;


select name ,gender ,math ,english

from score,student where student.id=score.id;



select city,max(math)
from student
         , score where student.id = score.id
 group by city;

CREATE VIEW view_student_score AS
select student.*,
       score.english,
       score.math
from (student
         join score on ((student.id = score.id)));

select city, max(math)
from view_student_score
group by city;


select *
from view_student_score
         join (select city, max(math)as mm
               from view_student_score
               group by city) as ss on view_student_score.city = ss.city  and view_student_score.math = ss.mm;

select datediff('2020-02-05', '2010-10-03');

select *
from student
limit 5,2;

select *
from view_student_score join (
select city,max(math) as mm
from view_student_score
group by city)as ss on view_student_score.city=ss.city and view_student_score.math=ss.mm ;

select distinct gender
from student;

show variables like "%storage_engine%";
show engines ;

set autocommit = false;



begin ;

