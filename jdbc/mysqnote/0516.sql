select max(math) from score;
select id from score where math=(select max(math) from score);
select name,gender from student where id =
            (select id from score where math=(select max(math) from score)); -- 查找数学最高分的学生姓名和性别
