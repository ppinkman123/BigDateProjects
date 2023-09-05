use day04_test01_bookstore;
create database day03_test05_tour;
use day03_test05_tour;
create table agency(
    id int primary key auto_increment,
    name varchar(32) not null ,
    Address varchar(32) not null ,
    Areaid int


);

create table travel(
    Tid int primary key auto_increment,
    Time varchar(21) not null ,
    Position varchar(32) not null ,
    Money float,
    Aid int not null ,
    Count int
    #constraint Aid_key foreign key (Aid) references agency(id)
);

insert into agency (id, name, Address, Areaid) VALUES (101,'青天旅行社','北京海淀',null),
                                                      (102,'天天旅行社','天津海院',null);

insert into travel(Tid, Time, Position, Money, Aid, Count) VALUES
(1,'5天','八达岭',3000,101,10),
(2,'7天','水长城',5000,101,14),
(3,'5天','水长城',6000,102,11);
select Aid ,count(*) as c from travel group by Aid;

select *from agency;
select *from travel;

select * from travel where Count = (select max(Count)from travel);
select * from travel where Money <6000;
select sum(Time) from travel where Aid = (select id from agency where name = '青天旅行社');
SELECT *
FROM agency INNER JOIN
     (SELECT t.aid,MAX(t.c) FROM (SELECT Aid,COUNT(*) AS c FROM travel GROUP BY Aid)  AS t)temp
     ON agency.id = temp.aid;


select * from agency join
(select Aid ,count(*) as c from travel group by Aid)as t on id = t.Aid;


SELECT t.aid,MAX(t.c) FROM (SELECT Aid,COUNT(*) AS c FROM travel GROUP BY Aid)  AS t;


select * from agency join (
select t.aid , max(t.c) from
(select Aid ,count(*) as c from travel group by Aid) as t) as tem on id = tem.Aid  ;



select * from agency join
(select t.aid ,max(t.c) from
(select aid,count(*) as c from travel group by aid) as t) as temp on id= temp.Aid ;



CREATE DATABASE day03_test04_library;
USE day03_test04_library;

CREATE TABLE press(
                      pressid INT(10) PRIMARY KEY,
                      pressname VARCHAR(30),
                      address VARCHAR(50)
);
CREATE TABLE sort(
                     sortno INT(10) PRIMARY KEY,
                     scount INT(10)
);
CREATE TABLE book(
                     bid INT(10) PRIMARY KEY,
                     bname VARCHAR(40),
                     bsortno INT(10),
                     pressid INT(10),
                     CONSTRAINT p_b_pid_fk FOREIGN KEY (pressid) REFERENCES press(pressid),
                     CONSTRAINT s_b_sno_fk FOREIGN KEY (bsortno) REFERENCES sort(sortno)
);

ALTER TABLE sort ADD COLUMN describes VARCHAR(30);

INSERT INTO press VALUES(100,'外研社','上海');
INSERT INTO press VALUES(101,'北大出版社','北京');
INSERT INTO press VALUES(102,'教育出版社','北京');

INSERT INTO sort(sortno,scount,describes)VALUES(11,50,'小说');
INSERT INTO sort(sortno,scount,describes)VALUES(12,300,'科幻');
INSERT INTO sort(sortno,scount,describes)VALUES(13,100,'神话');

INSERT INTO book VALUES(1,'红与黑',11,100);
INSERT INTO book VALUES(2,'幻城',12,102);
INSERT INTO book VALUES(3,'希腊神话',13,102);
INSERT INTO book VALUES(4,'一千零一夜',13,102);

#第一题
SELECT * FROM book WHERE pressid=100;

select * from sort join (
select * from book where pressid=100) as b on sortno = bsortno;
#第二题
select * from book where pressid = (select pressid from press where pressname = '外研社');
#第三题
select * from sort where scount>100;

#8.查询图书种类最多的出版社信息
SELECT * FROM press WHERE pressid=(
    SELECT pressid
    FROM (SELECT pressid,bsortno FROM book GROUP BY pressid,bsortno) temp
    GROUP BY pressid
    ORDER BY COUNT(*) DESC
    LIMIT 0,1);

select * from press where pressid = (select pressid  from
    (select pressid ,count(*) as c from book group by pressid) as temp group by pressid order by max(c) desc limit 0,1);

select pressid  from
(select pressid ,count(*) as c from book group by pressid) as temp group by pressid order by count(c) desc limit 0,1;

SELECT * FROM press WHERE pressid=(
    SELECT pressid
    FROM (SELECT pressid,bsortno FROM book GROUP BY pressid,bsortno) temp
    GROUP BY pressid
    ORDER BY COUNT(*) DESC
    LIMIT 0,1)

#第三题
CREATE DATABASE day03_test03_xuankedb;
-- 使用数据库
USE day03_test03_xuankedb;

-- 创建学生表
CREATE TABLE student(
                        sno INT(10) PRIMARY KEY,
                        sname VARCHAR(10),
                        ssex VARCHAR(10),
                        sage INT(10),
                        sdept VARCHAR(40)
);

-- 创建课程表
CREATE TABLE course(
                       cno INT(10) PRIMARY KEY,
                       cname VARCHAR(20),
                       cpno VARCHAR(40),
                       ccredit INT(20)
);

-- 创建成绩表
CREATE TABLE sg(
                   sno INT(10),
                   cno INT(10),
                   grade INT(3),
                   PRIMARY KEY(sno,cno),
                   CONSTRAINT stu_s_sno_fk FOREIGN KEY (sno) REFERENCES student(sno),
                   CONSTRAINT cou_s_sno_fk FOREIGN KEY (cno) REFERENCES course(cno)
);

SELECT MAX(grade),AVG(grade ) FROM sg WHERE cno=1;