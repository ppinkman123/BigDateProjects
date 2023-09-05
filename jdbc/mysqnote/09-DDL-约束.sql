/*
约束就是对字段添加一些规定，对数据业务规则和数据完整性进行实施、维护。
主键约束 / 自增约束 / 唯一约束 / 外键约束 / 非空约束 / 默认值约束 / 检查约束
*/

create database bigdata0409;
use bigdata0409;

/*
主键约束，自带 非空，唯一约束，还会自动添加索引,通常使用int类型的值作为主键，并设置值为自增
一个表最多只能有一个主键一个自增约束，而且自增约束只能用在主键上
*/
create table t1
(
    id     int primary key auto_increment, -- 增加了主键自增约束
    name   varchar(32),
    city   varchar(32),
    gender enum ('男','女','保密'),
    age    int
);

create table t2
(
    id   int,
    name varchar(32)
);

-- 创建表格时，没有指定主键，后续再通过修改表新增主键和自增约束
alter table t2
    add primary key (id);
alter table t2
    modify id int auto_increment;

-- 联合主键
create table t3
(
    -- id int primary key auto_increment  更推荐使用这种方式，简单
    id   int auto_increment,
    name varchar(32),
    primary key (id, name) -- 选择两个字段作为联合主键,使用联合主键，只能使用这种方式
);
create table t3
(
    id   int,
    name varchar(32)
);
alter table t3
    add primary key (id, name);

alter table t3
    modify id int; -- 删除自增约束
alter table t3
    drop primary key;
-- 删除主键约束

/*
唯一约束 unique 被唯一约束的字段，值不能重复
1. 被唯一约束的字段，MySQL会自动添加一个索引
2. 一个表里可以有多个 唯一约束
3. 值可以是 null
4. 还可以使用多个列联合唯一约束

唯一约束和主键的区别:  主键肯定是唯一约束，但是唯一约束不是主键。
一个表可以有多唯一约束，但是最多只能有一个主键;
如果主键没有添加自增约束，唯一约束的值也可以添加自增约束
*/
create table t4
(
    id    int primary key auto_increment,
    name  varchar(32) unique,
    phone varchar(32) unique
);
create table t5
(
    id    int primary key auto_increment,
    name  varchar(32),
    phone varchar(32),
    unique key (name, phone) -- 两个字段作为联合唯一约束
);

alter table t5
    drop index name; -- 如果是多个键组成的联合唯一约束，默认使用第一个字段名作为索引值名

create table t6
(
    id   int primary key auto_increment,
    name varchar(32)
);
alter table t6
    add unique (name); -- 追加唯一约束,约束名字默认是 字段名

alter table t6
    add constraint xxx unique (name); -- 添加唯一约束，同时给约束起名为 xxx
alter table t6
    drop index xxx;
-- 删除唯一约束，依据并不一定是字段名

/*
外键用来保证参照的完整性:
语法结构:
    foreign key(从表字段名) reference 主表名(主表字段);
外键约束建立失败的原因:
1. 从表字段和主表字段类型不一致!
2. 主表被引用的字段没有唯一约束
外键对数据库的性能有影响，要慎用!

外键约束还能设置约束等级:
    on delete / update 当主表的数据发生删除或者修改时，从表的应该做和反映
    on delete/update cascade 当主表的数据 删除(修改) 时，从表的数据也删除(修改)
    on delete/update set null  当主表删除(修改)时，设置为 null
*/
create table person
(
    person_id   int primary key auto_increment,
    person_name varchar(32),
    age         int,
    home_id     int
    # home        varchar(32)  开发中通常是直接使用varchar表示，不使用外键
    # foreign key (home_id) references province (province_id) on update cascade  # 创建表格时就执行外键约束
);

# 给已有的表格追加外键约束,并起一个外键名
alter table person
    add /*constraint xxx*/ foreign key (home_id) references province (province_id);


create table province
(
    province_id   int primary key auto_increment,
    province_name varchar(16),
    capital       varchar(16)
);

alter table person
    drop foreign key person_ibfk_1;

-- 如果没有给外键约束命名，通过下面的命令查看外键约束的名字
select *
from information_schema.TABLE_CONSTRAINTS
where TABLE_NAME = 'person';
/*
删除主键:  alter table person drop primary key;  一个表最多只能有一个主键，直接删除就行
删除唯一约束: alter table person drop index 唯一约束的名;  如果不给唯一约束起名字，默认就是字段名

删除外键约束: alter table person drop foreign key 外键约束名;
*/

create table t8
(
    id   int primary key auto_increment,
    name varchar(32) not null,
    age  int         not null default 10,
    x    boolean
);
-- 给表格追加非空约束
alter table t8
    modify x boolean not null;

alter table t8
    modify x boolean;

-- check约束: 用来对数据的合法性进行校验
create table student
(
    id    int primary key auto_increment,
    name  varchar(32),
    age   int,
    score int check ( score >= 0 and score <= 100 )
);