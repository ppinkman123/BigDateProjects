use bigdata0409;
-- 切换到BigData0409这个数据库

/*MySQL里的数据类型:
 1. 整数型
    tinyint: 占用一个字节，取值范围是  -128 ~ 127   byte
        tinyint unsigned: 取值范围是 [0,255]
        在开发中，如果有一个age字段，age字段最适合使用的类型应该是  tinyint unsigned
    smallint: 占用两个字节，取值范围是 -2的15次方 ~ 2的15次方 - 1  short
    mediumint: 占用三个字节
    int/integer: 占用4个字节   int
    bigint: 占用8个字节        long
整数类型后面如果添加了 unsigned 关键字，表示它是无符号数,所有的无符号数都是整数
zerofill 如果数字的位数不够，前面使用 0 来填充(不建议使用)

2. 小数型(浮点型和定点型)
 float 和 double
 decimal(M,N) 数字的总长度是 M 位，精确到小数点后 N位，整数部分最多是  M-N 位

 3. 字符串类型
    char(n)  / varchar(n) 更加推荐 varchar
    无论是 char还是varchar,n表示的都是字符的个数，而不是字节个数
    区别:
        最大值不同:
             char(n) n的最大取值是 255
             varchar(n) n的最大取值和数据库的编码有关。
                Latin1 编码，不支持中文，一个字占用一个字节  n的最大取值到 65532(留下3个字符记录长度)
                UTF8编码，支持中文，一个字最多占用三个字节  n的最大取值到 21844(留下1个字符记录长度)
                UTF8mb4编码，是 SQL8默认编码方式，一个字最多占用是个字节  16128
        数据库内存存储的长度不同：
            char(n)类型里，无论输入的内容是什么，存储的长度都是 n个字符。例如，数据类型char(10)，写入字符串'张三'，在数据库里存入的数据长度是10个字符。
            varchar(n)类型里，存储的长度是不固定可变的。
       对于空格的处理不同：
            char类型在存储字符串时，会将右侧末尾的空格去掉。
            varchar类型在存储字符串时，会将右侧末尾的空格保留
    MySQL里的字符串使用 单引号包裹
 4. 枚举类型: 多个选取一个 的场景
    enum('男','女','保密') 添加数据时，可以写枚举值，也可以写枚举编号，编号从1开始
 5. 集合类型: 多选的场景
    set('唱','跳','RAP','篮球')
 6. 日期时间类:
    year / date / time /datetime / timestamp
    都可以使用字符串直接填充数值
 7. 布尔类型的值 true 和 false
    在MySQL里没有布尔值，布尔值就是  tinyint类型
    1 用来表示true;  0 用来表示false
 */
create table t1
(
    x  tinyint,
    x1 tinyint unsigned,
    y  smallint,
    y1 smallint unsigned,
    z  mediumint,
    z1 mediumint unsigned,
    a  integer(10) zerofill,
    a1 int unsigned,
    b  bigint,
    b1 bigint unsigned
); -- 在BigData0409这个数据库里创建了一个表格t1

create table t2
(
    x float,
    y double,
    z decimal(5, 2) -- z这个字段数字的总长度是5,小数点后保留2位
);

create table t3
(
    x char(255),
    y varchar(16128)
);
create table t4
(
    gender enum ('男','女','保密')
);
create table t5
(
    hobbies set ('唱','跳','RAP','篮球')
);
use `0409`;
create table t6
(
    x date,
    y year,
    z time,
    a datetime,
    b timestamp
);
insert into t6
values ('2020-03-09', '2020', '89:34:56', '2022-5-13 15:43:00', '1998-10-23 12:45:13');
use demo;

create table t7
(
    x bool,
    y boolean
);
insert into t7
values (5, 7);

select x
from t7;

create table person
(
    id       int,
    name     varchar(32),
    age      tinyint unsigned,
    height   double,
    gender   enum ('男','女','保密'),
    hobbies  set ('唱','跳','吃','喝','睡'),
    birthday datetime,
    isPass   boolean
);
