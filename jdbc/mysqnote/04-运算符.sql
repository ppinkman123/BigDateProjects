/*
select 语句:
    1. 作为查询语句，可以在数据库里查询某一个字段
    2. 用来在MySQL里打印内容
select 语句后面通常会有一个 from 语句
dual 表示一个不存在的虚拟表，为了保证 select语句的完整性
*/

-- 算数运算符:  +  -  *  /  %
select 1 + 1, 5 - 2, 3 * 5, 10 / 3, 15 % 2
from dual;


/*
比较运算符:
    MySQL里的=是比较运算符
    !=和<> 都表示不等于
    2 between 1 and 5 判断2是否在[1,5]之间
    3 in (5,6,7,3,9) 判断3是否在(5,6,7,3,9)里
    like 使用 % 通配符来匹配
    rlike 使用 正则表达式 来匹配
 */
select 1 = 1,
       5 = 3,
       3 != 5,
       5 <> 5,
       5 > 3,
       6 >= 2,
       7 < 8,
       8 <= 8,
       3 = null,
       3 != null,
       3 > null,
       3 <=> null,          -- <=> 专门用来和 null 比较
       3 is null,
       3 is not null,
       2 between 1 and 5,
       3 in (4, 2, 6, 8),
       'hello' like '%ll%', -- 通配符匹配
       'goood' rlike 'go{0,2}d'
from dual;


/*
 逻辑运算符:
    单个的逻辑运算符 如果运算的是数字，把 运算符当做位运算符来计算
    两个逻辑运算符，无论运算的是数字还是布尔值,都是把 双逻辑运算符 当做逻辑运算

 true && true --> true
 0 当做 false, 非 0 都是true
 */
select true and true, -4 && 0, 34 or 45, 0 || 0, ! true, true xor false,false xor false
from dual;

show databases