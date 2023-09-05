/*
事务的特征:
    原子性: 同一个事务的多个操作，要么都执行成功，要么都执行失败，不可能在中间停止
    一致性: 在事务开始之前和事务结束以后, 数据库的完整性没有被破坏。
    隔离性: 多个客户端同时操作同一个数据库可能产生的问题，考虑的是并发的问题。
    持久性: 事务的代码一旦提交，数据就被写入到数据库了
*/
use bigdata0409;
insert into person
values (0, 'zhangsan', 18, '上海'); -- 每一次执行SQL语句都是一个单独的事务，并自动提交

set autocommit = false; -- 关闭 MySQL的自动提交，开启事务的前提
begin; -- 等价于 start transaction ;

insert into person values (0, 'jack', 19, '北京');
update person set name='jerry' where id = 1;

commit;  -- 提交数据,将 insert 和 update 两条命令同时提交到服务器
rollback ;  -- 回滚数据, insert 和 update两条命令都撤回
set autocommit = true;  -- 事务全都处理完成以后，再将自动提交打开

/*
脏读: 一个事务T1 读取到了另一个事务T2 更新但是还没有提交的数据,此时如果 T2回滚了数据,T1读取的结果有问题。
不可重复读: 一个事务T1 读取到了另一个事务 T2提交以后的数据。
    保证一个情况:在同一个事务里，多次读取到的结果是一样的!
幻读: 一个事务 T1读取到另一个事务 T2插入的数据

事务的隔离级别:
*/