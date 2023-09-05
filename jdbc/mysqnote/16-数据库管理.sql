/*
mysql数据库里有一个 user表，用来管理所有的用户
*/
use mysql;
show tables;
desc user;
/*
 Host: 表示用户可以登录的主机
 User: 表示用户名
 xxx_priv: 用户的权限
 plugin: 密码的加密方式
 authentication_string: 用户的密码
*/
-- update user set authentication_string=password('123456') where user='root';  MySQL5.7修改密码，在8.0里password函数被删除了
alter user 'root'@'localhost' identified with MYSQL_NATIVE_PASSWORD by '123456'; -- MySQL8修改密码
create user jack@'%' identified by '123123'; -- 创建了一个用户jack,可以在任意主机登录，密码是123123,没有任何的权限
show privileges; -- 显示所有的可用权限

show grants; -- 查看当前用户的权限
show grants for jack@'localhost';
-- 查看指定用户的权限
-- GRANT USAGE ON *.* TO `jack`@`localhost`  jack用户目前没有权限

-- *.* 表示任意数据库的任意表
GRANT SELECT ON *.* to jack@localhost; -- 给jack用户添加了select权限
grant update on bigdata0409.person to jack@'localhost';
-- 给jack用户添加了bigdata0409数据库person表的修改权限

-- 给 jack@localhost 用户添加了 所有数据库所有表的 insert和drop权限，而且它的权限可以下发
grant insert, drop on *.* to jack@localhost with grant option;

grant all privileges on *.* to jack@'localhost' with grant option;

revoke insert ON *.* FROM jack@'localhost'; -- 回收权限
flush privileges; -- 刷新权限

drop user jack@'localhost'; -- 删除用户


/*
数据库的备份和导入
mysqldump -uroot -p bigdata0409 > 1.sql 把 bigdata0409数据导出到 1.sql文件
mysql -uroot -p bigdata0409 < 1.sql 把 1.sql文件里的内容导入到 bigdata0409数据库

连接到mysql服务器以后，执行 source 命令
*/
