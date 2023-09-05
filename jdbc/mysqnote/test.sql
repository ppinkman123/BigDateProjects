create database day03;

use day3;
create table `student` (
                           `id` int unsigned primary key auto_increment,
                           `name` char(32) not null unique,
                           `gender` enum('男', '女') not null,
                           `city` char(32) not null,
                           `description` text,
                           `birthday` date not null default '1995-1-1',
                           `money` float(7, 2) default 0,
                           `only_child` boolean
);

insert into `student`
(`name`, `gender`, `city`, `description`, `birthday`, `money`, `only_child`)
values
('郭德纲', '男', '北京', '班长', '1997/10/1', rand() * 100, True),
('陈乔恩', '女', '上海', NULL, '1995/3/2', rand() * 100, True),
('赵丽颖', '女', '北京', '班花, 不骄傲', '1995/4/4', rand() * 100, False),
('王宝强', '男', '重庆', '超爱吃火锅', '1998/10/5', rand() * 100, False),
('赵雅芝', '女', '重庆', '全宇宙三好学生', '1996/7/9', rand() * 100, True),
('张学友', '男', '上海', '奥林匹克总冠军！', '1993/5/2', rand() * 100, False),
('陈意涵', '女', '上海', NULL, '1994/8/30', rand() * 100, True),
('赵本山', '男', '南京', '副班长', '1995/6/1',  rand() * 100, True),
('张柏芝', '女', '上海', NULL, '1997/2/28', rand() * 100, False),
('吴亦凡', '男', '南京', '大碗宽面要不要？', '1995/6/1',  rand() * 100, True),
('鹿晗', '男', '北京', NULL, '1993/5/28', rand() * 100, True),
('关晓彤', '女', '北京', NULL, '1995/7/12',  rand() * 100, True),
('周杰伦', '男', '台北', '小伙人才啊', '1998/3/28', rand() * 100, False),
('马云', '男', '南京', '一个字：贼有钱', '1990/4/1',  rand() * 100, False),
('马化腾', '男', '上海', '马云死对头', '1990/11/28', rand() * 100, False);
