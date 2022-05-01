drop table if exists dim_people_stg;

create table `dim_people_stg` (
  w_id int not null auto_increment,
  `name` nvarchar(100) default null,
  `age` int default null,
  `city` nvarchar(100) default null,
  `Interest1` nvarchar(100) default null,
  `Interest2` nvarchar(100) default null,
  `Interest3` nvarchar(100) default null,
  `Interest4` nvarchar(100) default null,
  `PhoneNumber` nvarchar(100) default null,
  `w_create_timestamp` timestamp,
  `w_update_timestamp` timestamp,
  primary key (`w_id`)
);


drop table if exists dim_people;

create table `dim_people` (
  w_id int not null auto_increment,
  `name` nvarchar(100) default null,
  `age` int default null,
  `city` nvarchar(100) default null,
  `Interest1` nvarchar(100) default null,
  `Interest2` nvarchar(100) default null,
  `Interest3` nvarchar(100) default null,
  `Interest4` nvarchar(100) default null,
  `PhoneNumber` nvarchar(100) default null,
  `w_create_timestamp` timestamp,
  `w_update_timestamp` timestamp,
  primary key (`w_id`)
);
