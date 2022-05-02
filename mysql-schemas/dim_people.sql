drop table if exists dim_people;

create table `dim_people` (
  `w_id` int not null auto_increment,
  `name` nvarchar(100) not null,
  `age` int default null,
  `city` nvarchar(100) default null,
  `Interest1` nvarchar(100) default null,
  `Interest2` nvarchar(100) default null,
  `Interest3` nvarchar(100) default null,
  `Interest4` nvarchar(100) default null,
  `Phone_Country_Code` nvarchar(15) default null,
  `Full_Phone_Number` nvarchar(15) default null,  
  `Phone_Extension` nvarchar(10) default null,
  `Phone_Area_Code` nvarchar(3) default null,
  `Phone_Exchange_Code` nvarchar(3) default null,
  `Phone_Subscriber_Number` nvarchar(4) default null,
  primary key (`w_id`)
);
