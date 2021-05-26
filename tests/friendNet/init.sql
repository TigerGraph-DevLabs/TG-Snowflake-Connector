create database test_connect;
use database test_connect;
create schema friend;
use schema friend;

create table persons(ID varchar(64));
create table friends(PERSON1 varchar(64), PERSON2 varchar(64));
create table coworkers(PERSON1 varchar(64), PERSON2 varchar(64));

INSERT INTO friend.persons (ID) values ('person1'),
('person2'),
('person3'),
('person4'),
('person5'),
('person6'),
('person7'),
('person8'),
('person9'),
('person10'),
('person11'),
('person12');


INSERT INTO friend.friends (PERSON1,PERSON2) values ('person1','person2'),('person1','person3'),('person1','person4'),('person2','person8'),
('person3','person9'),
('person4','person6'),
('person5','person6'),
('person6','person9'),
('person7','person9'),
('person8','person10'),
('person9','person8'),
('person10','person12'),
('person11','person12'),
('person12','person8'),
('person12','person9');

INSERT INTO friend.coworkers (PERSON1,PERSON2) values ('person1','person4'),
('person1','person5'),
('person1','person6'),
('person2','person3'),
('person2','person4'),
('person3','person5'),
('person3','person6'),
('person4','person5'),
('person4','person6'),
('person5','person6'),
('person7','person9'),
('person7','person5'),
('person7','person4'),
('person8','person9'),
('person9','person2'),
('person10','person7'),
('person11','person7'),
('person12','person7');


#drop database test_connect cascade