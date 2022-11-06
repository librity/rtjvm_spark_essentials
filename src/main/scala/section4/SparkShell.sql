-- Lesson 4.1

-- Connect and run Spark SQL
-- $ docker exec -it spark-cluster-spark-master-1 bash
-- $ cd spark/
-- $ ./bin/spark-sql

-- Create a database
show databases;
create database rtjvm;
show databases;
use rtjvm;


-- Create a managed table
create table persons(id integer, name string);
select * from persons;
insert into persons values (1, "Paul Denton"), (2, "JC Denton");
select * from persons;


-- Managed Table: Spark is in charged of the table's data and metadata.
describe extended persons;
-- Type                    MANAGED
-- Dropping the table completely deletes it.
drop table persons
-- Database and table are stored at /spark/spark-warehouse/*.db.


-- Creating an external (unmanaged) csv table:
create table flights(origin string, destination string)
    using csv options(header true, path "/home/rtjvm/data/flights");
insert into flights values ("Los Angeles", "New York"), ("London", "Prague");
select * from flights;
describe extended flights;
-- Type                    EXTERNAL
drop table flights;
-- Dropping the deletes the Spark metadata but doesn't delete the files.
