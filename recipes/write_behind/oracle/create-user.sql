
create user test identified by passwd;
grant connect,resource to test;
alter user test quota unlimited on users;
