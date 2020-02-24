
create user test identified by passwd;
grant connect,resource to test;
alter user test quota unlimited on users;
create profile umlimited_attempts limit failed_login_attempts unlimited;
alter user test profile umlimited_attempts;
