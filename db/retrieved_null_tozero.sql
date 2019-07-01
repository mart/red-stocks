update "comments" set retrieved_on='0' where retrieved_on is null;

update "posts" set retrieved_on='0' where retrieved_on is null;