to start airflow:
astro dev start

What bot needs is:
- trigger data loader
- trigger salary 
- get data to show

How to solve:
- create dag to compute salary
- create dag for backup the drive

PostgreSQL schema:
![alt text](image.png)

timeline:
- [x] 13.01 + create postgres scheme 
- [x] 14.01 + setup postgres
- [x] 14.01 + setup clickhouse
- [x] 17.01 + setup the connectors
- [x] 20.01 + create etl from drive to postgres
- [x] 20.01 + create etl from drive to postgres - extract
- [x] 20.01 + create etl from drive to postgres - transform
- [x] 20.01 + create etl from drive to postgres - load