## Installation
### 1. Install PostgreSQL

``` bash
sudo apt update
sudo apt install postgresql postgresql-contrib -y
```

### 2. Configure PostgreSQL

``` bash
sudo -i -u postgres psql
```

#### Set up the database and user

``` sql
CREATE DATABASE knigomag;
CREATE USER postgres WITH PASSWORD 'password';
ALTER USER postgres WITH SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE knigomag TO postgres;
```

### 4. Edit PostgreSQL configuration
``` bash
sudo nano /etc/postgresql/16/main/postgresql.conf
```
And Uncomment this section:
`listen_addresses = '*'`

``` bash
sudo nano /etc/postgresql/16/main/pg_hba.conf
```

And add this section to the end:
`host    all             all             0.0.0.0/0               md5`

### 5. Restart PostgreSQL
``` bash
sudo systemctl restart postgresql

```

### 6. Check Firewall Settings
``` bash
sudo ufw allow 5432/tcp
``` 

## Setup the tables
### 1. Enter the psql
``` bash
sudo -i -u postgres
psql -U postgres -d knigomag
```

### 2. Run the sql/pg_init.sql
