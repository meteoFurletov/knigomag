
### 1. Install ClickHouse: Add the ClickHouse repository and install the server and client

``` bash
sudo apt update
sudo apt install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt update
sudo apt install -y clickhouse-server clickhouse-client
```

### 2. Start and Enable ClickHouse

``` bash
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server
```

### 3. Edit the Configuration File to Allow Connections from All IPs
``` bash
sudo nano /etc/clickhouse-server/config.xml
```
And Uncomment this section:
`<listen_host>::</listen_host>`

### 4. (Optional) Enable User Authentication
``` xml
<users>
    <default>
        <password_sha256_hex>your_hashed_password</password_sha256_hex>
        <networks>
            <ip>::/0</ip>
        </networks>
    </default>
</users>
```

### 5. Restart ClickHouse
``` bash
sudo systemctl restart clickhouse-server
```

### 6. Check Firewall Settings
``` bash
sudo ufw allow 9000/tcp
```