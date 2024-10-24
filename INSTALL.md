gcc python3.9-dev python3.9-venv libmemcached-dev zlib1g-dev build-essential libffi-dev unixodbc unixodbc-dev libsqliteodbc libev4 libev-dev
sudo apt install python3-cassandra
apt install pkg-config
# For MySQL is required:
sudo apt-get install python3-dev default-libmysqlclient-dev build-essential libmariadb-dev libmysqlclient-dev
###  For JDBC is required:
sudo apt install openjdk-17-jre java-package
# adding the keys certs
export JAVA_HOME=/usr/lib/jvm/default-java
keytool -list -trustcacerts -keystore "$JAVA_HOME/lib/security/cacerts"
# For Oracle:
sudo apt-get install build-essential unzip python-dev libaio1 libaio-dev
# download client:
https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
# export InstantClient
mkdir /opt/oracle
# unzip instanclient in folder.
sudo sh -c "echo /opt/oracle/instantclient_19_16 > /etc/ld.so.conf.d/oracle-instantclient.conf"
sudo ldconfig
# For Cassandra:
sudo apt-get install libev4 libev-dev
