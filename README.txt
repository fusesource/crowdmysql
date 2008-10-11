===========================
Crowd MySQL Connector
===========================

build with mvn install

Add the MySQL JDBC driver and the Crowd MySQL Connector to $CROWD/crowd-webapp/WEB-INF/lib

Copy applicationContext-mysqlConnector.xml $CROWD/crowd-webapp/WEB-INF/classes
Edit the connection settings in applicationContext-mysqlConnector.xml

Edit $CROWD/crowd-webapp/WEB-INF/web.xml and add applicationContext-mysqlConnector.xml to the contextConfigLocation

<context-param>
    <param-name>contextConfigLocation</param-name>
    <param-value>
        classpath:/applicationContext-CrowdEncryption.xml,
        ...
        classpath:/applicationContext-mysqlConnector.xml
    </param-value>
</context-param>

Start Crowd
Add a new Directory
com.chariotsolutions.crowd.connector.MySQLConnector
