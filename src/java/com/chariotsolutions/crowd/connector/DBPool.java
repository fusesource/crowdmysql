package com.chariotsolutions.crowd.connector;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.NoSuchElementException;
import java.net.URL;
import java.io.InputStream;
import java.io.IOException;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Helper to make DB connections to MySQL
 */
public class DBPool {
    private final static Logger log = Logger.getLogger(DBPool.class);
    private GenericObjectPool pool;
    private PoolableObjectFactory factory;


    public DBPool() {
        // Load JDBC Settings
        URL url = getClass().getResource("/database.properties");
        if(url == null) {
            throw new IllegalStateException("Unable to find database configuration on CLASSPATH");
        }
        Properties props;
        try {
            InputStream in = url.openStream();
            props = new Properties();
            props.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read database configuration", e);
        }
        final String jdbcURL = props.getProperty("jdbc.url");
        String jdbcUser = props.getProperty("jdbc.user");
        String jdbcPassword = props.getProperty("jdbc.password");
        String jdbcDriver = props.getProperty("jdbc.driver");
        final Driver driver;
        try {
            driver = (Driver)Class.forName(jdbcDriver).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load JDBC driver '"+jdbcDriver+"'", e);
        }
        final Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", jdbcUser);
        jdbcProps.setProperty("password", jdbcPassword);


        // Create the object factory
        factory = new PoolableObjectFactory() {
            public Object makeObject() throws Exception {
                log.debug("Creating new DB connection");
                return driver.connect(jdbcURL, jdbcProps);
            }

            public void destroyObject(Object object) throws Exception {
                Connection con = (Connection) object;
                log.debug("Closing DB connection");
                con.close();
            }

            public boolean validateObject(Object object) {
                Connection con = (Connection) object;
                try {
                    PreparedStatement ps = con.prepareStatement("select count(*) from user where 0=1");
                    ResultSet rs = ps.executeQuery();
                    rs.next();
                    rs.close();
                    ps.close();
                } catch (SQLException e) {
                    log.debug("Validating connection failed: "+e.getMessage());
                    return false;
                }
                return true;
            }

            public void activateObject(Object object) throws Exception {
                Connection con = (Connection) object;
                con.setAutoCommit(true);
                log.debug("Pooled connection activated for use");
            }

            public void passivateObject(Object object) throws Exception {
            }
        };


        // Create the object pool
        pool = new GenericObjectPool(factory, 20, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 5000, 5, 2,
                true, false, 60000, 10, 60000, true, 20000);
    }

    public Connection getConnection() throws SQLException, NoSuchElementException {
        log.debug("Connection request ("+pool.getNumActive()+"/"+pool.getMaxActive()+" active with "+pool.getNumIdle()+" idle)");
        try {
            return (Connection) pool.borrowObject();
        } catch (SQLException e) {
            throw e;
        } catch (NoSuchElementException e) {
            throw e;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception connecting to database", e);
        }
    }

    public void returnConnection(Connection con) {
        if(con == null) return;
        log.debug("Connection returned to pool");
        try {
            pool.returnObject(con);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void returnConnection(Connection con, Statement st) {
        if(con == null) return;
        log.debug("Connection returned to pool");
        try {
            if(st != null) st.close();
            try {
                pool.returnObject(con);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch(SQLException e) {
            try {
                pool.invalidateObject(con);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    public void returnConnection(Connection con, Statement st, ResultSet rs) {
        if(con == null) return;
        log.debug("Connection returned to pool");
        try {
            if(rs != null) rs.close();
            if(st != null) st.close();
            try {
                pool.returnObject(con);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch(SQLException e) {
            try {
                pool.invalidateObject(con);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    public void close() {
        try {
            pool.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        factory = null;
        pool = null;
    }
}
