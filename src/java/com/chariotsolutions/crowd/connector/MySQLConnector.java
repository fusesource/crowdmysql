package com.chariotsolutions.crowd.connector;

import com.atlassian.crowd.integration.SearchContext;
import com.atlassian.crowd.integration.authentication.PasswordCredential;
import com.atlassian.crowd.integration.directory.RemoteDirectory;
import com.atlassian.crowd.integration.exception.InactiveAccountException;
import com.atlassian.crowd.integration.exception.InvalidAuthenticationException;
import com.atlassian.crowd.integration.exception.InvalidCredentialException;
import com.atlassian.crowd.integration.exception.InvalidGroupException;
import com.atlassian.crowd.integration.exception.InvalidPrincipalException;
import com.atlassian.crowd.integration.exception.InvalidRoleException;
import com.atlassian.crowd.integration.exception.ObjectNotFoundException;
import com.atlassian.crowd.integration.model.AttributeValues;
import com.atlassian.crowd.integration.model.RemoteGroup;
import com.atlassian.crowd.integration.model.RemotePrincipal;
import com.atlassian.crowd.integration.model.RemoteRole;
import org.apache.log4j.Logger;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Crowd connector that talks to a database with a proprietary schema.
 */
public class MySQLConnector implements RemoteDirectory {
    public final static String ATTRIBUTE_PHONE = "phone";
    public final static String ATTRIBUTE_TITLE = "title";
    public final static String ATTRIBUTE_COUNTRY = "country";
    public final static String ATTRIBUTE_COMPANY = "company";
    private final static Logger log = Logger.getLogger(MySQLConnector.class);
    private final static long DIRECTORY_ID = 42;
    private final static String USER_SELECT = "SELECT id, user_name, first_name, last_name, " +
                    "email, password, title, company, phone, country, created, account_disabled FROM user ";
    private final static int USER_ID_FIELD = 1;
    private final static int USER_NAME_FIELD = 2;
    private final static int USER_FIRST_NAME_FIELD = 3;
    private final static int USER_LAST_NAME_FIELD = 4;
    private final static int USER_EMAIL_FIELD = 5;
    private final static int USER_PASSWORD_FIELD = 6;
    private final static int USER_TITLE_FIELD = 7;
    private final static int USER_COMPANY_FIELD = 8;
    private final static int USER_PHONE_FIELD = 9;
    private final static int USER_COUNTRY_FIELD = 10;
    private final static int USER_CREATED_FIELD = 11;
    private final static int USER_DISABLED_FIELD = 12;
    private long id = DIRECTORY_ID;
    private static DBPool pool;
    static {
        try {
            pool = new DBPool();
        } catch (Exception e) {
            log.error("UNABLE TO CONFIGURE DATABASE POOL", e);
        }
    }


    public MySQLConnector() {
        log.debug("Creating new connection to MySQL directory");
    }

    public long getID() {
        return id;
    }

    public void setID(long l) {
        id = l;
    }

    public String getDirectoryType() {
        return "IONA Database";
    }

    public Map getAttributes() {
        return new HashMap();
    }

    public void setAttributes(Map map) {
        if(map.size() > 0) {
            log.debug("Setting directory attributes to "+map);
        }
    }

    /**
     * Adds a principal to the directory store.
     */
    public RemotePrincipal addPrincipal(RemotePrincipal principal) throws InvalidPrincipalException, RemoteException, InvalidCredentialException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = pool.getConnection();
            ps = con.prepareStatement("INSERT INTO user (user_name, first_name, last_name, " +
                    "email, password, title, company, phone, country, created, account_disabled) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            String password = null;
            if(principal.getCredentials().size() > 0) {
                password = ((PasswordCredential) principal.getCredentials().get(0)).getCredential();
            }

            // Handle Attributes
            String firstName = null;
            String lastName = null;
            String email = null;
            String company = null;
            String title = null;
            String country = null;
            String phoneNumber = null;

            for (Object o : principal.getAttributes().entrySet()) {
                Map.Entry entry = (Map.Entry) o;
                String key = (String) entry.getKey();
                if(key.equals(RemotePrincipal.FIRSTNAME)) {
                    firstName = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(RemotePrincipal.EMAIL)) {
                    email = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(RemotePrincipal.LASTNAME)) {
                    lastName = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_COUNTRY)) {
                    country = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_TITLE)) {
                    title = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_PHONE)) {
                    phoneNumber = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_COMPANY)) {
                    company = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else {
                    log.warn("Unrecognized attribute when creating principal: "+key+"="+entry.getValue());
                }
            }

            // Fill out the PreparedStatement
            ps.setString(1, principal.getName());
            setString(ps, 2, firstName);
            setString(ps, 3, lastName);
            setString(ps, 4, email);
            setString(ps, 5, password);
            setString(ps, 6, title);
            setString(ps, 7, company);
            setString(ps, 8, phoneNumber);
            setString(ps, 9, country);
            ps.setDate(10, new Date(System.currentTimeMillis()));
            ps.setInt(11, principal.isActive() ? 0 : 1);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            rs.next();
            long id = rs.getLong(1);
            principal.setID(id);
            return principal;
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    private static void setString(PreparedStatement ps, int index, String value) throws SQLException {
        if(value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    public RemoteGroup addGroup(RemoteGroup group) throws InvalidGroupException, RemoteException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("INSERT INTO group_table (group_name, description, disabled, created) VALUES (?, ?, ?, ?)");
            ps.setString(1, group.getName());
            ps.setString(2, group.getDescription());
            ps.setInt(3, group.isActive() ? 0 : 1);
            ps.setDate(4, new Date(System.currentTimeMillis()));
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            rs.next();
            long id = rs.getLong(1);
            group.setID(id);
            return group;
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public RemotePrincipal authenticate(String username, PasswordCredential[] pwds) throws RemoteException, InvalidPrincipalException, InactiveAccountException, InvalidAuthenticationException {
        if(username == null) {
            log.debug("Ignoring login attempt where username is null");
            throw new InvalidAuthenticationException("Login failed");
        }
        if(pwds == null || pwds.length < 1) {
            log.error("Login failed for "+username+" (no password supplied)");
            throw new InvalidAuthenticationException("Login failed");
        }
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement(USER_SELECT+" WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                log.error("Login failed for "+username+" (no such account)");
                throw new InvalidAuthenticationException("No such account");
            }
            String password = rs.getString(USER_PASSWORD_FIELD);
            boolean active = rs.getInt(USER_DISABLED_FIELD) == 0;

            if(!active) {
                log.error("Login failed for "+username+" (account disabled)");
                throw new InactiveAccountException("Account disabled");
            }

            PasswordCredential success = null;
            for (PasswordCredential pwd : pwds) {
                if(pwd.getCredential().equals(password)) {
                    success = pwd;
                    break;
                }
            }
            if(success == null) {
                log.error("Login failed for "+username+" (no supplied password matched)");
                throw new InvalidAuthenticationException("Login failed");
            }

            log.info("Successful login for "+username);

            return readPrincipal(username, rs, success);
        } catch (SQLException e) {
            log.error("Login failed for "+username+" (database error)", e);
            throw new RuntimeException("Unable to read from database", e);
        } catch(RuntimeException e) {
            log.error("Login failed for "+username+" (unexpected error)", e);
            throw e;
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public boolean isGroupMember(String group, String username) throws RemoteException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT 'x' FROM user u, group_table g, user_groups ug " +
                    "WHERE u.user_name=? AND g.group_name=? AND ug.user_id = u.id AND ug.group_id = g.id");
            ps.setString(1, username);
            ps.setString(2, group);
            rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public RemoteGroup findGroupByName(String name) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT id, group_name, created, description, disabled FROM group_table WHERE group_name=?");
            ps.setString(1, name);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new ObjectNotFoundException("No such group '"+name+"'");
            }
            long id = rs.getLong(1);
            Date created = rs.getDate(3);
            String description = rs.getString(4);
            boolean enabled = rs.getInt(5) == 0;

            RemoteGroup result = new RemoteGroup();
            result.setID(id);
            result.setName(name);
            result.setConception(created);
            result.setDirectoryID(this.id);
            result.setDescription(description);
            result.setActive(enabled);

            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement(USER_SELECT+" u, user_groups ug " +
                    "WHERE ug.group_id=? AND u.id=ug.user_id");
            ps.setLong(1, result.getID());
            rs = ps.executeQuery();
            while(rs.next()) {
                RemotePrincipal user = readPrincipal(null, rs, null);
                result.addMember(user);
            }

            return result;
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public void removeGroup(String name) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("DELETE FROM group_table WHERE group_name = ?");
            ps.setString(1, name);
            int count = ps.executeUpdate();
            if(count == 0) {
                throw new ObjectNotFoundException("No such group '"+name+"'");
            }
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps);
        }
    }

    public RemotePrincipal findPrincipalByName(String username) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement(USER_SELECT+" WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new ObjectNotFoundException("No such account");
            }
            return readPrincipal(username, rs, null);
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public void addPrincipalToGroup(String username, String groupname) throws RemoteException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT id FROM group_table WHERE group_name=?");
            ps.setString(1, groupname);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new RuntimeException("No such group '"+groupname+"'");
            }
            long groupId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("SELECT id FROM user WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new RuntimeException("No such user '"+username+"'");
            }
            long userId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("INSERT INTO user_groups (user_id, group_id) VALUES (?, ?)");
            ps.setLong(1, userId);
            ps.setLong(2, groupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public void removePrincipalFromGroup(String username, String groupname) throws RemoteException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT id FROM group_table WHERE group_name=?");
            ps.setString(1, groupname);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new RuntimeException("No such group '"+groupname+"'");
            }
            long groupId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("SELECT id FROM user WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new RuntimeException("No such user '"+username+"'");
            }
            long userId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("DELETE FROM user_groups WHERE user_id=? AND group_id=?");
            ps.setLong(1, userId);
            ps.setLong(2, groupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public void removePrincipal(String username) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT id FROM user WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new ObjectNotFoundException("No such user '"+username+"'");
            }
            long userId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("DELETE FROM user_groups WHERE user_id=?");
            ps.setLong(1, userId);
            ps.executeUpdate();
            ps.close();

            ps = con.prepareStatement("DELETE FROM user WHERE id=?");
            ps.setLong(1, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public void updatePrincipalCredential(String username, PasswordCredential pwc) throws RemoteException, ObjectNotFoundException, InvalidCredentialException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("UPDATE user SET password=? WHERE user_name=?");
            ps.setString(1, pwc.getCredential());
            ps.setString(2, username);
            int count = ps.executeUpdate();
            if(count == 0) {
                throw new ObjectNotFoundException("No such user '"+username+"'");
            }
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps);
        }
    }

    public void testConnection() throws RemoteException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT count(*) FROM user WHERE 0=1");
            rs = ps.executeQuery();
            rs.next();
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public List findGroupMemberships(String username) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("SELECT id FROM user WHERE user_name=?");
            ps.setString(1, username);
            rs = ps.executeQuery();
            if(!rs.next()) {
                throw new ObjectNotFoundException("No such user '"+username+"'");
            }
            long userId = rs.getLong(1);
            rs.close(); rs = null;
            ps.close();

            ps = con.prepareStatement("SELECT g.group_name FROM user_groups ug, group_table g " +
                    "WHERE ug.user_id=? AND g.id = ug.group_id");
            ps.setLong(1, userId);
            rs = ps.executeQuery();
            List<String> list = new ArrayList<String>();
            while(rs.next()) {
                list.add(rs.getString(1));
            }
            return list;
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public List searchGroups(SearchContext searchContext) throws RemoteException {
        log.debug("Group Search "+searchContext);

        String sql = "SELECT id, group_name, created, description, disabled FROM group_table";
        String where = "";
        boolean members = false;
        long min = 0, count = -1;
        List<Object> params = new ArrayList<Object>();
        for (Object o : searchContext.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            if (entry.getKey().equals("group.populate.memberships")) {
                members = (Boolean) entry.getValue();
            } else if (entry.getKey().equals("search.max.results")) {
                count = (Long)entry.getValue()-1l;
            } else if (entry.getKey().equals("search.index.start")) {
                min = (Long)entry.getValue();
            } else if (entry.getKey().equals("group.directory.id")) {
                long value = (Long) entry.getValue();
                if (value != this.id) {
                    return Collections.EMPTY_LIST;
                }
            } else if (entry.getKey().equals("group.name")) {
                where += "AND group_name=? ";
                params.add(entry.getValue());
            } else if (entry.getKey().equals("group.active")) {
                where += "AND disabled=? ";
                boolean active = ((Boolean)entry.getValue());
                params.add(active ? 0 : 1);
            } else {
                log.warn("Unexpected group search criterion: " + entry.getKey() + " = " + entry.getValue());
            }
        }
        if(where.length() > 0) {
            sql += " WHERE "+where.substring(4);
        }
        if(count > -1) {
            if(min > 0) {
                sql += " LIMIT "+min+","+count;
            } else {
                sql += " LIMIT "+count;
            }
        }

        List<RemoteGroup> list = new ArrayList<RemoteGroup>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            log.debug("Group search SQL is "+sql);

            ps = con.prepareStatement(sql);
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                ps.setObject(i+1, param);
            }
            rs = ps.executeQuery();
            while(rs.next()) {
                RemoteGroup result = new RemoteGroup();
                result.setID(rs.getLong(1));
                result.setName(rs.getString(2));
                result.setConception(rs.getDate(3));
                result.setDirectoryID(this.id);
                result.setDescription(rs.getString(4));
                result.setActive(rs.getInt(5) == 0);

                if(members) {
                    PreparedStatement ps2 = con.prepareStatement(USER_SELECT+" u, user_groups ug " +
                            "WHERE ug.group_id=? AND u.id=ug.user_id");
                    ps2.setLong(1, result.getID());
                    ResultSet rs2 = ps2.executeQuery();
                    while(rs2.next()) {
                        RemotePrincipal user = readPrincipal(null, rs, null);
                        result.addMember(user);
                    }
                    rs2.close();
                    ps2.close();
                }

                list.add(result);
            }
            return list;
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public List searchPrincipals(SearchContext searchContext) throws RemoteException {
        log.debug("Principal Search "+searchContext);
        String sql = USER_SELECT;
        String where = "";
        List<Object> params = new ArrayList<Object>();
        long min = 0, count = -1;
        for (Object o : searchContext.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            if (entry.getKey().equals("search.max.results")) {
                count = (Long)entry.getValue()-1l;
            } else if (entry.getKey().equals("search.index.start")) {
                min = (Long)entry.getValue();
            } else if (entry.getKey().equals("principal.directory.id")) {
                long value = (Long) entry.getValue();
                if (value != this.id) {
                    return Collections.EMPTY_LIST;
                }
            } else if (entry.getKey().equals("principal.name")) {
                where += "AND user_name=? ";
                params.add(entry.getValue());
            } else if (entry.getKey().equals("principal.email")) {
                where += "AND email=? ";
                params.add(entry.getValue());
            } else if (entry.getKey().equals("principal.active")) {
                where += "AND account_disabled=? ";
                boolean active = ((Boolean)entry.getValue());
                params.add(active ? 0 : 1);
            } else {
                log.warn("Unexpected Principal Search criterion: " + entry.getKey() + " = " + entry.getValue());
            }
        }
        if(where.length() > 0) {
            sql += " WHERE "+where.substring(4);
        }
        if(count > -1) {
            if(min > 0) {
                sql += " LIMIT "+min+","+count;
            } else {
                sql += " LIMIT "+count;
            }
        }


        List<RemotePrincipal> list = new ArrayList<RemotePrincipal>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = pool.getConnection();
            log.debug("User search SQL is "+sql);
            ps = con.prepareStatement(sql);
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                ps.setObject(i+1, param);
            }
            rs = ps.executeQuery();
            while(rs.next()) {
                RemotePrincipal result = readPrincipal(null, rs, null);
                list.add(result);
            }
            return list;
        } catch (SQLException e) {
            throw new RemoteException("Unable to read from database", e);
        } finally {
            pool.returnConnection(con, ps, rs);
        }
    }

    public RemoteGroup updateGroup(RemoteGroup group) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("UPDATE group_table SET group_name=?, description=?, disabled=? WHERE id=?");
            ps.setString(1, group.getName());
            ps.setString(2, group.getDescription());
            ps.setInt(3, group.isActive() ? 0 : 1);
            ps.setLong(4, group.getID());
            int count = ps.executeUpdate();
            if(count == 0) {
                throw new ObjectNotFoundException("No such group '"+group.getID()+"'");
            }
            return group;
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps);
        }
    }

    public RemotePrincipal updatePrincipal(RemotePrincipal principal) throws RemoteException, ObjectNotFoundException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = pool.getConnection();
            ps = con.prepareStatement("UPDATE user SET user_name=?, first_name=?, last_name=?, " +
                    "email=?, password=?, title=?, company=?, phone=?, country=?, account_disabled=? WHERE id=?");

            String password = null;
            if(principal.getCredentials().size() > 0) {
                password = ((PasswordCredential) principal.getCredentials().get(0)).getCredential();
            }

            // Handle Attributes
            String firstName = null;
            String lastName = null;
            String email = null;
            String company = null;
            String title = null;
            String country = null;
            String phoneNumber = null;

            for (Object o : principal.getAttributes().entrySet()) {
                Map.Entry entry = (Map.Entry) o;
                String key = (String) entry.getKey();
                if(key.equals(RemotePrincipal.FIRSTNAME)) {
                    firstName = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(RemotePrincipal.EMAIL)) {
                    email = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(RemotePrincipal.LASTNAME)) {
                    lastName = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_COUNTRY)) {
                    country = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_TITLE)) {
                    title = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_PHONE)) {
                    phoneNumber = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else if(key.equals(ATTRIBUTE_COMPANY)) {
                    company = (String)((AttributeValues) entry.getValue()).getValues().get(0);
                } else {
                    log.warn("Unrecognized attribute when updating principal: "+key+"="+entry.getValue());
                }
            }

            // Fill out the PreparedStatement
            ps.setString(1, principal.getName());
            setString(ps, 2, firstName);
            setString(ps, 3, lastName);
            setString(ps, 4, email);
            setString(ps, 5, password);
            setString(ps, 6, title);
            setString(ps, 7, company);
            setString(ps, 8, phoneNumber);
            setString(ps, 9, country);
            ps.setInt(10, principal.isActive() ? 0 : 1);
            ps.setLong(11, principal.getID());
            int count = ps.executeUpdate();
            if(count == 0) {
                throw new ObjectNotFoundException("No such user '"+principal.getID()+"'");
            }
            return principal;
        } catch (SQLException e) {
            throw new RemoteException("Unable to write to database", e);
        } finally {
            pool.returnConnection(con, ps);
        }
    }

    public List searchRoles(SearchContext searchContext) throws RemoteException {
        return Collections.EMPTY_LIST;
    }

    public RemoteRole findRoleByName(String string) throws RemoteException, ObjectNotFoundException {
        throw new ObjectNotFoundException();
    }

    public RemoteRole addRole(RemoteRole remoteRole) throws InvalidRoleException, RemoteException {
        throw new UnsupportedOperationException("NOT IMPLEMENTED: addRole");
    }

    public RemoteRole updateRole(RemoteRole remoteRole) throws RemoteException, ObjectNotFoundException {
        throw new UnsupportedOperationException("NOT IMPLEMENTED: updateRole");
    }

    public void removeRole(String string) throws RemoteException, ObjectNotFoundException {
        throw new UnsupportedOperationException("NOT IMPLEMENTED: removeRole");
    }

    public void addPrincipalToRole(String string, String string1) throws RemoteException {
        throw new UnsupportedOperationException("NOT IMPLEMENTED: addPrincipalToRole");
    }

    public void removePrincipalFromRole(String string, String string1) throws RemoteException {
        throw new UnsupportedOperationException("NOT IMPLEMENTED: removePrincipal");
    }

    public boolean isRoleMember(String role, String principal) throws RemoteException {
        return false;
    }

    public List findRoleMemberships(String username) throws RemoteException, ObjectNotFoundException {
        return Collections.EMPTY_LIST;
    }



    private RemotePrincipal readPrincipal(String username, ResultSet rs, PasswordCredential pwd) throws SQLException {
        long id = rs.getLong(USER_ID_FIELD);
        if(username == null) username = rs.getString(USER_NAME_FIELD);
        String firstName = rs.getString(USER_FIRST_NAME_FIELD);
        String lastName = rs.getString(USER_LAST_NAME_FIELD);
        String email = rs.getString(USER_EMAIL_FIELD);
        if(pwd == null) {
            String password = rs.getString(USER_PASSWORD_FIELD);
            pwd = new PasswordCredential();
            pwd.setCredential(password);
        }
        String title = rs.getString(USER_TITLE_FIELD);
        String company = rs.getString(USER_COMPANY_FIELD);
        String phoneNumber = rs.getString(USER_PHONE_FIELD);
        String country = rs.getString(USER_COUNTRY_FIELD);
        Date created = rs.getDate(USER_CREATED_FIELD);
        boolean active = rs.getInt(USER_DISABLED_FIELD) == 0;

        RemotePrincipal result = new RemotePrincipal();
        result.setID(id);
        result.setName(username);
        result.setCredentials(Collections.singletonList(pwd));
        result.setCredentialHistory(Collections.singletonList(pwd));
        result.setEmail(email);
        result.setConception(created);
        result.setDirectoryID(this.id);
        result.setActive(active);
        result.setAttribute(RemotePrincipal.FIRSTNAME, firstName);
        result.setAttribute(RemotePrincipal.LASTNAME, lastName);
        if(company != null) result.setAttribute(ATTRIBUTE_COMPANY, company);
        if(title != null) result.setAttribute(ATTRIBUTE_TITLE, title);
        if(phoneNumber != null) result.setAttribute(ATTRIBUTE_PHONE, phoneNumber);
        if(country != null) result.setAttribute(ATTRIBUTE_COUNTRY, country);
        return result;

    }
}


