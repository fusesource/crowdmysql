package com.chariotsolutions.crowd.connector;

import com.atlassian.crowd.integration.model.RemotePrincipal;
import com.atlassian.crowd.integration.authentication.PasswordCredential;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Date;
import java.util.Collections;

public class PrincipalRowMapper implements ParameterizedRowMapper<RemotePrincipal> {

    public RemotePrincipal mapRow(ResultSet rs, int i) throws SQLException {

        long id = rs.getLong("id");
        String username = rs.getString("user_name");
        String email = rs.getString("email");
        String firstName = rs.getString("first_name");
        String lastName = rs.getString("last_name");
        String company = rs.getString("company");
        String title = rs.getString("title");
        String phone = rs.getString("phone");
        String country = rs.getString("country");

        String password = rs.getString("password");
        PasswordCredential credential = new PasswordCredential(password, true);

        boolean active = !rs.getBoolean("account_disabled");
        Date created = rs.getDate("created");

        RemotePrincipal principal = new RemotePrincipal();

        principal.setID(id);
        principal.setName(username);
        principal.setEmail(email);
        principal.setActive(active);
        principal.setAttribute(RemotePrincipal.FIRSTNAME, firstName);
        principal.setAttribute(RemotePrincipal.LASTNAME, lastName);
        principal.setAttribute(RemotePrincipal.DISPLAYNAME, firstName + " " + lastName);

        principal.setAttribute("company", company);
        principal.setAttribute("title", title);
        principal.setAttribute("phone", phone);
        principal.setAttribute("country", country);

        principal.setConception(created);

        principal.setCredentials(Collections.singletonList(credential));
        principal.setCredentialHistory(Collections.singletonList(credential));

        return principal;
    }
}
