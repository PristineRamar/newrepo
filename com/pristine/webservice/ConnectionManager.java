package com.pristine.webservice;

import java.sql.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pristine.util.PropertyManager;

final class ConnectionManager {
	private static Logger logger = Logger.getLogger("ConnectionManager");
	
		
	/**
	 * Get connection from JNDI source. Typically used when getting connections from Application Server or Web Container
	 * @return
	 */
	public static Connection getJNDIConnection() {

		Connection connection = null;
		try {
			Context initialContext = new InitialContext();
			Context envContext  = (Context)initialContext.lookup("java:/comp/env");
			if (envContext == null) {
				logger.error("JNDI problem. Cannot get Environment Context.");
			}
			DataSource ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			if (ds != null) {
				connection = ds.getConnection();
			} else {
				logger.error("Failed to lookup datasource : " + PropertyManager.getProperty("WS_CONNECTION") );
			}
		} catch (NamingException ex) {
			logger.error("Error when creating connection from datasource " + ex.toString());
		} catch (SQLException ex) {
			logger.error("Error when creating connection from datasource " + ex.toString());
		}
		return connection;
	}

}
