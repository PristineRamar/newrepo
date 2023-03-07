/*
 * Author : Suresh 
 * Start Date : Jul 22, 2009
 * 
 * Change Description 					Changed By 				Date
 * ------------------------------------------------------------------------
 * Added getOracleConnection method 	Naimish 				07/22/2009
 */

package com.pristine.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PropertyManager;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;

public class DBManager
{
	static Logger									logger	= Logger.getLogger("DBManager");
	private static Connection						conn	= null;
	private static OracleConnectionPoolDataSource	dataSource;
	private static String							db_username;
	private static String							db_password;
	private static String							db_connection_url;
	private static String							db_con_min_limit;
	private static String							db_con_max_limit;
	private static String							db_con_initial_limit;
	private static String							db_con_inactivity_timeout;
	private static String							db_con_abondened_timeout;
	public static String							db_link_name;
	public static String							db_ident_incr;
	static
	{
		PropertyManager.initialize(DBManager.class.getClassLoader().getResourceAsStream(Constants.CONST_DB_FILENAME));

		if (dataSource == null)
		{
			try
			{
				// Function initializing the DB property file.
				initialiseDBProperties();

				// Function initializing the datasource
				initialiseDataSource();
			}
			catch (Exception e)
			{
				GenericUtil.logError(e.getMessage(), e);
			}
		}
	}

	private static void initialiseDBProperties()
	{
		db_username = PropertyManager.getProperty(Constants.CONST_DB_USERNAME);
		db_password = PropertyManager.getProperty(Constants.CONST_DB_PASSWORD);
		db_connection_url = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_URL);

		// Initializing caching properties
		db_con_min_limit = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_MIN_LIMIT);
		db_con_max_limit = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_MAX_LIMIT);
		db_con_initial_limit = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_INITIAL_LIMIT);
		db_con_inactivity_timeout = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_INACTIVITY_TIMEOUT);
		db_con_abondened_timeout = PropertyManager.getProperty(Constants.CONST_DB_CONNECTION_ABONDENED_TIMEOUT);
		db_link_name = PropertyManager.getProperty("DB_LINK_NAME");
		db_ident_incr = PropertyManager.getProperty("DB_IDENT_INCR","%s.NEXTVAL");
	}

	public static Connection getConnection() throws GeneralException {
		try {
			if( conn == null) {
				connect();
			}else {
				if ( conn.isClosed() )
					connect();
			}
		} catch (Exception e) {
			//logger.error("Error in connection", e);
			throw new GeneralException( "Connection Failed", e);
		}
		return conn;
	}
	
	public static String getIdentityIncrement(String seqName) {
		return String.format(db_ident_incr, seqName);
	}

	private static void connect() throws SQLException {
		String db_connect_string = PropertyManager.getProperty("DB_CONNECT_STRING");
		String db_userid = PropertyManager.getProperty("DB_USER");
		String db_password = PropertyManager.getProperty("DB_PASSWORD");
		
		logger.info("db connection String = " + db_connect_string);
		logger.info("db User Id = " + db_userid);
		
		DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
		conn = DriverManager.getConnection(db_connect_string, db_userid, db_password);
		conn.setAutoCommit(false);
		logger.debug("connected to database");
	}


	public static final void initialiseDataSource() throws Exception
	{
		// Pooling the connections
		dataSource = new OracleConnectionPoolDataSource();

		dataSource.setURL(db_connection_url);
		dataSource.setUser(db_username);
		dataSource.setPassword(db_password);

		// Caching the connections
		//dataSource.setImplicitCachingEnabled(true);
		//dataSource.setConnectionCacheName(Constants.CONST_DB_CACHE_NAME);

		java.util.Properties prop = new java.util.Properties();
		prop.setProperty(Constants.CONST_DB_CONNECTION_MIN_LIMIT, db_con_min_limit);
		prop.setProperty(Constants.CONST_DB_CONNECTION_MAX_LIMIT, db_con_max_limit);
		prop.setProperty(Constants.CONST_DB_CONNECTION_INITIAL_LIMIT, db_con_initial_limit);
		prop.setProperty(Constants.CONST_DB_CONNECTION_INACTIVITY_TIMEOUT, db_con_inactivity_timeout);
		prop.setProperty(Constants.CONST_DB_CONNECTION_ABONDENED_TIMEOUT, db_con_abondened_timeout);

		//dataSource.setConnectionCacheProperties(prop);
	}

	public static OracleConnectionPoolDataSource getDataSource()
	{
		return dataSource;
	}

	public static void setDataSource(OracleConnectionPoolDataSource dataSource)
	{
		DBManager.dataSource = dataSource;
	}

	public static Connection getOracleConnection() throws GeneralException
	{
//		logger.debug("Connection created from connection pool.");
		
		GenericUtil.logMessage("Connection String " + db_connection_url);
		GenericUtil.logMessage("User Id " + db_username);

		Connection con = null;

		try
		{
			con = dataSource.getPooledConnection().getConnection();
			con.setAutoCommit(false);
		}
		catch (Exception e)
		{
			throw new GeneralException(e.getMessage(), e);
		}

		return con;
	}
	
	/*	// method added for spring jdbc connection
	public static JdbcTemplate getJdbcTemplate() throws GeneralException {

		ApplicationContext ac = null;
		DataSource dataSource = null;
		JdbcTemplate jdbcTemplate = null;

		try {
			ac = new ClassPathXmlApplicationContext("applicationContext.xml",
					DBManager.class);
			dataSource = (DataSource) ac.getBean("dataSource");
			jdbcTemplate = new JdbcTemplate(dataSource);
		} catch (Exception exe) {
			logger.error(exe);
		}
		return jdbcTemplate;
	}
	*/
}