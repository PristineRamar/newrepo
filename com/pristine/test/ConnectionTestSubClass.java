package com.pristine.test;

import java.sql.Connection;
import java.sql.SQLException;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ConnectionTestSubClass {
	private Connection _conn = null;
	
	public void testConnection(Connection conn) {
		PristineDBUtil.close(conn);
		
	}
	
	public void scenario1() throws Exception, GeneralException {
		//open connection
		_conn = getNewConnection();
		
		//do process
		process(_conn);
		
		
		new PricingEngineDAO().getPriceZoneIdForStore(_conn, 124);
		
	}
	
	private void process(Connection conn) throws SQLException, GeneralException {
		PristineDBUtil.close(_conn);
		
		_conn = getNewConnection();
		
		new PricingEngineDAO().getPriceZoneIdForStore(_conn, 124);
		
	}
	
	protected Connection setConnection(Connection conn) throws SQLException{
		if(conn == null || conn.isClosed()){
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
//				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
		return conn;
	}
	
	protected Connection getNewConnection() throws SQLException, GeneralException{
		return DBManager.getConnection();
	}
}
