package com.pristine.test;

import java.sql.Connection;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ConnectionTestMainClass {
	private Connection conn = null;
	
	public static void main(String[] args) {
		PropertyManager.initialize("recommendation.properties");
		ConnectionTestMainClass connectionTestMainClass = new ConnectionTestMainClass();
		
		connectionTestMainClass.doProcess();
	}
	
	private void doProcess() {
		try {
			initialize();
			
			ConnectionTestSubClass connectionTestSubClass = new ConnectionTestSubClass();
			
			PristineDBUtil.close(conn);
			
			//this is long process, so connection need not to be opened unnecessarily
			connectionTestSubClass.scenario1();
			
			setConnection();
			
			//query something
			new PricingEngineDAO().getPriceZoneIdForStore(conn, 124);
					
		} catch (GeneralException | Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void initialize() throws GeneralException, Exception {
		setConnection();
	}
	
	
	protected void setConnection() throws Exception {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			throw new Exception("Error in setConnection() - " + e);
		}
	}

	 

}
