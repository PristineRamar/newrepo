package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;

public class Dashboard {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		Dashboard dashboard = new Dashboard();
		dashboard.intialSetup();
		dashboard.test();		
	}
	
	public void test(){
		DashboardDAO dashboardDAO = new DashboardDAO(conn);
		HashMap<Integer, ItemDTO> allItems = new HashMap<Integer,ItemDTO>();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		
		priceZoneStores.add(1557);
		priceZoneStores.add(1558);
		priceZoneStores.add(1561);
		priceZoneStores.add(1562);
		priceZoneStores.add(1564);
		
		//allItems = dashboardDAO.getAllItems(66, 4, 264, priceZoneStores);
		
		//dashboardDAO.getNewItemCount(66, 4, 264, "06/01/2014", "09/19/2014", priceZoneStores);
		
		//dashboardDAO.getRevenueData(66, 4, 264, "09/07/2014", priceZoneStores);
		
		//dashboardDAO.getCompPriceChangeCount(66, 978, 4, 264, "06/01/2014", "09/19/2014", authorizedItems);
		
	}
	
	public void intialSetup() {
		initialize();
	}
	
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
