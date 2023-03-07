package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
//import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
//import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.PropertyManager;

public class StrategyTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	ObjectMapper mapper = new ObjectMapper();
	
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
	
	@Before
	public void init() {
//		PropertyConfigurator.configure("log4j-pricing-engine.properties");
		PropertyManager.initialize("recommendation.properties");
		intialSetup();
	}
	
	public void getStrategyDetail() throws GeneralException, OfferManagementException{
		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();	
		PRStrategyDTO actualStrategy = new PRStrategyDTO();
		PRStrategyDTO expStrategy = new PRStrategyDTO();
		StrategyDAO strategyDAO = new StrategyDAO();
		
		List<Long> strategyIds =  strategyDAO.GetAllStrategyId(conn);
		int counter = 0;
		for (Long strategyId : strategyIds) {
			if (strategyId > 1376 ) {
			
				logger.debug("Remaining Strategies: " + (strategyIds.size() - counter));
				logger.debug("Strategy Id : " + strategyId);
				tStartTime = System.currentTimeMillis();
				actualStrategy = strategyDAO.getStrategyDefinition(conn, strategyId);
				tEndTime = System.currentTimeMillis();
				logger.debug("^^^ Time -- New Strategy Function" + ((tEndTime - tStartTime) / 1000) + " s ^^^");

//				PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//				tStartTime = System.currentTimeMillis();
//				expStrategy = pricingEngineDAO.getStrategyDefinition(conn, strategyId);
//				tEndTime = System.currentTimeMillis();
//				logger.debug("^^^ Time -- Old Strategy Function" + ((tEndTime - tStartTime) / 1000) + " s ^^^");

				try {
					String actualStrategyLog = mapper.writeValueAsString(actualStrategy);
					String expStrategyLog = mapper.writeValueAsString(expStrategy);
					assertEquals("JSON Not Matching", expStrategyLog, actualStrategyLog);
				//} catch (JsonProcessingException | AssertionError e) {
				} catch (JsonProcessingException e) {
					logger.debug("Error Strategy Id: " + strategyId);
					e.printStackTrace();
				}
			}
				counter++;
		}
	}
	
}

