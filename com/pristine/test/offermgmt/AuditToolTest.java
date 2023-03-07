package com.pristine.test.offermgmt;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dataload.offermgmt.AuditEngineWS;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.PriceAdjustment;
import com.pristine.util.PropertyManager;

public class AuditToolTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	
	
	public static void main(String[] args) throws OfferManagementException, JsonParseException, JsonMappingException, IOException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("recommendation.properties");
		AuditToolTest priceAdjustementTest = new AuditToolTest();
		priceAdjustementTest.intialSetup();
		try{
			priceAdjustementTest.testAuditTool();
		}
		catch(GeneralException e){
			logger.error("Error while running audit - " + e.toString());
		}
	}
	
	private void testAuditTool() throws JsonParseException, JsonMappingException, IOException, GeneralException{
		AuditEngineWS auditEngineWS = new AuditEngineWS();
		//product-level-id="4" location-level-id="6" location-id="6" product-id="1266"
		int locationLevelId = 6;
		int locationId = 6;
		int productLevelId = 4;
		int productId = 1480;
		String userId = "prestolive";
		long reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		logger.info("Audit is in progress for report id - " + reportId);
		
		/*locationLevelId = 6;
		locationId = 69;
		productLevelId = 4;
		productId = 264;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 69;
		productLevelId = 4;
		productId = 149;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);

		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 264;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 69;
		productLevelId = 4;
		productId = 247;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 149;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 157;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 510;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 264;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);

		logger.info("Audit is in progress for report id - " + reportId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 160;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 569;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
		
		locationLevelId = 6;
		locationId = 66;
		productLevelId = 4;
		productId = 26;
		userId = "prestolive";
		reportId = auditEngineWS.getAuditHeader(locationLevelId, locationId, productLevelId, productId, userId);
*/		
		
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
