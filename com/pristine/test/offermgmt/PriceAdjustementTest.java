package com.pristine.test.offermgmt;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.PriceAdjustment;
import com.pristine.util.PropertyManager;

public class PriceAdjustementTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	
	
	public static void main(String[] args) throws OfferManagementException, JsonParseException, JsonMappingException, IOException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		PriceAdjustementTest priceAdjustementTest = new PriceAdjustementTest();
		priceAdjustementTest.intialSetup();
		//priceAdjustementTest.testPriceAdjustment();
		priceAdjustementTest.testPriceAdjustment1();
	}
	
	private void testPriceAdjustment1() throws OfferManagementException, JsonProcessingException {
		PriceAdjustment priceAdjustment = new PriceAdjustment();
		priceAdjustment.balancePIAndMargin(conn, 8515l, 'D');

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		String jsonString = mapper.writeValueAsString(16926);
		//priceAdjustment.rollbackAdjustedPriceTest(conn, jsonString);
	}
	
	private void testPriceAdjustment() throws JsonParseException, JsonMappingException, IOException{
		PriceAdjustment priceAdjustment = new PriceAdjustment();
		List<Long> runIds = new ArrayList<Long>();
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		 ObjectMapper mapper = new ObjectMapper();
		 mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		 JsonNode df;
		 
		//Store recommendation run
		//runIds.add((long) 7872);	
		runIds.add((long) 11534);
		runIds.add((long) 11136);	
		String jsonString = mapper.writeValueAsString(runIds);
		
//		String inpRunIds = "{'runIds':'[14756]'}";
//		 df = mapper.readValue(inpRunIds, JsonNode.class);
		//runStatusDTOs = priceAdjustment.adjustRecPriceToMeetPITest(conn, df.toString());
		
		runStatusDTOs = priceAdjustment.balancePIAndMarginTest(conn, jsonString);
		
		//runStatusDTOs = priceAdjustment.rollbackAdjustedPriceTest(conn, jsonString);
		
		for(RunStatusDTO runStatusDTO : runStatusDTOs){
			logger.debug("RunId: " + runStatusDTO.runId + ",Status Code: " + runStatusDTO.statusCode + ",Message: " + runStatusDTO.message);
		}
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
