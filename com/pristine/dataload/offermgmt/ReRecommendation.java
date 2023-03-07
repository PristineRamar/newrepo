package com.pristine.dataload.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.RerecommendationService;
import com.pristine.util.PropertyManager;

public class ReRecommendation {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("ReRecommendation");

	private static final String RUN_ID = "RUN_ID=";
	private String inputRunIds = "";
	List<Long> runIds = new ArrayList<Long>();

	public static void main(String[] args) {
		PropertyManager.initialize("recommendation.properties");

		ReRecommendation reRecommendation = new ReRecommendation();

		reRecommendation.intialSetup();
		
		// Set input arguments
		reRecommendation.setArguments(args);

		reRecommendation.mainEntry();
	}

	private void setArguments(String[] args) {
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(RUN_ID)) {
					inputRunIds = arg.substring(RUN_ID.length());
					runIds = Stream.of(inputRunIds.split(",")).map(Long::parseLong).collect(Collectors.toList());
				}
			}
		}
	}

	private void mainEntry() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			
			new RerecommendationService().rerecommendRetails(this.conn, mapper.writeValueAsString(runIds));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
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
