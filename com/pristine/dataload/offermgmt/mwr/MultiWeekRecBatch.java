package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

/**
 * Quarter/Multi week recommendation main class
 * 
 * @author Pradeepkumar
 * @version 1.0
 * 
 */

public class MultiWeekRecBatch {

	private static Logger logger = Logger.getLogger("MultiWeekRecommendation");

	// Input parameter constants
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String REC_TYPE = "REC_TYPE=";
	private static final String START_WEEK = "START_WEEK=";
	private static final String END_WEEK = "END_WEEK=";
	private static final String QUARTER_START_DATE = "QUARTER_START_DATE=";
	private static final String QUARTER_END_DATE = "QUARTER_END_DATE=";
	private static final String NO_OF_WEEKS_IN_ADVANCE = "NO_OF_WEEKS_IN_ADVANCE=";
	private static final String QUEUE_ID = "QUEUE_ID=";
	private static final String SUBMITTED_BY = "SUBMITTED_BY=";

	// Input variables
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int noOfWeeksInAdvance;
	private String recType;
	private String startWeek;
	private String endWeek;
	private String quarterStartDate;
	private String quarterEndDate;
	private boolean isGlobalZone;
	private long queueID;
	private String submittedBy = "";

	// Connection variables
	private Connection conn = null;

	public static void main(String[] args) {

		PropertyManager.initialize("recommendation.properties");
		MultiWeekRecBatch multiWeekRecommendation = new MultiWeekRecBatch();
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					multiWeekRecommendation
							.setLocationLevelId(Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length())));
				} else if (arg.startsWith(LOCATION_ID)) {
					multiWeekRecommendation.setLocationId(Integer.parseInt(arg.substring(LOCATION_ID.length())));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					multiWeekRecommendation
							.setProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				} else if (arg.startsWith(PRODUCT_ID)) {
					multiWeekRecommendation.setProductId(Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				} else if (arg.startsWith(REC_TYPE)) {
					multiWeekRecommendation.setRecType(arg.substring(REC_TYPE.length()));
				} else if (arg.startsWith(START_WEEK)) {
					multiWeekRecommendation.setStartWeek(arg.substring(START_WEEK.length()));
				} else if (arg.startsWith(END_WEEK)) {
					multiWeekRecommendation.setEndWeek(arg.substring(END_WEEK.length()));
				} else if (arg.startsWith(QUARTER_START_DATE)) {
					multiWeekRecommendation.setQuarterStartDate(arg.substring(QUARTER_START_DATE.length()));
				} else if (arg.startsWith(QUARTER_END_DATE)) {
					multiWeekRecommendation.setQuarterEndDate(arg.substring(QUARTER_END_DATE.length()));
				} else if (arg.startsWith(NO_OF_WEEKS_IN_ADVANCE)) {
					multiWeekRecommendation
							.setNoOfWeeksInAdvance(Integer.parseInt(arg.substring(NO_OF_WEEKS_IN_ADVANCE.length())));
				} else if (arg.startsWith(QUEUE_ID)) {
					multiWeekRecommendation.setQueueID(Long.parseLong(arg.substring(QUEUE_ID.length())));
				} else if (arg.startsWith(SUBMITTED_BY)) {
					multiWeekRecommendation.setSubmittedBy((arg.substring(SUBMITTED_BY.length())));

				}
			}

			logger.info("***********************************************************");
			multiWeekRecommendation.initiateMultiWeekRecommendation();
			logger.info("***********************************************************");
		}
	}

	/**
	 * Core menthod for multi week recommendation.
	 */
	private void initiateMultiWeekRecommendation() {

		try {
			// Initilize connection and logger properties
			initialize();

			// Call recommendation flow
			callMultiWeekRecommendation();

		} catch (GeneralException | Exception e) {
			logger.error("Unable to execute muti week recommendation", e);
		}
	}

	/**
	 * Inializes required objects for multi week recommendation
	 * 
	 * @throws GeneralException
	 */
	private void initialize() throws GeneralException {

		// Standard Conenction
		setupStandardConnection();

		// Log4j properties initializtion for product and location
		setLog4jProperties(getLocationId(), getProductId());
	}

	/**
	 * Calls the recommendation flow
	 * @throws GeneralException 
	 */
	private void callMultiWeekRecommendation() throws GeneralException {

		// Get Input parameters
		RecommendationInputDTO recommendationInputDTO = getRecommendationInput();

		// Create object for recommendation flow with input parameters and constructor
		RecommendationFlow recommendationFlow = new RecommendationFlow();

		// Call multi week recommendation
		recommendationFlow.multiWeekRecommendation(getConnection(), recommendationInputDTO);
	}

	/**
	 * Initializes connection
	 * 
	 * @throws GeneralException
	 */
	protected void setupStandardConnection() throws GeneralException {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	/**
	 * 
	 * @param locationId
	 * @param productId
	 * @throws GeneralException
	 */
	public void setLog4jProperties(int locationId, int productId) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");

		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager
				.getProperty("log4j.appender.console.layout.ConversionPattern");

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();

		String curWkStartDate = getStartWeek() != null ? getStartWeek() : DateUtil.getWeekStartDate(-1);
		RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), curWkStartDate,
				Constants.CALENDAR_WEEK);

		Date recWeekStartDate = DateUtil.toDate(curWkDTO.getStartDate());
		SimpleDateFormat nf = new SimpleDateFormat("MM-dd-yyy");
		String dateInLog = nf.format(recWeekStartDate);

		String catAndZoneNum = String.valueOf(locationId) + "_" + String.valueOf(productId);
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		logPath = logPath + "/" + catAndZoneNum + "_" + dateInLog + "_" + timeStamp + ".log";

		Properties props = new Properties();
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);

		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}

	/**
	 * 
	 * @return input parameters for recommendation
	 */
	private RecommendationInputDTO getRecommendationInput() {
		// Set input parameters
		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
		recommendationInputDTO.setLocationLevelId(getLocationLevelId());
		recommendationInputDTO.setLocationId(getLocationId());
		recommendationInputDTO.setProductLevelId(getProductLevelId());
		recommendationInputDTO.setProductId(getProductId());
		recommendationInputDTO.setQuarterStartDate(getQuarterStartDate());
		recommendationInputDTO.setQuarterEndDate(getQuarterEndDate());
		recommendationInputDTO.setStartWeek(getStartWeek());
		recommendationInputDTO.setEndWeek(getEndWeek());
		recommendationInputDTO.setActualStartWeek(getStartWeek());
		recommendationInputDTO.setActualEndWeek(getEndWeek());
		recommendationInputDTO.setRecType(getRecType());
		recommendationInputDTO.setNoOfWeeksInAdvance(getNoOfWeeksInAdvance());
		recommendationInputDTO.setGlobalZone(isGlobalZone());
		if (getSubmittedBy() != "") {
			recommendationInputDTO.setUserId(getSubmittedBy());
		} else {
			recommendationInputDTO.setUserId(PRConstants.BATCH_USER);
		}
		if(getQueueID() > 0) {
			recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_DASHBOARD);
		} else {
			recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_BATCH);
		}
		recommendationInputDTO.setQueueId(getQueueID());
		return recommendationInputDTO;
	}

	protected Connection getConnection() {
		return conn;
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public String getRecType() {
		return recType;
	}

	public void setRecType(String recType) {
		this.recType = recType;
	}

	public String getStartWeek() {
		return startWeek;
	}

	public void setStartWeek(String startWeek) {
		this.startWeek = startWeek;
	}

	public String getEndWeek() {
		return endWeek;
	}

	public void setEndWeek(String endWeek) {
		this.endWeek = endWeek;
	}

	public String getQuarterStartDate() {
		return quarterStartDate;
	}

	public void setQuarterStartDate(String quarterStartDate) {
		this.quarterStartDate = quarterStartDate;
	}

	public String getQuarterEndDate() {
		return quarterEndDate;
	}

	public void setQuarterEndDate(String quarterEndDate) {
		this.quarterEndDate = quarterEndDate;
	}

	public int getNoOfWeeksInAdvance() {
		return noOfWeeksInAdvance;
	}

	public void setNoOfWeeksInAdvance(int noOfWeeksInAdvance) {
		this.noOfWeeksInAdvance = noOfWeeksInAdvance;
	}

	public boolean isGlobalZone() {
		return isGlobalZone;
	}

	public void setGlobalZone(boolean isGlobalZone) {
		this.isGlobalZone = isGlobalZone;
	}

	public long getQueueID() {
		return queueID;
	}

	public void setQueueID(long queueID) {
		this.queueID = queueID;
	}

	public String getSubmittedBy() {
		return submittedBy;
	}

	public void setSubmittedBy(String submittedBy) {
		this.submittedBy = submittedBy;
	}
}
