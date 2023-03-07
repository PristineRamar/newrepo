package com.pristine.dataload.offermgmt.mwr;

//import java.io.IOException;
//import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import com.pristine.dao.DBManager;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class RecommendationWS {
	private static Logger logger = Logger.getLogger("RecommendationWS");
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private Connection conn = null;
	/*private int batchLocationId = -1;
	private int batchProductId = -1;*/
	
	public long runStrategyWhatIf(int locationLevelId, int locationId, int productLevelId, int productId,
			String predictedUserId, List<Long> strategyId,int getOnlyTempStrat) throws GeneralException {
		initializeForWS(locationId, productId);
		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setProductId(productId);
		recommendationInputDTO.setRecType(PRConstants.MW_WEEK_RECOMMENDATION);
		recommendationInputDTO.setRunMode(PRConstants.RUN_TYPE_ONLINE);
		recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_TEMP);
		recommendationInputDTO.setNoOfWeeksInAdvance(1);
		recommendationInputDTO.setUserId(predictedUserId);
		recommendationInputDTO.setStrategyId(strategyId.get(0));
		if (getOnlyTempStrat == 1)
			recommendationInputDTO.setRunOnlyTempStrats(true);
		else
			recommendationInputDTO.setRunOnlyTempStrats(false);
		RecommendationFlow recommendationFlow = new RecommendationFlow();
		recommendationFlow.multiWeekRecommendation(conn, recommendationInputDTO);
		return recommendationInputDTO.getRunId();
	}
	
	
	public long runPriceRecommendation(int locationLevelId, int locationId, int productLevelId, int productId,
			String predictedUserId) throws GeneralException {
		initializeForWS(locationId, productId);
		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setProductId(productId);
		recommendationInputDTO.setRecType(PRConstants.MW_QUARTER_RECOMMENDATION);
		recommendationInputDTO.setRunMode(PRConstants.RUN_TYPE_ONLINE);
		recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_DASHBOARD);
		recommendationInputDTO.setNoOfWeeksInAdvance(1);
		recommendationInputDTO.setUserId(predictedUserId);
		RecommendationFlow recommendationFlow = new RecommendationFlow();
		recommendationFlow.multiWeekRecommendation(conn, recommendationInputDTO);
		return recommendationInputDTO.getRunId();
	}
	
		
	/**
	 * Initializes connection. Used when program is accessed through webservice
	 * @throws GeneralException 
	 */
	protected void initializeForWS(int locationId, int productId) throws GeneralException {
		setConnection(getDSConnection());
		//setLog4jProperties(locationId, productId);
		logger.info("Connection : " + getConnection());
	}
	
	/*public void setLog4jProperties(int locationId, int productId) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");
		
		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager.getProperty("log4j.appender.console.layout.ConversionPattern");
		
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		
		//RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), DateUtil.getWeekStartDate(-1), Constants.CALENDAR_WEEK);		
		
		String curWkStartDate = DateUtil.getWeekStartDate(-1);
		RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), curWkStartDate, Constants.CALENDAR_WEEK);

//		LocalDate recWeekStartDate = DateUtil.toDateAsLocalDate(curWkDTO.getStartDate());
		Date recWeekStartDate = DateUtil.toDate(curWkDTO.getStartDate());
		SimpleDateFormat nf = new SimpleDateFormat("MM-dd-yyy");
		String dateInLog = nf.format(recWeekStartDate);
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String catAndZoneNum = String.valueOf(locationId) + "_" + String.valueOf(productId);
		logPath = logPath + "/" + catAndZoneNum + "_" + dateInLog + "_" + timeStamp + ".log";

		Properties props = new Properties();
//		try {
//			InputStream configStream = getClass().getResourceAsStream("/log4j-pricing-engine.properties");
//			props.load(configStream);
//			configStream.close();
//		} catch (IOException e) {
//			System.out.println("Error: Cannot laod configuration file ");
//		}
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
	}*/
	
	/**
	 * Returns Connection from datasource
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));
		try{
			if(ds == null){
				logger.info("getDSConnection() - Initializing datasource...");
				initContext = new InitialContext();
				envContext  = (Context)initContext.lookup("java:/comp/env");
				ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
				logger.info("getDSConnection() - Initializing datasource is completed.");
			}
			connection = ds.getConnection();
		}catch(NamingException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(SQLException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(Exception ex){
			logger.error("Error when creating connection from datasource " + ex.toString());
		}
		return connection;
	}
	
	protected Connection getConnection(){
		return conn;
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection(){
		if(conn == null){
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	protected void setConnection(Connection conn){
		this.conn = conn;
	}
	
	/**
	 * Initializes connection
	 * @throws GeneralException 
	 */
	protected void initialize() throws GeneralException{
		setConnection();
		//setLog4jProperties(batchLocationId, batchProductId);
	}
	
}


