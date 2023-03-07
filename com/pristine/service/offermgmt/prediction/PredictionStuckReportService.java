package com.pristine.service.offermgmt.prediction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dto.offermgmt.prediction.StuckRunDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.PropertyManager;

public class PredictionStuckReportService {
	
	private static final Logger logger = Logger.getLogger("PredictionStuckReportService");
	
	private static final String header = "RUN_ID,RUN_TYPE,RUN_STATUS,PROGRESS_PERCENT,"
			+ "START_TIME,STARTED_BY,USER_ID,PRODUCT,PRODUCT_ID,LOCATION,LOCATION_ID,MESSAGE";
	
	private static final String emailSubject = "Stuck runs.";
	
	private static final String emailBody = "Please find the attached report on recommendations that have been running for over %d hours.";
	
	private static final String getStuckRuns = "SELECT " + 
			"pqrh.run_id, " + 
			"pqrh.run_type, " + 
			"pqrh.run_status, " + 
			"pqrh.percent_completion                               AS progress_percent, " + 
			"to_char(pqrh.start_run_time, 'MM/DD/YYYY HH24:MI:SS') AS start_time, " + 
			"concat(ud.first_name, concat(' ', ud.last_name))      AS started_by, " + 
			"ud.user_id, " + 
			"pg.name                                               AS product, " + 
			"pqrh.product_id, " + 
			"rpz.name                                              AS location, " + 
			"pqrh.location_id, " + 
			"pqrh.message  " + 
			"FROM " + 
			"pr_quarter_rec_header pqrh, " + 
			"product_group         pg, " + 
			"retail_price_zone     rpz, " + 
			"user_details          ud  " + 
			"WHERE " + 
			"pqrh.predicted_by = ud.user_id " + 
			"AND pqrh.product_id = pg.product_id " + 
			"AND pqrh.location_id = rpz.price_zone_id " + 
		    "AND pqrh.run_status IS NULL " + 
			"AND pqrh.start_run_time < ( sysdate - ( 1 / 24 ) * ? ) " + 
		    "%s" + 
			"AND ( pqrh.percent_completion <> 100 OR pqrh.end_run_time IS NULL ) " + 
			"ORDER BY " + 
			"pqrh.start_run_time DESC";
	private static final String historyConditionTemplate = "AND pqrh.start_run_time > ( sysdate - %s ) ";
	
	private static Connection connection;
	private static int runTimeThreshold;
	private String reportPath;
	private static String emailFromAddress, historyCondition;
	private static List<String> runPolice;
	
	
	PredictionStuckReportService() {
		logger.info("Reading the .properties file ");
		logger.debug("recommendation.properties");
		PropertyManager.initialize("recommendation.properties");
		logger.info("Properties initialized");
		logger.debug("from recommendation.properties");
		PropertyConfigurator.configure("log4j.properties");
		
		if (connection == null) {
			try {
				connection = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB: " + exe);
				System.exit(1);
			}
		}
		
		reportPath = PropertyManager.getProperty("STUCK_RUNS_REPORT_PATH");
		runTimeThreshold = Integer.parseInt(PropertyManager.getProperty("RUNTIME_THRESHOLD_HOURS", "3"));
		String historyThreshold = PropertyManager.getProperty("RUNHISTORY_THRESHOLD_DAYS", "0");
		historyCondition = Integer.parseInt(historyThreshold) > 0 ? String.format(historyConditionTemplate, historyThreshold) : "";
		String theRunPolice = PropertyManager.getProperty("RUN_POLICE", "ramabadran.rajagopal@pristineinfotech.com");
		String[] runPoliceArr = theRunPolice.split(",");
		runPolice = Arrays.asList(runPoliceArr);
		emailFromAddress = PropertyManager.getProperty("MAIL.FROMADDR");
		
	}

	public static void main(String[] args) {
		
		PredictionStuckReportService stuckRunsReport = new PredictionStuckReportService();
		List<StuckRunDTO> stuckRuns = stuckRunsReport.getStuckRuns(connection, runTimeThreshold, historyCondition);
		try {
			connection.close();
		} catch (SQLException e1) {
			logger.error("Error while closing the connection to the DB!", e1);
			System.exit(3);
		}
		if(stuckRuns.isEmpty())
			logger.info("No runs have been acitve for over 3 hours. No need to send the notification email.");
		else {
			List<String> emailAttachmentPath = new ArrayList<String>();
			String fullyQualifiedReportName = stuckRunsReport.writeStuckRunDTOListtoCSV(stuckRuns);
			emailAttachmentPath.add(fullyQualifiedReportName);
			try {
				EmailService.sendEmail(emailFromAddress, runPolice, null, null, emailSubject, String.format(emailBody, runTimeThreshold), emailAttachmentPath);
			} catch (GeneralException e) {
				logger.error("Error while sending the email!", e);
				System.exit(5);
			}
		}
	}
	
	/**
	 * Retrieves information about such runs that have been triggered 'threshold' hours ago and have NOT yet completed.
	 * 
	 * @return a list of StuckRunDTO objects. The StuckRunDTO object contains the run id, run type, run status, progress 
	 * percentage, start time, name and id of the user who triggered it, the name and id of the product/ru it was triggered for, 
	 * the name and id of the location it was triggered for, and any message recorded by the run.
	 */
	public List<StuckRunDTO> getStuckRuns(Connection connection, int threshold, String historyCondition){
		List<StuckRunDTO> result = new ArrayList<StuckRunDTO>();
		try {
			String query = String.format(getStuckRuns, historyCondition);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, threshold);
			ResultSet resultSet = preparedStatement.executeQuery();
			while(resultSet.next()) {
				StuckRunDTO stuckRun = new StuckRunDTO();
				stuckRun.setRunId(resultSet.getLong("RUN_ID"))
				.setRunType(resultSet.getString("RUN_TYPE"))
				.setRunStatus(resultSet.getString("RUN_STATUS"))
				.setProgressPercent(resultSet.getInt("PROGRESS_PERCENT"))
				.setStartTime(resultSet.getString("START_TIME"))
				.setStartedBy(resultSet.getString("STARTED_BY"))
				.setUserId(resultSet.getString("USER_ID"))
				.setProduct(resultSet.getString("PRODUCT"))
				.setProductId(resultSet.getString("PRODUCT_ID"))
				.setLocation(resultSet.getString("LOCATION"))
				.setLocationId(resultSet.getInt("LOCATION_ID"))
				.setMessage(resultSet.getString("MESSAGE"));
				result.add(stuckRun);
			}
		} catch (SQLException e) {
			logger.error("Could NOT retrieve the stuck runs!", e);
			System.exit(2);
		}
		
		return result;
	}
	
	/**
	 * Takes a list of StuckRunDTO objects and writes it to a CSV file where each line represents data from one list element
	 * @param stuckRuns - a list of StuckRunDTO objects.
	 * @return a full name of the csv file containing all the data from stuckRuns
	 */
	String writeStuckRunDTOListtoCSV(List<StuckRunDTO> stuckRuns) {
		Date date = new Date();
		DateFormat format = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		String fqFileName = reportPath+"/stuck_runs_"+format.format(date)+".csv";
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fqFileName));
			bw.write(header);
			bw.newLine();
			for(StuckRunDTO stuckRun : stuckRuns) {
				bw.write(stuckRun.toString());
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
			logger.error("Error while creating the report file!", e);
			System.exit(4);
		}
		return fqFileName;
	}

}
