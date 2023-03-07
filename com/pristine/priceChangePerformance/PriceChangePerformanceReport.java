package com.pristine.priceChangePerformance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.priceChangePerformance.*;
import com.pristine.dto.priceChangePerformance.*;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PriceChangePerformanceReport {

	private static Connection conn = null;
	private static Logger logger = Logger.getLogger("PriceChangePerformanceReport");
	
	
	public PriceChangePerformanceReport(Connection connection )
	{
		this.conn=connection;
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j_PriceChangePerformance.properties");
		PropertyManager.initialize("analysis.properties");

		
		//objects
		PriceChangePerformanceServices PCPService = new PriceChangePerformanceServices();
		CommonTaskDAO CTDAO = new CommonTaskDAO();
		IMSDataDAO ImsDataDao = new IMSDataDAO();
		TlogDataDAO TlogDataDao = new TlogDataDAO();
		PriceDataDAO PriceDataDao = new PriceDataDAO();
		
		//datasets
		List<TlogDataDTO> OldTlogData = new ArrayList<TlogDataDTO>();
		List<TlogDataDTO> NewTlogData = new ArrayList<TlogDataDTO>();
		List<TlogDataDTO> trendsOldData = new ArrayList<TlogDataDTO>();
		List<TlogDataDTO> trendsNewData = new ArrayList<TlogDataDTO>();
		//Map<Integer, Map<LocalDate, IMSDataDTO>> TotalIMSData = new HashMap();

		
		//inputs
		String productId =null;
		String productLevelId=null;
		String locationId =null;
		String locationLevelId=null;
		String strStartWeekDate=null ;
		String strEndWeekDate =null;
		String strPriceStartWeekDate =null;
		String strPriceEndWeekDate =null;
		
		for (String arg : args) {
			if (arg.startsWith("PRODUCT_ID=")) {
				productId = arg.substring("PRODUCT_ID=".length());
			}
			if(arg.startsWith("PRODUCT_LEVEL_ID=")) {
				productLevelId=arg.substring("PRODUCT_LEVEL_ID=".length());
			}
			if (arg.startsWith("LOCATION_ID=")) {
				locationId = arg.substring("LOCATION_ID=".length());
			}

			if(arg.startsWith("LOCATION_LEVEL_ID=")) {
				locationLevelId=arg.substring("LOCATION_LEVEL_ID=".length());
			}
			
			if (arg.startsWith("START_WEEK_DATE=")) {
				strStartWeekDate = arg.substring("START_WEEK_DATE=".length());
			}
			if(arg.startsWith("END_WEEK_DATE=")) {
				strEndWeekDate= arg.substring("END_WEEK_DATE=".length());
			}
		//	if(arg.startsWith("PRICE_START_WEEK_DATE=")) {
			//	strPriceStartWeekDate = arg.substring("PRICE_START_WEEK_DATE=".length());
				
		//	}

			//if(arg.startsWith("PRICE_END_WEEK_DATE=")) {
				//strPriceEndWeekDate = arg.substring("PRICE_END_WEEK_DATE=".length());
				
		//	}
		}
		
		String rootPath = PropertyManager.getProperty("NEW_PREDICTION.ROOTPATH", "");
		
		//converting to date to find last day date.
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		//monitoring dates
		LocalDate newStartWeekDate = LocalDate.parse(strStartWeekDate,dtf);
		LocalDate newEndWeekDate = LocalDate.parse(strEndWeekDate,dtf);
		//calculating the price dates
		LocalDate PriceStartWeekDate = newStartWeekDate.minusWeeks(2);
		LocalDate PriceEndWeekDate = PriceStartWeekDate;
		strPriceStartWeekDate = dtf.format(PriceStartWeekDate);
		strPriceEndWeekDate = dtf.format(PriceEndWeekDate);
		
		//actual report dates 3 weeks ahead as store may take upto 3 weeks to implement new prices
		 newStartWeekDate = newStartWeekDate.plusWeeks(3); 
		 newEndWeekDate = newEndWeekDate;
		
		LocalDate newStartDayDate = newStartWeekDate;
		LocalDate newEndDayDate = newEndWeekDate.plusDays(6);
		
		LocalDate oldStartWeekDate = newStartWeekDate.minusDays(364);
		LocalDate oldEndWeekDate = newEndWeekDate.minusDays(364);
		LocalDate oldStartDayDate = oldStartWeekDate;
		LocalDate oldEndDayDate = oldEndWeekDate.plusDays(6);
		
		
		
	
		
		logger.info("Genertaing report for product Id="+productId+" location Id="+locationId );
		
		
		try {
			
			
			// Creating Connection
			int iterations = 0;
			int ItemCount = 0;
			conn = DBManager.getConnection();
			logger.info("Getting Item Codes from product ID" );
			System.out.println("Getting Item Codes from product ID");
			//Getting Item Codes from product ID
			
			Map<Integer,ItemInfoDTO> iteminfo = CTDAO.getItemInfo(Integer.parseInt(productId),conn); 
			Map<Long,ItemInfoDTO> ligInfo = CTDAO.getLigInfo(Integer.parseInt(productId),conn); 

			//Sorting Unique Itemcodes
			List<Integer> UniqueItemCodes = PCPService.GetUniqueItemCodes(iteminfo);
			
			
			logger.info("Retrieved Unique Item Codes" );
			logger.info("Retrieved "+UniqueItemCodes.size()+" Unique Item Codes And LIG : "+ligInfo.size());
			
			ItemCount = UniqueItemCodes.size();
			if((ItemCount%500)==0) {
				iterations = ItemCount/500;
				
			}
			else {
				iterations = (ItemCount/500)+1;
			}
			logger.info(iterations+" iterations are required." );
			
			
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//GetOldData Start

			logger.info("Getting Old Data " );
			System.out.println("Getting Old Data ");
			//Main Loop to Group Items in small Groups and retrieve old data and new data
			for(int i=0 ; i<iterations ; i++ ) {
				System.out.println("________________________________________________________________________________");
				System.out.println("Iteration Number : "+ (i+1));
				
				//List<IMSDataDTO> OldIMSData = new ArrayList();
				Map<Integer, Map<LocalDate, IMSDataDTO>> OldIMSData = new HashMap();
				//List<PriceDataDTO> PriceData = new ArrayList();
				Map<Integer, PriceDataDTO> PriceData = new HashMap();
				
				String ItemCodeString= new String();
				int from = i*500;
				int to = (i+1)*500>ItemCount?ItemCount:((i+1)*500);
				ItemCodeString = PCPService.CreateItemCodeString(from, to, UniqueItemCodes);
				//logger.info("Item Codes : "+ItemCodeString);
				//Get Price Data 
				PriceData = (PriceDataDao.GetPriceData(conn, ItemCodeString, strPriceStartWeekDate, strPriceEndWeekDate, locationId));
				
				//Get Last Years Data
				//IMS Data OLD
				OldIMSData = ImsDataDao.getIMSData(conn, ItemCodeString, locationId, dtf.format(oldStartWeekDate),dtf.format(oldEndWeekDate));
				
				
				
				//Get Transition Data LOG OLD
				OldTlogData.addAll(TlogDataDao.GetTlogData(conn, "OLD",ItemCodeString, productId,productLevelId, locationId,locationLevelId, dtf.format(oldStartDayDate), dtf.format(oldEndDayDate),PriceData,iteminfo, OldIMSData));
				System.out.println("Current Old Tlog Count : "+OldTlogData.size());
				
			}
			conn.close();
			
			
			//filterOLDTlog Data
			OldTlogData=PCPService.FilterTlogOnCustType(OldTlogData);
			System.out.println("Current Old Tlog Count After Filter by cust Type: "+OldTlogData.size());
			
			//OldTlogData = PCPService.FilterTlogOnNegativeUnitPrice( OldTlogData);
			//System.out.println("Current New Tlog Count After Filter on Unit Price :" +OldTlogData.size());
			
			OldTlogData = PCPService.AddOtherPerc(OldTlogData);
			System.out.println("Current Old Tlog Count After AddOther Perc : "+OldTlogData.size());
			
			//Removing Duplicate data
			OldTlogData = PCPService.SortUniqueData(OldTlogData,"OLD",true,false);
			System.out.println("Current Old Tlog Count After Removing duplicates 1st time: "+OldTlogData.size());
			
			OldTlogData = PCPService.AggregateTlogBySum(OldTlogData);
			System.out.println("Current Old Tlog Count After Aggregate Sum: "+OldTlogData.size());
			
			//Removing Duplicate data again
			OldTlogData = PCPService.SortUniqueData((OldTlogData),"OLD",true,false);
			System.out.println("Current Old Tlog Count After Removing duplicates 2nd time : "+OldTlogData.size());
			
			logger.info("Finished Getting Old Data " );
			System.out.println("Finished Getting Old Data ");
			///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			logger.info("Getting New Data " );
			System.out.println("Getting New Data ");
			//Start After Data
			conn = DBManager.getConnection();
			//TotalIMSData.clear();
			for(int i=0 ; i<iterations ; i++ ) {
				System.out.println("________________________________________________________________________________");
				System.out.println("New Data Iteration Number : "+ (i+1));
				
				//List<IMSDataDTO> NewIMSData = new ArrayList<IMSDataDTO>();
				Map<Integer, Map<LocalDate, IMSDataDTO>> NewIMSData = new HashMap();

				String ItemCodeString= new String();
				int from = i*500;
				int to = (i+1)*500>ItemCount?ItemCount:(i+1)*500;
				ItemCodeString = PCPService.CreateItemCodeString(from, to, UniqueItemCodes);
				
				 
				
				
				//Get Curr Years Data
				//IMS Data OLD
				NewIMSData = ImsDataDao.getIMSData(conn, ItemCodeString, locationId, dtf.format(newStartWeekDate),dtf.format(newEndWeekDate));
				
				//Get Transition Data LOG OLD
				NewTlogData.addAll(TlogDataDao.GetTlogData(conn, "NEW",ItemCodeString, productId,productLevelId,locationId,locationLevelId, dtf.format(newStartDayDate), dtf.format(newEndDayDate),null ,iteminfo,NewIMSData));
				System.out.println("Current New Tlog Count : "+NewTlogData.size());
				
			}
			conn.close();
			//commented as merged at fetch time only
			//Merging IMS and Tlog Data
			//NewTlogData = PCPService.mergeTlogAndIMS(TotalIMSData,NewTlogData);
			
			//filterNEWTlog Data
			NewTlogData=PCPService.FilterTlogOnCustType(NewTlogData);
			System.out.println("Current New Tlog Count After Filter on Cust Type : "+NewTlogData.size());
			
			//NewTlogData = PCPService.FilterTlogOnNegativeUnitPrice( NewTlogData);
			//System.out.println("Current New Tlog Count After Filter on Unit Price :" +NewTlogData.size());
			
			NewTlogData = PCPService.AddOtherPerc(NewTlogData);
			System.out.println("Current New Tlog Count After Adding Other Perc: "+NewTlogData.size());

			//Removing Duplicate data
			NewTlogData = PCPService.SortUniqueData(NewTlogData,"NEW",true,false);
			System.out.println("Current New Tlog Count After Removing Duplicates 1st time: "+NewTlogData.size());

			HashMap<Integer, HashMap<LocalDate, Float>> newRecom = null ;
			//Add CSV Code Here
			try {
					newRecom = PCPService.readPredictionsFile(rootPath, productId,locationId, newStartWeekDate, newEndWeekDate,ligInfo,iteminfo);
			} catch (Exception e) {
				logger.error("Error While reading Prediction file: "+e.getMessage());
				System.out.println("Error While reading Prediction file: "+e.getMessage());
				e.printStackTrace();
			} 
			logger.info("RECOM COUNT: "+newRecom.size());
			NewTlogData = PCPService.AddNewPredData(newRecom, NewTlogData);
			System.out.println("Current New Tlog Count After Adding new recom : "+NewTlogData.size());

			
			NewTlogData = PCPService.AggregateTlogBySum(NewTlogData);
			System.out.println("Current New Tlog Count After Aggregate by sum: "+NewTlogData.size());

			//Removing Duplicate data again
			NewTlogData = PCPService.SortUniqueData(NewTlogData,"NEW",true,true);
			System.out.println("Current New Tlog Count 2nd time duplictes removal: "+NewTlogData.size());
			
			
			//End of getting new data 

			logger.info("Completed Getting New Data " );
			System.out.println("Completed Getting New Data ");
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			

			OldTlogData= PCPService.RemoveFutureWeeksData(OldTlogData,NewTlogData,oldStartWeekDate);
			
			//Group By UPCs
			logger.info("Old Data count before filtering MulUPC:"+OldTlogData.size() );
			OldTlogData = PCPService.GroupItemsWithMultiUPCRetItemCode(OldTlogData,"OLD");
			logger.info("Old Data count after filtering MulUPC:"+OldTlogData.size() );
			
			//Group By UPCs
			logger.info("New Data count before filtering MulUPC:"+NewTlogData.size() );
			NewTlogData = PCPService.GroupItemsWithMultiUPCRetItemCode(NewTlogData,"NEW");
			logger.info("New Data count after filtering MulUPC:"+NewTlogData.size() );
			
			//Rolling Up the Data to LIG LEVEL OLD and NEW
			Map<Long,Map<LocalDate,List<Integer>>> WeeklyLigMembers = PCPService.identifyCommonWeeklyLigMembers(OldTlogData,NewTlogData,ligInfo,iteminfo);
			
			OldTlogData = PCPService.FilterWeeklyLigMembers("OLD",OldTlogData,WeeklyLigMembers,ligInfo,iteminfo);
			
			NewTlogData = PCPService.FilterWeeklyLigMembers("NEW",NewTlogData,WeeklyLigMembers,ligInfo,iteminfo);
			
			
			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//Part to consider latest price 
			
			NewTlogData = PCPService.FilterNonPriceChangedWeeks(NewTlogData);
			
			
			
			
			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			//Rolling Up To Lig Level
			NewTlogData = PCPService.RollupToLIGLevel("NEW",ligInfo,iteminfo,NewTlogData);
			
			//Backing up the Trends Data
			trendsNewData=PCPService.CopyListToTrends("NEW",NewTlogData);
			
			//write To csv
			PCPService.writeTlogToCSV("NEW",NewTlogData,productId,locationId);
			
			//Rolling Up To Lig Level
			OldTlogData = PCPService.RollupToLIGLevel("OLD",ligInfo,iteminfo,OldTlogData);
			
			//Backing up the Trends Data
			trendsOldData=PCPService.CopyListToTrends("OLD",OldTlogData);
			
			PCPService.writeTlogToCSV("OLD",OldTlogData,productId,locationId);
			//Aggregate Tlog Data by mean
			OldTlogData = PCPService.AggregateTlogByMean(OldTlogData,"OLD");
			System.out.println("Current Old Tlog Count After mean: "+OldTlogData.size());
			
			OldTlogData = PCPService.SortUniqueData((OldTlogData),"OLD",false,false);
			System.out.println("Current Old Tlog Count Last Removing Duplicates: "+OldTlogData.size());
			
			NewTlogData = PCPService.AggregateTlogByMean(NewTlogData,"NEW");
			System.out.println("Current New Tlog Count After Aggregate by mean: "+NewTlogData.size());
			
			//Removing Duplicate data
			NewTlogData = PCPService.SortUniqueData(NewTlogData,"NEW",false,true);
			System.out.println("Current New Tlog Count Afte last uplicate removal: "+NewTlogData.size());

			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			//Combining two data
			List<FinalDataDTO> FinalData= PCPService.CombineOldAndNewData(OldTlogData,NewTlogData);
			System.out.println("Current Final Data Count : "+FinalData.size());
		
			FinalData = PCPService.DecideLIGRegPriceBasedOnPerformance(FinalData,ligInfo,iteminfo);
			
			//overwritting the tier units sales and revenue using the IMS DATA
			FinalData = PCPService.CalculateTiersMatrices(FinalData);
			
			//Calculating Difference Metrices
			FinalData = PCPService.CalculateDiffMetrics(FinalData);
			System.out.println("Current Final Data Count after calculate diff: "+FinalData.size());

			
			
			//Mark The Anomalous items
			FinalData = PCPService.MarkAnomalousItems(FinalData);
			System.out.println("Current Final Data Count after mark anomalous: "+FinalData.size());
			
			
			//Writing Result to CSV
			PCPService.writeToCSV(FinalData, productId, locationId);
			
			
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//Calculating Graph Data
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			Map<Integer,WeekGraphDataDTO> All = new HashMap();
			Map<Integer,WeekGraphDataDTO> Decrease = new HashMap();
			Map<Integer,WeekGraphDataDTO> Increase = new HashMap();
			
			newRecom = PCPService.rollUpPredDataToMulUPC(newRecom, iteminfo,ligInfo,FinalData);
			int countIncrease[] = {0};
			int countDecrease[] = {0};
			FinalData.forEach(f->{
					if(f.isToConsiderRecord()&&!f.isLigMember()) {
						if(f.getPriceChangeIndicator()==2) {
							countIncrease[0]++;
						}else if(f.getPriceChangeIndicator()==3) {
							countDecrease[0]++;
						}
					}
			});
			int totalCount = countIncrease[0]+countDecrease[0];
			logger.info("Total Items:"+totalCount);
			logger.info("Inc Items:"+countIncrease[0]);
			logger.info("Dec Items:"+countDecrease[0]);

			if(totalCount!=0) {
			conn = DBManager.getConnection();

			All = PCPService.getGraphdata(1,FinalData,trendsNewData,trendsOldData,newRecom,conn);
			if(countDecrease[0]!=0) {
			Decrease = PCPService.getGraphdata(3,FinalData,trendsNewData,trendsOldData,newRecom,conn);
			}
			if(countIncrease[0]!=0) {
			Increase=PCPService.getGraphdata(2,FinalData,trendsNewData,trendsOldData,newRecom,conn);
			}			
			conn.close();
			
			PCPService.writeGraphDataToCSV(1,All, productId, locationId);
			if(countIncrease[0]!=0) {
			PCPService.writeGraphDataToCSV(2,Increase, productId, locationId);
			}
			if(countDecrease[0]!=0) {
			PCPService.writeGraphDataToCSV(3,Decrease, productId, locationId);
			}
			
			
			
			
			
			CatContriSummaryBO IncreasedSummary=null;
			CatContriSummaryBO DecreasedSummary=null;
			CatContriSummaryBO AllSummary = PCPService.getCategoryContributionSummary(1,All,FinalData);
			if(countIncrease[0]!=0) {
			 IncreasedSummary = PCPService.getCategoryContributionSummary(2,Increase,FinalData);
			}
			if(countDecrease[0]!=0) {
				DecreasedSummary = PCPService.getCategoryContributionSummary(3,Decrease,FinalData);
			}
			
			CatForecastAccBO IncFAcc =null;
			CatForecastAccBO DecFAcc =null;
			CatForecastAccBO AllFAcc = PCPService.getForecastAccuracy(1,FinalData);
			if(countIncrease[0]!=0) {
			 IncFAcc = PCPService.getForecastAccuracy(2,FinalData);
			}
			if(countDecrease[0]!=0) {
			 DecFAcc = PCPService.getForecastAccuracy(3,FinalData);
			}
			
			///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//Writting Data to DB 
			///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//Commented code is used to insert the data into test DB
			logger.info("Inserting Report to Data Base ");
	/*		///////////////////////////////////////////////////////////////////
			String db_connect_string = PropertyManager.getProperty("DB_CONNECT_STRING_2");
			String db_userid = PropertyManager.getProperty("DB_USER_2");
			String db_password = PropertyManager.getProperty("DB_PASSWORD_2");
			
			logger.info("db connection String = " + db_connect_string);
			logger.info("db User Id = " + db_userid);
			//logger.debug("db Password = " + db_password);
			
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
			Connection conn2 = DriverManager.getConnection(db_connect_string, db_userid, db_password);
			conn2.setAutoCommit(false);
			logger.debug("connected to database");
		//*/
			////////////////////////////////////////////////////////////////////
			conn=DBManager.getConnection();
			
			Map<LocalDate,Integer>CalendarIds =PCPService.getWeekCalendarIds(conn,strStartWeekDate,strEndWeekDate);
			
			PCPService.WriteDataToDB(conn,productId,productLevelId,locationId,locationLevelId,ligInfo,dtf.format(newStartWeekDate),dtf.format(newEndWeekDate),dtf.format(PriceStartWeekDate),dtf.format(PriceEndWeekDate),FinalData,trendsNewData,trendsOldData,All,Increase,Decrease,AllSummary,IncreasedSummary,DecreasedSummary,AllFAcc,IncFAcc,DecFAcc,CalendarIds,totalCount,countIncrease[0],countDecrease[0]);
			
			}
		} catch (GeneralException | Exception e) {
			
			e.printStackTrace();
		}finally {
			
			PristineDBUtil.close(conn);
		}
		
		
		
		
		

	}
	
	

}
