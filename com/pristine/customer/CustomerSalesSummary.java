package com.pristine.customer;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.business.entity.CustomerSegment;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.customer.CustomerSalesSummaryDAO;
import com.pristine.dao.customer.CustomerVisitSummaryDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.customer.CustomerVisitSummaryDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CustomerSalesSummary {
	
private static Logger logger = Logger.getLogger("CustomerSalesSummary");
	
	private  Connection	conn = null;
	private RetailCalendarDAO calDao;
	private CustomerSalesSummaryDAO custSalesDAO;
	private CustomerVisitSummaryDAO custVisitDAO;
	private CompStoreDAO compDAO;	
	private List<CustomerVisitSummaryDTO> list = null;
	private CustomerSegment custSeg;
	
	Double dbPrice = 0D;
	Double salePrice = 0D;
	Double regPrice = 0D;
	Double saleMov = 0D;
	Double regMov = 0D;
	Double margin = 0D;
	Double salemargin = 0D;
	Double regmargin = 0D;
	Double totmov = 0D;
	String item;
	int transCount = 0;
	
	public CustomerSalesSummary(){
		try{
			PropertyManager.initialize("analysis.properties");
			conn = DBManager.getConnection();
			conn.setAutoCommit(false);
			calDao = new RetailCalendarDAO();
			custSalesDAO = new CustomerSalesSummaryDAO();
			compDAO = new CompStoreDAO();			
			custVisitDAO = new CustomerVisitSummaryDAO();
			custSeg = new CustomerSegment();
		}catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	public static void main(String args[]){
		PropertyConfigurator.configure("log4j-customer-sales-summary.properties");
		logger.info("*** Customer Sales Summary begins ***");
		logCommand(args);		
		Date endDate = null;
		String storeno = "";
		String period = "";
		CustomerSalesSummary cps = null;
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");		
		
		try{
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
					
				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} 
					catch (ParseException par) {
						logger.error("End Date Parsing Error, checkInput");
					}
				}
			
				if (arg.startsWith("PERIOD")) {
					period = arg.substring("PERIOD=".length());
				}
			
				if (arg.startsWith("STORE")) {
					storeno = arg.substring("STORE=".length());
				}
			}
			
			cps = new CustomerSalesSummary();
			cps.calculateCustomerSalesSummary(storeno,period,endDate);
		
		}catch ( ParseException pe) {
			logger.error(pe);
		}catch ( SQLException se) {
			logger.error(se);
		} catch ( GeneralException ge) {
			logger.error(ge);
		} catch ( Exception e) {
		logger.error(e);
		}		
		finally {
			PristineDBUtil.close(cps.conn);
			logger.info("*** Customer Sales Summary ends ***");
		}
	}
	
	public void calculateCustomerSalesSummary(String storeno, String period, 
						Date endDate) throws GeneralException, Exception{
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date strDate;		
		RetailCalendarDTO calDto = calDao.week_period_quarter_yearCalendarList(conn, endDate, period);
		String startDate = calDto.getStartDate();
		if(startDate!=null)
		 strDate = dateFormat.parse(startDate);
		else
			strDate = endDate;
		
		List<RetailCalendarDTO> listCalDto = calDao.dayCalendarList(conn, strDate, endDate, "D");
		CompetitiveDataDTO dto = compDAO.getCompeStoreDetails(conn, "", storeno);
		int storeId = dto.getCompStrID();
		String strcalList = "(";
		for(RetailCalendarDTO r: listCalDto){
			if(strcalList.equals("("))
			strcalList = strcalList + r.getCalendarId();
			else
				strcalList = strcalList +" , "+ r.getCalendarId();
		}
		
		strcalList = strcalList + " ) ";
		
		logger.info("Populate Customer Sales summary data");
		logger.debug("Delete Customer Sales Summary data");
		custSalesDAO.deleteCustomerSegmentData(conn, storeId, calDto.getCalendarId());
		
		logger.debug("Delete Customer Segment data");
		custSalesDAO.deleteCustomerSalesSummary(conn, storeId, calDto.getCalendarId());
		
		list = populateCustomerSalesSummary(conn,storeId,calDto.getCalendarId(),strcalList);
		logger.debug("Total Customers visited store:" + list.size());
		
		if (list.size() > 0){
		
			logger.debug("Insert Customer Sales Summary");
			custSalesDAO.insertCustomerSalesSummary(conn, list);

			logger.info("Populate Customer Segment data");
			logger.debug("Calculate Customer Segment"); 
			custSeg.populateSegmentData(endDate, storeId, calDto.getCalendarId(), conn);
		}
		else{
			logger.error("No customer summary data found");
		}
			
		
	}
	
	//@SuppressWarnings("unused")
	public ArrayList<CustomerVisitSummaryDTO> populateCustomerSalesSummary(
				Connection conn, int storeId, int calid, String strcalList) 
										throws GeneralException,Exception {
		
		ArrayList<CustomerVisitSummaryDTO> stores = new ArrayList<CustomerVisitSummaryDTO>();				
		
		logger.debug("Fetch customer visit summary data");
		CachedRowSet result = custVisitDAO.getCustomerVisitSummary(conn, storeId, strcalList);
		
		if (result.size() > 0 ) {

			String txn;	
			Double customerId = 0.0;
			
			CustomerVisitSummaryDTO dto = new CustomerVisitSummaryDTO();						
			
			DecimalFormat twoDForm = new DecimalFormat("#.##");
			
			dto = new CustomerVisitSummaryDTO();
			while (result.next()) {
				//result.getString("CUSTOMER_ID");
				if ((customerId == 0)
						|| (customerId > 0 && customerId == result
								.getDouble("CUSTOMER_ID"))) {
					if (customerId == 0) {
						txn = result.getString("TRX_NO");

						customerId = result.getDouble("CUSTOMER_ID");
						if (customerId > 0)
							dto.setCustomerid(customerId);
						else
							dto.setCustomerid(null);
						dto.setComp_str_no(new Double(storeId));
						dto.setCalendarid(new Double(calid));
						dto.setTransactionno(new Double(txn));
					}
					populateValues(result);
					customerId = result.getDouble("CUSTOMER_ID");
					continue;
				} else {

					dbPrice = Double.valueOf(twoDForm.format(dbPrice));
					salePrice = Double.valueOf(twoDForm.format(salePrice));
					regPrice = Double.valueOf(twoDForm.format(regPrice));
					totmov = Double.valueOf(twoDForm.format(totmov));
					saleMov = Double.valueOf(twoDForm.format(saleMov));
					regMov = Double.valueOf(twoDForm.format(regMov));
					salemargin = Double.valueOf(twoDForm.format(salemargin));
					regmargin = Double.valueOf(twoDForm.format(regmargin));
					margin = Double.valueOf(twoDForm.format(margin));

					dto.setTotrevenue(dbPrice);
					dto.setSalerevenue(salePrice);
					dto.setRegrevenue(regPrice);
					dto.setTotmovement(totmov);
					dto.setSalemovement(saleMov);
					dto.setRegmovement(regMov);
					dto.setSalemargin(salemargin);
					dto.setRegmargin(regmargin);
					dto.setTotmargin(margin);
					dto.setTransactionCount(transCount);

					stores.add(dto);

					dbPrice = 0D;
					salePrice = 0D;
					regPrice = 0D;
					totmov = 0D;
					saleMov = 0D;
					regMov = 0D;
					margin = 0D;
					salemargin = 0D;
					regmargin = 0D;
					transCount = 0;

					dto = new CustomerVisitSummaryDTO();
					txn = result.getString("TRX_NO");

					customerId = result.getDouble("CUSTOMER_ID");
					if (customerId > 0)
						dto.setCustomerid(customerId);
					else
						dto.setCustomerid(null);
					dto.setCustomerid(customerId);
					dto.setComp_str_no(new Double(storeId));
					dto.setCalendarid(new Double(calid));
					dto.setTransactionno(new Double(txn));

					populateValues(result);
				}
			}
			dbPrice = Double.valueOf(twoDForm.format(dbPrice));
			salePrice = Double.valueOf(twoDForm.format(salePrice));
			regPrice = Double.valueOf(twoDForm.format(regPrice));
			totmov = Double.valueOf(twoDForm.format(totmov));
			saleMov = Double.valueOf(twoDForm.format(saleMov));
			regMov = Double.valueOf(twoDForm.format(regMov));
			salemargin = Double.valueOf(twoDForm.format(salemargin));
			regmargin = Double.valueOf(twoDForm.format(regmargin));
			margin = Double.valueOf(twoDForm.format(margin));
			dto.setTotrevenue(dbPrice);
			dto.setSalerevenue(salePrice);
			dto.setRegrevenue(regPrice);
			dto.setTotmovement(totmov);
			dto.setSalemovement(saleMov);
			dto.setRegmovement(regMov);
			dto.setSalemargin(salemargin);
			dto.setRegmargin(regmargin);
			dto.setTotmargin(margin);
			dto.setTransactionCount(transCount);
			stores.add(dto);
		}
		return stores;
	}
	
	public void populateValues(CachedRowSet result) 
									throws GeneralException,SQLException{
		
		dbPrice = dbPrice + new Double(result.getString("TOT_REVENUE"));		
		salePrice = salePrice + new Double(result.getString("SALE_REVENUE"));
		regPrice = regPrice + new Double(result.getString("REG_REVENUE"));
		totmov =  totmov + new Double(result.getString("TOT_MOVEMENT"));
		saleMov = saleMov + new Double(result.getString("SALE_MOVEMENT"));
		regMov = regMov + new Double(result.getString("REG_MOVEMENT"));
		margin =  margin + new Double(result.getString("TOT_MARGIN"));
		salemargin = salemargin + new Double(result.getString("SALE_MARGIN"));
		regmargin = regmargin + new Double(result.getString("REG_MARGIN"));
		transCount = transCount + 1;
											
	}

	private static void logCommand (String[] args)
	{
		StringBuffer sb = new StringBuffer("Command: CustomerSalesSummary ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
}
