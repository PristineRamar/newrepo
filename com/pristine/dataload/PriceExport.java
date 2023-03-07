package com.pristine.dataload;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class PriceExport {
	private static Logger  logger = Logger.getLogger("PriceExport");

	OOSCalcsDAO ossCalcDao = new OOSCalcsDAO();
	RetailCalendarDAO calendarDao = new RetailCalendarDAO();
	Connection conn = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-price-export.properties");
		PropertyManager.initialize("analysis.properties");
 		Date startDate ;
 		String storeNum;
		if(args.length == 2){
			try {
				storeNum = args[0];
				startDate = DateUtil.toDate(args[1]);
				PriceExport pe = new PriceExport();

				pe.exportPrice(storeNum,args[1]);
			} catch (GeneralException e) {
				
				logger.info("Error exporting price:"+e.getMessage());
				System.exit(1);
			}
		}
		else{ 
			logger.info("Insufficient Arguments - PriceExport StoreNumber StartDate(MM/DD/YYYY) ");
			System.exit(1);
		}


	}
	
	public void exportPrice(String storeNum, String strDate) throws GeneralException{
		conn = DBManager.getConnection();

		RetailCalendarDTO c = calendarDao.getCalendarId(conn, strDate, Constants.CALENDAR_WEEK);
		int calendarId = c.getCalendarId();

		//Get Store Id
		StoreDAO strdao = new StoreDAO();
		int strId = strdao.getStoreID(conn, storeNum,null,-1);
		if( strId <= 0){
			throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
		}

		
		try{
		List<String> itemCodeList = new ArrayList<String>();
/*
		//Get the check list
		int checkListId = Integer.parseInt(PropertyManager.getProperty("PRICE_EXPORT.CHECKLIST_ID", "222"));
		if( checkListId <= 0){
			throw new GeneralException ("Invalid CheckList Id, please set the OOS_ANALYSIS.CHECKLIST_ID property " );
		}

		logger.info("Begin Load Price list");

		CachedRowSet crs = ossCalcDao.getNonLIGItemList(conn, checkListId);
*/
		CachedRowSet crs = ossCalcDao.getItemListForAnalysis(conn);
		
			while( crs.next()){
			Integer currentItemCode  = crs.getInt("ITEM_CODE");
			itemCodeList.add(currentItemCode.toString());
		}
		
//		itemCodeList.add("25771");  //for debug

		PriceDataLoad objPriceLoad = new PriceDataLoad();
		HashMap<String, RetailPriceDTO> priceDataMap = objPriceLoad
				.getRetailPrice(conn, storeNum,
						itemCodeList, calendarId, null, Constants.STORE_LEVEL_TYPE_ID);
		logger.info("End Load Price list");
		
		generateCSV(storeNum, strId, c, priceDataMap);
		
		}catch(Exception e){
			throw new GeneralException(e.getMessage());
		}

	}
	
	public void generateCSV(String storeNum, int strId, RetailCalendarDTO c,HashMap<String, RetailPriceDTO> priceDataMap) throws Exception{
		try {
			logger.info("Begin Generate CSV");
			String filename = "retail_price_"+storeNum+"_"+c.getCalendarId()+".csv";
			Set<String> keys = priceDataMap.keySet();
			String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
			ItemDAO itemDao = new ItemDAO();
			BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/"+filename)); 
			out.write("\"PRESTO_COMP_ID\",\"COMP_STR_NO\",\"ITEM_NAME\",\"UPC\",\"PRESTO_ITEM_CODE\",\"REG_PRICE\",\"REG_M_PACK\",\"REG_M_PRICE\",\"SALE_PRICE\",\"SALE_M_PACK\",\"SALE_M_PRICE\",\"PROMOTION_FLG\",\"SEGMENT_ID\",\"WEEK_START_DATE\",\"WEEK_END_DATE\"\r\n");
			SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			Date startDate = df.parse(c.getStartDate());
			Date endDate = df.parse(c.getEndDate());
			
			SimpleDateFormat nf = new SimpleDateFormat("dd-MMM-yy");
			
			for(String k:keys){
					RetailPriceDTO p = priceDataMap.get(k);
				try {
					int regMpack = (p.getRegQty()>1)?p.getRegQty():0;
					int saleMpack = (p.getSaleQty()>1)?p.getSaleQty():0;
					
					ItemDTO ilookup = new ItemDTO();
					ilookup.setItemCode( Integer.parseInt( p.getItemcode()));
					ItemDTO item = itemDao.getItemDetails(conn, ilookup);
					
					String iName = item.getItemName().replace("\"","\"\"");
					
					out.write(strId+",\""+ storeNum+"\",\""+iName+"\",\""+item.getUpc()+"\","+p.getItemcode()+","+p.getRegPrice()+","+regMpack+","+p.getRegMPrice()+","
					+p.getSalePrice()+","+saleMpack+","+p.getSaleMPrice()+",\""+p.getPromotionFlag()+"\"," +item.getSegmentID()+"," +nf.format(startDate)+"," +nf.format(endDate)+"\r\n");
				} catch (GeneralException   e) {
					e.printStackTrace();
				}catch ( Exception e) {
					e.printStackTrace();
				}
			}
			out.flush();
			out.close();
			logger.info("End Generate CSV");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage());
			
		}
	}

}
