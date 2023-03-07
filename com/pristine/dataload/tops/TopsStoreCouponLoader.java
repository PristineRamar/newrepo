package com.pristine.dataload.tops;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.TopsStoreCouponDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.StoreCouponDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TopsStoreCouponLoader
  extends PristineFileParser
{
  private static Logger logger = Logger.getLogger("TopsStoreCouponLoader");
  private static LinkedHashMap COUPON_FIELDS = null;
  private int rejectedCount = 0;
  private int errorCount = 0;
  private int processedCount = 0;
  private int recordCount = 0;
  private int ignoredRecords = 0;
  private Connection conn = null;
  private boolean isMultiCouponLoad = false;
  private String storeNum = null;
  private boolean filterByStore = false;
  private int calendarId = -1;
  private final int BATCH_INSERT_COUNT = 1000;
  private ItemDAO objItemDao = new ItemDAO();
  private ArrayList<StoreCouponDTO> insertCouponList = new ArrayList();
  private TopsStoreCouponDAO couponDao = new TopsStoreCouponDAO();
  RetailCalendarDAO objCal = new RetailCalendarDAO();
  private HashMap<String, Integer> itemCodeMap = new HashMap();
  private HashMap<String, Integer> calendarIdMap = new HashMap();
  private static int columnCount = 27;
//Changes to retrieve item_code using upc and retailer_item_code
	boolean checkRetailerItemCode = false;
	private HashMap<Integer, HashMap<String, Integer>> _itemHashMapStoreUpc = new HashMap<Integer, HashMap<String, Integer>>();
	private HashMap<String, Integer> compIdMap = new HashMap<String, Integer>();
	private List<String> noItemCodeUPCs = new ArrayList<String>();
  public static void main(String[] args)
  {
    PropertyConfigurator.configure("log4j-store-coupon.properties");
    if (args.length < 2)
    {
      logger.info("Invalid Arguments, args[0] should be relative path, store number/All and  [calendarId]");
      System.exit(-1);
    }
    TopsStoreCouponLoader couponLoader = new TopsStoreCouponLoader();
    couponLoader.processRecords(args);
  }
  
  private void processRecords(String[] args)
  {
    ArrayList<String> fileList = null;
    if ((args.length > 1) && 
      (!args[1].equalsIgnoreCase("All")))
    {
      this.storeNum = args[1];
      this.filterByStore = true;
    }
    if (args.length > 2) {
      if (args[2].equalsIgnoreCase("MULTI")) {
        this.isMultiCouponLoad = true;
      } else {
        this.calendarId = Integer.parseInt(args[2]);
      }
    }
    String relativePath = args[0];
    logger.info("Coupon processing started for Calendar " + this.calendarId);
    logger.info("Processing for Store " + this.storeNum);
    
    ArrayList<String> zipFileList = null;
    String zipFilePath = getRootPath() + "/" + relativePath;
    try
    {
      zipFileList = getZipFiles(relativePath);
    }
    catch (GeneralException ge)
    {
      logger.error("Error in setting up objects", ge);
      return;
    }
    int curZipFileCount = -1;
    boolean processZipFile = false;
    do
    {
      this.conn = getOracleConnection();
      boolean commit = true;
      try
      {
    	  compIdMap = new StoreDAO().getStoreIdMap(conn);
          checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
          if(checkRetailerItemCode && !this.isMultiCouponLoad){
				getAuthorizedItems();
			}
        if (processZipFile) {
          PrestoUtil.unzip((String)zipFileList.get(curZipFileCount), zipFilePath);
        }
        fileList = getFiles(relativePath);
        for (int i = 0; i < fileList.size(); i++)
        {
          String files = (String)fileList.get(i);
          logger.info("Processing file " + files);
          int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
          

          String[] fieldNames = new String[27];
          
          fieldNames[0] = "company";
          fieldNames[1] = "store";
          fieldNames[2] = "terminal";
          fieldNames[3] = "transactionNo";
          fieldNames[4] = "operator";
          fieldNames[5] = "cpnDate";
          fieldNames[6] = "cpnTime";
          fieldNames[7] = "customerId";
          fieldNames[8] = "couponNumber";
          fieldNames[9] = "itemUPC";
          fieldNames[10] = "itemNetPrice";
          fieldNames[11] = "posDept";
          fieldNames[12] = "cpnFamilyCurr";
          fieldNames[13] = "cpnFamilyPrev";
          fieldNames[14] = "cpnMfgNbr";
          fieldNames[15] = "strCpnQty";
          fieldNames[16] = "strCpnWeight";
          fieldNames[17] = "cpnCnt";
          fieldNames[18] = "discSalePrcAmt";
          fieldNames[19] = "discTaxExempt";
          fieldNames[20] = "discSalePrice";
          fieldNames[21] = "discQty";
          fieldNames[22] = "cpnDiscGrp";
          fieldNames[23] = "cpnDiscPct";
          fieldNames[24] = "couponType";
          fieldNames[25] = "cpnNotUsed";
          fieldNames[26] = "cpnMult";
          String[] fileFields = mapFileField(fieldNames, columnCount);
          
          logger.info("Processing Coupon Records ...");
          super.parseDelimitedFile(StoreCouponDTO.class, files, '|', fileFields, stopCount);
          
          logger.info("No of records = " + this.recordCount);
          logger.info("No of records Ignored = " + this.ignoredRecords);
          
          logger.info("No of records Rejected = " + this.rejectedCount);
          logger.info("No of records Accepted = " + this.processedCount);
          logger.info("No of records in error = " + this.errorCount);
          if(noItemCodeUPCs.size() > 0){
          Iterator<String> it = noItemCodeUPCs.iterator();
			int ii = 0;
			StringBuffer sb = new StringBuffer("processRecords() - UPCs for which item code not found: ");
			while (it.hasNext()) {
			    String store = it.next();
				if ( ii > 0 ) sb.append(',');
				sb.append(store);
				ii++;
			}
			logger.info(sb);
          }
          
          
        }
      }
      catch (GeneralException ex)
      {
        logger.error("GeneralException", ex);
        commit = false;
      }
      catch (Exception ex)
      {
        logger.error("JavaException", ex);
        commit = false;
      }
      try
      {
        if ((PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")) && (commit))
        {
          logger.info("Committing transacation");
          PristineDBUtil.commitTransaction(this.conn, "Data Load");
          if (processZipFile)
          {
            PrestoUtil.deleteFiles(fileList);
            fileList.clear();
            fileList.add((String)zipFileList.get(curZipFileCount));
          }
          String archivePath = getRootPath() + "/" + relativePath + "/";
          PrestoUtil.moveFiles(fileList, archivePath + "CompletedData");
        }
        else
        {
          logger.info("Rolling back transacation");
          if (!commit)
          {
            String archivePath = getRootPath() + "/" + relativePath + "/";
            PrestoUtil.moveFiles(fileList, archivePath + "BadData");
          }
          PristineDBUtil.rollbackTransaction(this.conn, "Data Load");
        }
      }
      catch (GeneralException ge)
      {
        logger.error("Error in commit", ge);
        return;
      }
      curZipFileCount++;
      processZipFile = true;
    } while (curZipFileCount < zipFileList.size());
  }
  
  public static String[] mapFileField(String[] fieldNames, int columnCount2)
  {
    int fieldLength = columnCount2 < fieldNames.length ? columnCount2 : fieldNames.length;
    String[] fileFields = new String[fieldLength];
    for (int i = 0; i < fieldLength; i++) {
      fileFields[i] = fieldNames[i];
    }
    return fileFields;
  }
  
  public void processRecords(List listObject)
    throws GeneralException
  {
    List<StoreCouponDTO> storeCouponList = listObject;
    for (int j = 0; j < storeCouponList.size(); j++)
    {
      this.recordCount += 1;
      
      StoreCouponDTO couponDTO = (StoreCouponDTO)storeCouponList.get(j);
      
      String str = couponDTO.getStore();
      couponDTO.setStore(str.substring((str.length() > 4)?str.length() - 4:0));
      couponDTO.setTransactionNo(couponDTO.getTerminal() * 10000 + couponDTO.getTransactionNo());
      

      String dateStr = couponDTO.getCpnDate();
      String timeStr = couponDTO.getCpnTime();
      int year = Integer.parseInt(dateStr.substring(0, 4));
      int month = Integer.parseInt(dateStr.substring(4, 6));
      int date = Integer.parseInt(dateStr.substring(6));
      int hour = Integer.parseInt(timeStr.substring(0, 2));
      int minute = Integer.parseInt(timeStr.substring(2));
      Calendar cal = Calendar.getInstance();
      cal.set(year, month - 1, date, hour, minute);
      couponDTO.setItemDateTime(cal.getTime());
      if (couponDTO.getCustomerId() == null) {
        couponDTO.setCustomerId("");
      }
      couponDTO.setItemNetPrice(couponDTO.getItemNetPrice());
      

      String upc = PrestoUtil.castUPC(couponDTO.getItemUPC(), false);
      couponDTO.setItemUPC(upc);
      /*if (!this.itemCodeMap.containsKey(upc))
      {
        int itemCode = this.objItemDao.getItemCodeForUPC(this.conn, upc);
        this.itemCodeMap.put(upc, Integer.valueOf(itemCode));
      }*/
      
      if(this.checkRetailerItemCode){
    	  int storeId = 0;
    	  if(compIdMap.get(str) != null)
    		  storeId = compIdMap.get(str);
    	  if(storeId > 0){
	    	  int itemCode = getItemCode(upc, storeId);
	    	  if(itemCode > 0)
	    		  couponDTO.setItemCode(((Integer)this.itemCodeMap.get(upc)).intValue());
	    	  else
	    		  noItemCodeUPCs.add(upc);
    	  }
    	  else{
    		  if (!this.itemCodeMap.containsKey(upc))
              {
                int itemCode = this.objItemDao.getItemCodeForUPC(this.conn, upc);
                this.itemCodeMap.put(upc, Integer.valueOf(itemCode));
                couponDTO.setItemCode(((Integer)this.itemCodeMap.get(upc)).intValue());
              }
    	  }
      }
      else{
    	  if (!this.itemCodeMap.containsKey(upc))
          {
            int itemCode = this.objItemDao.getItemCodeForUPC(this.conn, upc);
            this.itemCodeMap.put(upc, Integer.valueOf(itemCode));
          }
    	  couponDTO.setItemCode(((Integer)this.itemCodeMap.get(upc)).intValue());
      }
     
      
      int lCalendarId = getRetailCalendarID(this.conn, couponDTO.getItemDateTime(), "D");
      couponDTO.setCalendarId(lCalendarId);
      
      int couponType = Integer.parseInt(couponDTO.getCouponType());
      try
      {
        double qty = Double.parseDouble(couponDTO.getStrCpnQty());
        couponDTO.setCpnQty(PrestoUtil.round(qty, 2));
      }
      catch (Exception e)
      {
        this.errorCount += 1;
        couponDTO.setCpnQty(1.0D);
      }
      try
      {
        double weight = Double.parseDouble(couponDTO.getStrCpnWeight());
        couponDTO.setCpnWeight(PrestoUtil.round(weight, 2));
      }
      catch (Exception e)
      {
        this.errorCount += 1;
        couponDTO.setCpnWeight(0.0D);
      }
      if ((couponDTO.getItemNetPrice() > 0.0D) && (couponDTO.getItemNetPrice() < 99999.0D))
      {
        if (this.filterByStore)
        {
          if (couponDTO.getStore().equals(this.storeNum)) {
            loadCouponData(couponDTO);
          } else {
            this.ignoredRecords += 1;
          }
        }
        else {
          loadCouponData(couponDTO);
        }
      }
      else
      {
        if (couponDTO.getItemNetPrice() > 99999.0D) {
          logger.error("Coupon Record price is too high: " + couponDTO.getItemNetPrice() + "  record : " + this.recordCount);
        }
        this.ignoredRecords += 1;
      }
      if (this.recordCount % 25000 == 0) {
        logger.info("No of records processed = " + this.recordCount);
      }
    }
    if (this.insertCouponList.size() > 0)
    {
      int returnCount = this.couponDao.insertCoupons(this.conn, this.insertCouponList, this.isMultiCouponLoad);
      this.processedCount += returnCount;
      this.rejectedCount += this.insertCouponList.size() - returnCount;
      this.insertCouponList.clear();
    }
  }
  
  private void loadCouponData(StoreCouponDTO couponDTO)
    throws GeneralException
  {
    logger.debug("Store, UPC, Price, Time " + couponDTO.getStore() + ", " + couponDTO.getItemUPC() + ", " + 
      couponDTO.getItemNetPrice() + ", " + couponDTO.getItemDateTimeStr());
    
    this.insertCouponList.add(couponDTO);
    if (this.insertCouponList.size() % 1000 == 0)
    {
      int returnCount = this.couponDao.insertCoupons(this.conn, this.insertCouponList, this.isMultiCouponLoad);
      this.processedCount += returnCount;
      this.rejectedCount += this.insertCouponList.size() - returnCount;
      this.insertCouponList.clear();
    }
  }
  
  public int getRetailCalendarID(Connection _Conn, Date startDate, String calendarMode)
    throws GeneralException
  {
    int calId = -1;
    
    String strDate = DateUtil.dateToString(startDate, "MM/dd/yyyy");
    if (this.calendarId != -1)
    {
      calId = this.calendarId;
    }
    else if (this.calendarIdMap.containsKey(strDate))
    {
      calId = ((Integer)this.calendarIdMap.get(strDate)).intValue();
    }
    else
    {
      List<RetailCalendarDTO> dateList = this.objCal.dayCalendarList(_Conn, startDate, startDate, calendarMode);
      if (dateList.size() > 0)
      {
        RetailCalendarDTO calDto = (RetailCalendarDTO)dateList.get(0);
        calId = calDto.getCalendarId();
      }
      this.calendarIdMap.put(strDate, Integer.valueOf(calId));
      
      logger.info("Calendar Id for date " + startDate.toString() + " is " + calId);
    }
    return calId;
  }
  
  public static LinkedHashMap getStrCoupon_Fields()
  {
    if (COUPON_FIELDS == null)
    {
      COUPON_FIELDS = new LinkedHashMap();
      COUPON_FIELDS.put("company", "0-4");
      COUPON_FIELDS.put("store", "5-11");
      COUPON_FIELDS.put("terminal", "11-15");
      COUPON_FIELDS.put("transactionNo", "15-19");
      COUPON_FIELDS.put("operator", "19-29");
      COUPON_FIELDS.put("itemDateTimeStr", "29-41");
      COUPON_FIELDS.put("customerId", "41-65");
      COUPON_FIELDS.put("couponNumber", "65-79");
      COUPON_FIELDS.put("itemUPC", "79-93");
      COUPON_FIELDS.put("itemNetPrice", "93-103");
      COUPON_FIELDS.put("posDept", "103-107");
      COUPON_FIELDS.put("cpnFamily", "107-110");
      COUPON_FIELDS.put("filler1", "110-127");
      
      COUPON_FIELDS.put("strCpnQty", "127-133");
      COUPON_FIELDS.put("strCpnWeight", "133-141");
      
      COUPON_FIELDS.put("filler2", "141-177");
      COUPON_FIELDS.put("couponType", "177-178");
      COUPON_FIELDS.put("filler3", "178-188");
    }
    return COUPON_FIELDS;
  }
  
	private void getAuthorizedItems(){
		ItemDAO objItem = new ItemDAO();
		try {
			logger.info("Preload Item code/UPC authorized at store level");
			_itemHashMapStoreUpc = objItem.getAuthorizedItems(conn);
			logger.info("Total Item code/UPC authorized at store level: " + itemCodeMap.size());
		
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
	}
	
	/**
	 * Returns item code authorized for store and input upc
	 * @param strUpc
	 * @param posDept
	 * @param storeId
	 * @return
	 * @throws GeneralException
	 */
	public int getItemCode(String strUpc, Integer storeId) throws GeneralException
	{
    	int itemCode=-1;
    	String itemUpc = PrestoUtil.castUPC(strUpc, false);
    	
    	if (_itemHashMapStoreUpc.containsKey(storeId)) {
    		HashMap<String, Integer> tMap = _itemHashMapStoreUpc.get(storeId);
    		if(tMap.containsKey(itemUpc)){
    			itemCode = tMap.get(itemUpc);
    		}else if(itemCodeMap.containsKey(itemUpc)){
        		// Code should reach this point only if items are preloaded
        		itemCode = itemCodeMap.get(itemUpc);
        	}else{
        		ItemDAO objItemDao = new ItemDAO();
    			itemCode = objItemDao.getItemCodeForStoreUPC(conn, itemUpc, storeId);
    			if (itemCode > 0){
    				tMap.put(itemUpc, itemCode);
    				_itemHashMapStoreUpc.put(storeId, tMap);
    			}
    		}
    	}else if(itemCodeMap.containsKey(itemUpc)){
    		// Code should reach this point only if items are preloaded
    		itemCode = itemCodeMap.get(itemUpc);
    	}else{
    		//Get Item code from DB
			ItemDAO objItemDao = new ItemDAO();
			itemCode = objItemDao.getItemCodeForStoreUPC(conn, itemUpc, storeId);
			
			//If exist
			if (itemCode > 0){
				HashMap<String, Integer> tMap = null;
				if(_itemHashMapStoreUpc.get(storeId) != null)
					tMap = _itemHashMapStoreUpc.get(storeId);
				else
					tMap = new HashMap<String, Integer>();
				tMap.put(itemUpc, itemCode);
				_itemHashMapStoreUpc.put(storeId, tMap);
			}
		}
		return itemCode;
	}
	
	
	
}