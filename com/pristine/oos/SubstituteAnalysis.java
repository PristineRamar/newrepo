package com.pristine.oos;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SubstituteAnalysis {
	
	static Logger logger = Logger.getLogger("SubstituteAnalysis");
	
	private Connection _conn = null; // DB connection
	
	private String storeNum = null; // Processing store number
	private String analysisDate = null; 
	private int categoryId = 0; 
	private int subcategoryId = 0;
	

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-substitute-analysis.properties");

		logger.info("main() - Started");
		
		logCommand(args);
		
		String strAnalysisDate = "";
		String argStoreNum = "";
		String argCat = "";
		String argSubCat = "";
		int categoryId = 0;
		int subcategoryId = 0;
		
		SubstituteAnalysis sA = new SubstituteAnalysis();
		logger.info("main() - SubstituteAnalysis created.");

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
				
				// Get the Store Number from command line
				if (arg.startsWith("STORE")) {
					argStoreNum = arg.substring("STORE=".length());
				}

				// Get the Category Id from command line
				if (arg.startsWith("CAT")) {
					argCat = arg.substring("CAT=".length());
				}

				// Get the Sub-category Id from command line
				if (arg.startsWith("SUBCAT")) {
					argSubCat = arg.substring("SUBCAT=".length());
				}

				// Get the Analysis Date from command line
				if (arg.startsWith("ANALYSISDATE")) {
					strAnalysisDate = arg.substring("ANALYSISDATE=".length());
				}
			}
			
			if ( argCat.length() > 0 ) {
				categoryId = Integer.parseInt(argCat);
			}
			if ( argSubCat.length() > 0 ) {
				subcategoryId = Integer.parseInt(argSubCat);
			}

			sA.categoryId = categoryId;
			sA.subcategoryId = subcategoryId;
			sA.analysisDate = strAnalysisDate;
			sA.storeNum = argStoreNum;

			//TODO Call to be parameterized and enabled 
			//sA.generateSubstituteAnalysis();
			
			sA.doItemSubstituteAnalysis();

		} catch (Exception e) {
			logger.error("main() - " + e.getMessage());

		} finally {
			PristineDBUtil.close(sA._conn);
			logger.info("main() - Ended");
		}

	}

	
	public SubstituteAnalysis(){
		try {
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("SubstituteAnalysis() - " + exe);
			System.exit(1);
		}

	}
	
	public void doItemSubstituteAnalysis(){
		logger.info("doItemSubstituteAnalysis() - Started");
		
		
		try{
			// Validations
			int storeId =-1;

			StoreDAO strdao = new StoreDAO();
			storeId  = strdao.getStoreID(_conn, storeNum,null,-1);
			if( storeId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
			}			
		
			// Initialize Parameters
		    int item_type_item = 1;
		    int store_location_type_id = 5;
		    Date dNow = new Date( );
		    SimpleDateFormat ft = new SimpleDateFormat ("yyMMddhhmm");		    
		    String batchId = ft.format(dNow);		    
		    String itemFilter;
		    boolean result = false;
		    
		    logger.info("doItemSubstituteAnalysis() - Batch Id=" + batchId);
		    
		    OOSCalcsDAO substitutesDAO = new OOSCalcsDAO();
		    
		    // Processing
		    if (subcategoryId > 0)
		    {
		    	itemFilter = " sub_category_id = " + subcategoryId + " ";
		    	
		    	logger.info("doItemSubstituteAnalysis() - Processing for subcategoryId=" + subcategoryId + "..");
		    	result = substitutesDAO.getItemSubstitutes(_conn, batchId, analysisDate, store_location_type_id, storeId, item_type_item, itemFilter);
		    }
		    else if (categoryId > 0)
		    {
		    	itemFilter = " category_id = " + categoryId + " ";
		    	
		    	logger.info("doItemSubstituteAnalysis() - Processing for categoryId=" + categoryId + "..");
		    	result = substitutesDAO.getItemSubstitutes(_conn, batchId, analysisDate, store_location_type_id, storeId, item_type_item, itemFilter);
		    }
		    else
		    {		    	
		    	List<Integer> subcategoryIds = substitutesDAO.getEligibleCategories(_conn);
		    	int subcategoryCount = subcategoryIds.size();
		    	
		    	logger.info("doItemSubstituteAnalysis() - Processing for segments.. segments_size=" + subcategoryIds.size());
		    	for (int i = 0; i < subcategoryCount; i++)
		    	{
		    		subcategoryId = subcategoryIds.get(i);
		    		itemFilter = " segment_id = " + subcategoryId + " ";		    		
		    		
		    		logger.info("doItemSubstituteAnalysis() - Processing " + (i + 1) + " of " + subcategoryCount + " for subcategoryid=" + subcategoryId + "..");
		    		result = substitutesDAO.getItemSubstitutes(_conn, batchId, analysisDate, store_location_type_id, storeId, item_type_item, itemFilter);
		    		if (!result)
		    			logger.info("doItemSubstituteAnalysis() - FAILED for subcategory_id=" + subcategoryId + "!");
		    	}
		    }

		    _conn.close();
			
		    logger.info("doItemSubstituteAnalysis() - Ended");
			
		}catch(GeneralException e){
			logger.error("generateSubstituteAnalysis() - GenralException=" + e.getMessage());
			e.printStackTrace();
		}catch(Exception e){
			logger.error("generateSubstituteAnalysis() - Exception=" + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void generateSubstituteAnalysis(){
		logger.info("generateSubstituteAnalysis() - Started");		
		
		try{
			
			// Validations
			int storeId =-1;

			StoreDAO strdao = new StoreDAO();
			storeId  = strdao.getStoreID(_conn, storeNum,null,-1);
			if( storeId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
			}

			
			
			// Data Structure Declarations
		    int item_type_item = 1;
		    int item_type_lig = 2;
			
			List<Integer> iList = new ArrayList<Integer>();
			List<Integer> lirList = new ArrayList<Integer>();
			Hashtable<Integer,String> iListName = new Hashtable<Integer,String>();
			Hashtable<Integer,String> iListCat = new Hashtable<Integer,String>();
			Hashtable<Integer,String> iListSubCat = new Hashtable<Integer,String>();
			Hashtable<Integer,Integer> iListSubCatNo = new Hashtable<Integer,Integer>();
			Hashtable<Integer,String> iListSeg = new Hashtable<Integer,String>();
			Hashtable<Integer,Integer> iListSegNo = new Hashtable<Integer,Integer>();
			Hashtable<Integer,Integer> iTrxCount = new Hashtable<Integer,Integer>();
			Hashtable<Integer,String> iListItemSize = new Hashtable<Integer,String>();
			Hashtable<Integer,String> iListUOM = new Hashtable<Integer,String>();
			Hashtable<Integer,Double> iListPrice = new Hashtable<Integer,Double>();
	
			
			Hashtable<Integer,String> lirListName = new Hashtable<Integer,String>();
			Hashtable<Integer,String> lirListCat = new Hashtable<Integer,String>();
			Hashtable<Integer,String> lirListSeg = new Hashtable<Integer,String>();
			Hashtable<Integer,Integer> lirListSegNo = new Hashtable<Integer,Integer>();
			Hashtable<Integer,String> lirListSubCat = new Hashtable<Integer,String>();
			Hashtable<Integer,Integer> lirSubCatNo = new Hashtable<Integer,Integer>();
			Hashtable<Integer,Integer> lirTrxCount = new Hashtable<Integer,Integer>();
			Hashtable<Integer,String> lirItemSize = new Hashtable<Integer,String>();
			Hashtable<Integer,String> lirUOM = new Hashtable<Integer,String>();
			Hashtable<Integer,Double> lirPrice = new Hashtable<Integer,Double>();
	
			Hashtable<Integer,Integer> iSubCatTrxCount = new Hashtable<Integer,Integer>();
			Hashtable<Integer,Integer> iSubCatCustCount = new Hashtable<Integer,Integer>();
			
			
		    Date dNow = new Date( );
		    SimpleDateFormat ft = new SimpleDateFormat ("yyMMddhhmm");
		    String batchId = ft.format(dNow);
		    logger.info("generateSubstituteAnalysis() - Batch Id=" + batchId);
			
			OOSCalcsDAO oosCalcsDao = new OOSCalcsDAO();			
			
			// NON LIG Items that moved x days and above with y or more quantity in the n days period
			CachedRowSet crsItemList = oosCalcsDao.getItemsForSubstituteAnalysis(_conn, storeId, 
					categoryId, subcategoryId, analysisDate, "itemListforSubstituteSQL2"); // Rite Aid
			logger.info("generateSubstituteAnalysis() - Item List Size=" + crsItemList.size());
			
			//CachedRowSet crsItemList = oosCalcsDao.getListForSubstituteAnalysis(_conn, storeId, 
			//		categoryId,subcategoryId,analysisDate,"itemListforSubstituteSQL");
		
			// LIG Items that moved x days and above with y or more quantity in the n days period
			CachedRowSet crsLIRList = oosCalcsDao.getLIRsForSubstituteAnalysis(_conn, storeId, 
					categoryId, subcategoryId, analysisDate, "lirListforSubstituteSQL2");  // Rite Aid			
			logger.info("generateSubstituteAnalysis() - LIG List Size=" + crsLIRList.size());
	
		    
			while(crsItemList.next()){
				Integer itemCode = crsItemList.getInt("item_code");
				Integer theId =null;
				theId = itemCode;
					
				if(iList.contains(theId)){
					continue;
				}
				
				iList.add(theId);
				
				iListName.put(theId, crsItemList.getString("item_name"));
				iListSegNo.put(theId, crsItemList.getInt("segment_id"));
				iListSeg.put(theId, crsItemList.getString("segment_name"));
				iListSubCatNo.put(theId, crsItemList.getInt("sub_category_id"));
				iListSubCat.put(theId, crsItemList.getString("sub_cat_name"));
				iListCat.put(theId, crsItemList.getString("cat_name"));				
				
				iListItemSize.put(theId, (crsItemList.getString("it_size")==null)?"":crsItemList.getString("it_size"));
				iListUOM.put(theId, (crsItemList.getString("uom_nm")==null)?"":crsItemList.getString("uom_nm"));
				iListPrice.put(theId, crsItemList.getDouble("pr"));
				
				// Find the number of transactions from n days before (and why no end date?)
				iTrxCount.put(theId,oosCalcsDao.getTrxCountForItem(_conn, theId, storeId, analysisDate));
				
				logger.debug("generateSubstituteAnalysis() - ID=" + theId + ", sub_category_id=" + crsItemList.getInt("sub_category_id") + 
						", iListSubCatNo=" + iListSubCatNo.get(theId));
				
				if(!iSubCatTrxCount.containsKey(crsItemList.getInt("sub_category_id"))){
					// Find the number of transactions for the SUB CATEGORY to which the Item belongs to from n days before (and why no end date?)
					iSubCatTrxCount.put(crsItemList.getInt("sub_category_id"), 
						oosCalcsDao.getTrxCountForSubCat(_conn, crsItemList.getInt("sub_category_id"), storeId,analysisDate));
				}
				if(!iSubCatCustCount.containsKey(crsItemList.getInt("sub_category_id"))){
					// Find the number of Customers for the SUB CATEGORY to which the Item belongs to from n days before (and why no end date?)
					iSubCatCustCount.put(crsItemList.getInt("sub_category_id"), 
						oosCalcsDao.getCustCountForSubCat(_conn, crsItemList.getInt("sub_category_id"), storeId,analysisDate));
				}
			}
			logger.info("generateSubstituteAnalysis() - Transaction counts fetched.  iList.size()=" + iList.size());
	
			 while(crsLIRList.next()){
				Integer lirId = crsLIRList.getInt("ret_lir_id");
				Integer theId =null;
				theId = lirId;
				
				if(lirList.contains(theId) || theId==0){
					continue;
				}
				
				lirList.add(theId);
				//lirListName.put(theId, crsLIRList.getString("ret_lir_name"));			
				lirListSegNo.put(theId, crsLIRList.getInt("segment_id"));
				//lirListSeg.put(theId, crsLIRList.getString("segment_name"));
				lirSubCatNo.put(theId, crsLIRList.getInt("sub_category_id"));
				//lirListSubCat.put(theId, crsLIRList.getString("sub_cat_name"));
				//lirListCat.put(theId, crsLIRList.getString("cat_name"));
				
				String iSize = (crsLIRList.getString("it_size")==null)?"":crsLIRList.getString("it_size");
				if(iSize.length()>0 && iSize.indexOf(",")>0){
					iSize = iSize.substring(0, iSize.indexOf(","));
				}
				lirItemSize.put(theId, iSize);
				String iUom = (crsLIRList.getString("uom_nm")==null)?"":crsLIRList.getString("uom_nm");
				if(iUom.length()>0 && iUom.indexOf(",")>0){
					iUom = iUom.substring(0, iUom.indexOf(","));
				}
				lirUOM.put(theId, iUom);
				lirPrice.put(theId, crsLIRList.getDouble("pr"));
				
				// Find the number of transactions from n days before (and why no end date?)
				Integer segId = crsLIRList.getInt("segment_id");
				lirTrxCount.put(theId,oosCalcsDao.getTrxCountForLIG(_conn, theId, segId, storeId,analysisDate));
				
				logger.debug("generateSubstituteAnalysis() - ret_lir_id=" + theId + ", sub_category_id=" + crsLIRList.getInt("sub_category_id") + 
						", lirSubCatNo=" + lirSubCatNo.get(theId));
			
				if(!iSubCatTrxCount.containsKey(crsLIRList.getInt("sub_category_id"))){
					iSubCatTrxCount.put(crsLIRList.getInt("sub_category_id"), oosCalcsDao.getTrxCountForSubCat(_conn, crsLIRList.getInt("sub_category_id"), storeId, analysisDate));
				}
				if(!iSubCatCustCount.containsKey(crsLIRList.getInt("sub_category_id"))){
					iSubCatCustCount.put(crsLIRList.getInt("sub_category_id"), oosCalcsDao.getCustCountForSubCat(_conn, crsLIRList.getInt("sub_category_id"), storeId, analysisDate));
				}
			}
			logger.info("generateSubstituteAnalysis() - Transaction counts fetched.  lirList.size()=" + lirList.size());
			 
			// Excel Report Creation
			logger.info("generateSubstituteAnalysis() - Begin to generate Excel Header");
		    
		    HSSFWorkbook wb = new HSSFWorkbook();
		    HSSFSheet sheet = wb.createSheet("subst_analysis");
		    
		    HSSFCell firstCell;
		    HSSFCell lastCell;
	
		    HSSFRow header = sheet.createRow(0);
		    int c = 0;
		    header.createCell(c++).setCellValue("Store"); 
		    header.createCell(c++).setCellValue("Category");
		    header.createCell(c++).setCellValue("Sub Category");
		    header.createCell(c++).setCellValue("Segment A");
		    header.createCell(c++).setCellValue("Segment B");
		    header.createCell(c++).setCellValue("Item Code A");
		    header.createCell(c++).setCellValue("Item A Desc");
		    header.createCell(c++).setCellValue("Item Code B");
		    header.createCell(c++).setCellValue("Item B Desc");
		    
		    header.createCell(c++).setCellValue("# of trans with either A or B was present");
		    header.createCell(c++).setCellValue("% of trans with either A or B was present");
		    header.createCell(c++).setCellValue("# of trans with A present");
		    header.createCell(c++).setCellValue("% of trans with A present");
		    header.createCell(c++).setCellValue("# of trans with B present");
		    header.createCell(c++).setCellValue("% of trans with B present");
		    header.createCell(c++).setCellValue("# of trans with A present & B absent");
		    header.createCell(c++).setCellValue("% of trans with A present & B absent");
		    header.createCell(c++).setCellValue("# of trans with B present & A absent");
		    header.createCell(c++).setCellValue("% of trans with B present & A absent");
		    header.createCell(c++).setCellValue("# of trans with both A & B present");
		    header.createCell(c++).setCellValue("% of trans with both A & B present");
		    header.createCell(c++).setCellValue("# of trans with both A & B absent");
		    header.createCell(c++).setCellValue("# of trans with any item from the Sub category list was present");
		    
		    header.createCell(c++).setCellValue("ITEM/LIG");
		    header.createCell(c++).setCellValue("# of customers satisfy criteria");
	
		    header.createCell(c++).setCellValue("# of Cust with Trans in A or B");
		    header.createCell(c++).setCellValue("# of Cust with Trans in A And B");
		    header.createCell(c++).setCellValue("# of Cust with Trans in A Not B");
		    header.createCell(c++).setCellValue("# of Cust with Trans in A as well as B");
	
		    header.createCell(c++).setCellValue("# of Trans within the cust in A or B");
		    header.createCell(c++).setCellValue("# of Trans within the cust in A and B");
		    header.createCell(c++).setCellValue("# of Trans within the cust in A");
		    header.createCell(c++).setCellValue("# of Trans within the cust in B");
		    
		    header.createCell(c++).setCellValue("# of Cust with Trans in Sub Category");
		    
		    header.createCell(c++).setCellValue("Size (A)");
		    header.createCell(c++).setCellValue("UOM (A)");
		    header.createCell(c++).setCellValue("Price (A)");
		    header.createCell(c++).setCellValue("Size (B)");
		    header.createCell(c++).setCellValue("UOM (B)");
		    header.createCell(c++).setCellValue("Price (B)");
	
		    header.setHeightInPoints((3*sheet.getDefaultRowHeightInPoints()));
		    HSSFCellStyle cstyle = wb.createCellStyle();
		    cstyle.setWrapText(true);
		    header.setRowStyle(cstyle);
		    		 
		 int rowCount =0;
		 
		 logger.info("generateSubstituteAnalysis() - Identifying Substitutes at Item level." );
		 
	     for(int i=0; i<iList.size(); i++){
			for(int j=0; j<iList.size(); j++){
				
				// run queries for the combination of i and j
				try {
					Integer a = iList.get(i);
					Integer b = iList.get(j);
					
					
					if ((a == b) || (!iListSubCatNo.get(a).equals(iListSubCatNo.get(b)))){
						logger.debug("generateSubstituteAnalysis() - ITEM a=" + a + ", b=" + b + ", SubCatNo(a)=" + iListSubCatNo.get(a) + 
								", SubCatNo(b)="+iListSubCatNo.get(b) );
						continue;
					}
					
					Hashtable<String,Integer> result  = oosCalcsDao.getSubsitutionAnalysisMetricForItem(_conn, iList.get(i), iList.get(j), storeId, analysisDate);
					logger.debug("-");
					Hashtable<String,Hashtable<Integer,Integer>> resultCust  = oosCalcsDao.getCustomerWiseSubsitutionAnalysisMetricForItem(_conn, iList.get(i), iList.get(j), storeId, analysisDate);
					logger.debug("generateSubstituteAnalysis() - ITEM a=" + a + ", b=" + b + 
							", cust-count-aorb=" + resultCust.get("AorB").size() + ", cust-count-aAndb=" + resultCust.get("AandB").size());

					
					Integer aOrb = result.get("AorB");
					Integer aCount = iTrxCount.get(a);
					Integer bCount = iTrxCount.get(b);
					Integer aAndb = result.get("AandB");
					Integer aNotb = aCount - aAndb;
					Integer bNota = bCount - aAndb;
					
					Integer subCatTrxCount = iSubCatTrxCount.get(iListSubCatNo.get(a));
					Integer subcatCustCount = iSubCatCustCount.get(iListSubCatNo.get(a));

					Hashtable<Integer,Integer> cAorB = resultCust.get("AorB");
					Hashtable<Integer,Integer> cAandB = resultCust.get("AandB");
					Hashtable<Integer,Integer> cAnotB = resultCust.get("AnotB");
					Hashtable<Integer,Integer> cAasWellasB = resultCust.get("AasWellasB");  //  <-- candidates for consideration
					Hashtable<Integer,Integer> cA = resultCust.get("A");
					Hashtable<Integer,Integer> cB = resultCust.get("B");
					
					Enumeration<Integer> cust = cAasWellasB.keys();
					int criteriaCustCount =0;
					
					Integer trxCWithAorB =0;
					Integer trxCWithAandB =0;
					Integer trxCWithA =0;
					Integer trxCWithB =0;
					
					while(cust.hasMoreElements()){
						Integer cus = cust.nextElement();
						Integer countAorB = cAorB.get(cus);
						Integer countAandB = cAandB.get(cus);
						Integer countA = cA.get(cus);
						Integer countB = cB.get(cus);

						if(countA==null) countA=0;
						if(countB==null) countB=0;
						if(countAandB==null){
							countAandB=0;
						}
						
						trxCWithAorB = trxCWithAorB+ countAorB;
						trxCWithAandB = trxCWithAandB+ countAandB;
						trxCWithA = trxCWithA+ countA;
						trxCWithB = trxCWithB+ countB;
						
						if(countAandB/countAorB < 15 && countA*100/countAorB > 50 && countB*100/countAorB > 20){
							criteriaCustCount++;
						}
					
						
						
					}
				
					rowCount++;
				    HSSFRow row = sheet.createRow(rowCount);
					int count = 0;
					row.createCell(count++).setCellValue(storeNum);
				    row.createCell(count++).setCellValue(iListCat.get(a));
				    row.createCell(count++).setCellValue(iListSubCat.get(a));
				    row.createCell(count++).setCellValue(iListSeg.get(a));
				    row.createCell(count++).setCellValue(iListSeg.get(b));
				    row.createCell(count++).setCellValue(a);
				    row.createCell(count++).setCellValue(iListName.get(a));
				    row.createCell(count++).setCellValue(b);
				    row.createCell(count++).setCellValue(iListName.get(b));
				    row.createCell(count++).setCellValue(aOrb);
				    row.createCell(count++).setCellValue(aOrb*100/subCatTrxCount);
				    row.createCell(count++).setCellValue(aCount);
				    row.createCell(count++).setCellValue(aCount*100/aOrb);
				    row.createCell(count++).setCellValue(bCount);
				    row.createCell(count++).setCellValue(bCount*100/aOrb);
				    row.createCell(count++).setCellValue(aNotb);
				    row.createCell(count++).setCellValue(aNotb*100/aOrb);
				    row.createCell(count++).setCellValue(bNota);
				    row.createCell(count++).setCellValue(bNota*100/aOrb);
				    row.createCell(count++).setCellValue(aAndb);
				    row.createCell(count++).setCellValue(aAndb*100/aOrb);
				    row.createCell(count++).setCellValue(subCatTrxCount-aOrb);
				    row.createCell(count++).setCellValue(subCatTrxCount);

				   	row.createCell(count++).setCellValue("ITEM");
				    row.createCell(count++).setCellValue(criteriaCustCount);
				    

				    row.createCell(count++).setCellValue(cAorB.size());
				    row.createCell(count++).setCellValue(cAandB.size());
				    row.createCell(count++).setCellValue(cAnotB.size());
				    row.createCell(count++).setCellValue(cAasWellasB.size());
				    
				    row.createCell(count++).setCellValue(trxCWithAorB);
				    row.createCell(count++).setCellValue(trxCWithAandB);
				    row.createCell(count++).setCellValue(trxCWithA);
				    row.createCell(count++).setCellValue(trxCWithB);

				    row.createCell(count++).setCellValue(subcatCustCount);

				    row.createCell(count++).setCellValue(iListItemSize.get(a));
				    row.createCell(count++).setCellValue(iListUOM.get(a));
				    row.createCell(count++).setCellValue(iListPrice.get(a));
				    row.createCell(count++).setCellValue(iListItemSize.get(b));
				    row.createCell(count++).setCellValue(iListUOM.get(b));
				    row.createCell(count++).setCellValue(iListPrice.get(b));
				    

				    int store_location_type_id = 5;
				    
				    int overallStrength = 0;
				    float criteriaCustPct = 0;
				    float aawab_cst_pct = 0;
				    float aorb_cst_pct = 0;
				    
				    float a_trx_pct = 0;
				    float b_trx_pct = 0;
				    float aandb_trx_pct = 0;

				    
				    // % cust satisfying criteria
				    if ( cAasWellasB.size() > 0 ) 
				    	criteriaCustPct = (criteriaCustCount * 100) / cAasWellasB.size();
				    
				    // % Customers who bought A as well as B
				    if ( cAorB.size() > 0 )
				    	aawab_cst_pct = (cAasWellasB.size() * 100) / cAorB.size();
				    
				    // % customers who bought A or B within the segment
				    if ( subcatCustCount > 0 )
				    	aorb_cst_pct = (cAorB.size() * 100) / subcatCustCount;

				    if ( trxCWithAorB > 0 )
				    {
				    	// % Trans with A within Trx involving A or B
				    	a_trx_pct = (trxCWithA * 100) / trxCWithAorB;
				    
				    	// % Trans with B within Trx involving A or B
				    	b_trx_pct = (trxCWithB * 100) / trxCWithAorB;
				    
				    	// % trax contining both A and B
				    	aandb_trx_pct = (trxCWithAandB * 100) / trxCWithAorB;
				    }
				    
				    
				    double sizediff_pct = 0;
				    double a_price_rate = 0;
				    double b_price_rate = 0;
				    double price_pct = 0;
				    
				    //% Size Diff					    
				    if ( iListPrice.get(a) > 0 )
				    	sizediff_pct = ( ( iListPrice.get(a) - iListPrice.get(b) ) * 100 ) / iListPrice.get(a);
				    
				    // Price per Unit (A)
				    if ( Double.parseDouble(iListItemSize.get(a)) > 0 )
				    	a_price_rate = iListPrice.get(a) / Double.parseDouble(iListItemSize.get(a));
				    
				    // Price per Unit (B)
				    if ( Double.parseDouble(iListItemSize.get(b)) > 0 )
				    	b_price_rate = iListPrice.get(b) / Double.parseDouble(iListItemSize.get(b));
				    
				    if ( a_price_rate > 0 )
				    	price_pct = ( ( a_price_rate - b_price_rate ) * 100 ) / a_price_rate;   

				    int aawab_cst_score = 0;
				    int trx_pct_score = 0;
				    int size_score = 0;
				    int price_score = 0;
				    int segment_score = 0;
				    int aandb_score = 0;
				    int overall_score = 0;

				    // score based on % A as well as B
				    if (aawab_cst_pct > 5) 
				    	aawab_cst_score = 2;
				    else if (aawab_cst_pct > 3) 
				    	aawab_cst_score = 1;
				    else
				    	aawab_cst_score = 0;
				    
				    // Score based on trx %					    
				    if ( b_trx_pct > 40 )
				    	trx_pct_score = 2;
				    else if ( b_trx_pct > 30 )
						trx_pct_score = 1;
					else
						trx_pct_score = 0;
				    	
				    // Score based on size diff
				    if ( sizediff_pct < 40 )
				    	size_score = 1; 
				    // Score based on price diff
				    if ( price_pct < 40 )
				    	price_score = 1;
				    
				    // Score based on same segment
				    if ( iListSeg.get(a) == iListSeg.get(b) )
				    	segment_score = 2;
				    else
				    	segment_score = 0;					    
				    
				    // Score based on AandB
				    if ( aandb_trx_pct < 5 )					    	
				    	aandb_score = 1;
				    else
				    	aandb_score = 0;
				    
				    // Overall score
				    overall_score = aawab_cst_score + 
				    		trx_pct_score +
				    		size_score +
				    		//price_score +
				    		segment_score +
				    		aandb_score;
				    
				    // Strength					    
				    if ( overall_score >= 8 )
				    	overallStrength = 3; // "high";
				    else if ( overall_score >= 6 )
				    	overallStrength = 2; // "medium";
				    else if ( overall_score >= 3 )
				    	overallStrength = 1; // "low";
				    else
				    	overallStrength = 0; // "???";					     
				    
				    StringBuffer sb = new StringBuffer();
				    
					sb.append(" INSERT INTO item_substitutes VALUES ( ITEM_SUBSTITUTES_SEQ.NEXTVAL, " + store_location_type_id + ", ");
					sb.append(storeId + ", " + item_type_item + ", " + a + ", " + b + ", ");
					sb.append(iListItemSize.get(a) + ", '" + iListUOM.get(a) + "', ROUND(" + iListPrice.get(a) + ", 2), ");
					sb.append(iListItemSize.get(b) + ", '" + iListUOM.get(b) + "', ROUND(" + iListPrice.get(b) + ", 2), ");
					sb.append(overallStrength + ", "); // overall strength;
					sb.append(subCatTrxCount + ", " + subcatCustCount + ", ");
					sb.append(criteriaCustCount + ", ROUND(" + criteriaCustPct + ", 2), ");
					
					sb.append(cAorB.size() + ", " + cAasWellasB.size() + ", " + cAandB.size() + ", " + cAnotB.size() + ", ");
					sb.append(trxCWithAorB + ", " + trxCWithAandB + ", " + trxCWithA + ", " + trxCWithB + ", ");
					
					sb.append("ROUND(" + aawab_cst_pct + ", 2), ROUND(" + aorb_cst_pct + ", 2), ROUND(" + a_trx_pct + ", 2), ROUND(" + b_trx_pct + ", 2), ROUND(" + aandb_trx_pct + ", 2), ");						
					sb.append("ROUND(" + sizediff_pct + ", 2), ");
					sb.append("ROUND(" + a_price_rate + ", 2), ROUND(" + b_price_rate + ", 2), ROUND(" + price_pct + ", 2), ");
					
					sb.append(aawab_cst_score + ", " + trx_pct_score + ", " + size_score + ", " + price_score + ", " + segment_score + ", " + aandb_score + ", " + overall_score + ", " );
					
					sb.append( batchId + ", sysdate, 'Y'");
					sb.append(")");
					
					logger.debug("generateSubstituteAnalysis() - item-insert-sql=" + sb);
					
					boolean result1 = oosCalcsDao.insertItemSubstitutes(_conn, sb);
					_conn.commit();
						
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
			}
		}

	logger.info("generateSubstituteAnalysis() - Identifying Substitutes at LIG level." );

// For LIG
	    
	    
	    for(int i=0;i<lirList.size();i++){
			for(int j=0;j<lirList.size();j++){
				// run queries for the combination of i and j
				try {
					Integer a = lirList.get(i);
					Integer b = lirList.get(j);
					if((a==b) || (!lirSubCatNo.get(a).equals(lirSubCatNo.get(b))) ){
						logger.debug("generateSubstituteAnalysis() - LIG a=" + a + ", b=" + b + ", SubCatNo(a)=" + lirSubCatNo.get(a) + 
								", SubCatNo(b)="+lirSubCatNo.get(b) );
						continue;
					}
					Integer segId = lirListSegNo.get(a);
					
					Hashtable<String,Integer> result  = oosCalcsDao.getSubsitutionAnalysisMetricForLIG(_conn, lirList.get(i), lirList.get(j), segId, storeId, analysisDate);
					Hashtable<String,Hashtable<Integer,Integer>> resultCust  = oosCalcsDao.getCustomerWiseSubsitutionAnalysisMetricForLIG(_conn, lirList.get(i), lirList.get(j), segId,storeId, analysisDate);
					logger.debug("generateSubstituteAnalysis() - LIG a=" + a + ", b=" + b + 
							", cust-count-aorb=" + resultCust.get("AorB").size() + ", cust-count-aAndb=" + resultCust.get("AandB").size());

					int criteriaCustCount = 0;

					Integer aOrb = result.get("AorB");
					Integer aCount = lirTrxCount.get(a);
					Integer bCount = lirTrxCount.get(b);
					Integer aAndb = result.get("AandB");
					Integer aNotb = aCount - aAndb;
					Integer bNota = bCount - aAndb;

					Integer subcatTrxCount = iSubCatTrxCount.get(lirSubCatNo.get(a));
					Integer subcatCustCount = iSubCatCustCount.get(lirSubCatNo.get(a));

					if (aOrb == 0 || aAndb == 0){
						logger.debug("generateSubstituteAnalysis() - aOrb == 0 || aAndb == 0, Skipping pair=" + a + ", " + b + 
								", SegId=" + segId + ", SegNo=" + iListSegNo.get(a));
						continue;
					}

					Hashtable<Integer,Integer> cAorB = resultCust.get("AorB");
					Hashtable<Integer,Integer> cAandB = resultCust.get("AandB");
					Hashtable<Integer,Integer> cAnotB = resultCust.get("AnotB");
					Hashtable<Integer,Integer> cAasWellasB = resultCust.get("AasWellasB");  //  <-- candidates for consideration
					Hashtable<Integer,Integer> cA = resultCust.get("A");
					Hashtable<Integer,Integer> cB = resultCust.get("B");
					
					Enumeration<Integer> cust = cAasWellasB.keys();
					
					Integer trxCWithAorB =0;
					Integer trxCWithAandB =0;
					Integer trxCWithA =0;
					Integer trxCWithB =0;
					
					if(cA==null){
						continue;
					}
					while(cust.hasMoreElements()){
						Integer cus = cust.nextElement();
						Integer countAorB = cAorB.get(cus);
						Integer countAandB = cAandB.get(cus);
						Integer countA = cA.get(cus);
						Integer countB = cB.get(cus);
					
						if(countA==null) countA=0;
						if(countB==null) countB=0;
						
						if(countAandB==null){
							countAandB=0;
						}

						trxCWithAorB = trxCWithAorB+ countAorB;
						trxCWithAandB = trxCWithAandB+ countAandB;
						trxCWithA = trxCWithA+ countA;
						trxCWithB = trxCWithB+ countB;

						if(countAandB==0 && countA*100/countAorB > 40 && countB*100/countAorB > 15){
							criteriaCustCount++;
						}
						
						
					}
					
						rowCount++;
					    HSSFRow row = sheet.createRow(rowCount);
						int count = 0;
						row.createCell(count++).setCellValue(storeNum);
					    row.createCell(count++).setCellValue("-");	//lirListCat.get(a)
					    row.createCell(count++).setCellValue("-");	//lirListSubCat.get(a)
					    row.createCell(count++).setCellValue("-");	//lirListSeg.get(a)
					    row.createCell(count++).setCellValue("-");	//lirListSeg.get(b)
					    row.createCell(count++).setCellValue(a);
					    row.createCell(count++).setCellValue("-");	//lirListName.get(a)
					    row.createCell(count++).setCellValue(b);
					    row.createCell(count++).setCellValue("-");	//lirListName.get(b)
					    row.createCell(count++).setCellValue(aOrb);
					    row.createCell(count++).setCellValue(aOrb*100/subcatTrxCount);
					    row.createCell(count++).setCellValue(aCount);
					    row.createCell(count++).setCellValue(aCount*100/aOrb);
					    row.createCell(count++).setCellValue(bCount);
					    row.createCell(count++).setCellValue(bCount*100/aOrb);
					    row.createCell(count++).setCellValue(aNotb);
					    row.createCell(count++).setCellValue(aNotb*100/aOrb);
					    row.createCell(count++).setCellValue(bNota);
					    row.createCell(count++).setCellValue(bNota*100/aOrb);
					    row.createCell(count++).setCellValue(aAndb);
					    row.createCell(count++).setCellValue(aAndb*100/aOrb);
					    row.createCell(count++).setCellValue(subcatTrxCount-aOrb);
					    row.createCell(count++).setCellValue(subcatTrxCount);
						row.createCell(count++).setCellValue("LIG");
					    row.createCell(count++).setCellValue(criteriaCustCount);

					    row.createCell(count++).setCellValue(cAorB.size());
					    row.createCell(count++).setCellValue(cAandB.size());
					    row.createCell(count++).setCellValue(cAnotB.size());
					    row.createCell(count++).setCellValue(cAasWellasB.size());
					    
					    row.createCell(count++).setCellValue(trxCWithAorB);
					    row.createCell(count++).setCellValue(trxCWithAandB);
					    row.createCell(count++).setCellValue(trxCWithA);
					    row.createCell(count++).setCellValue(trxCWithB);

					    row.createCell(count++).setCellValue(subcatCustCount);

					    row.createCell(count++).setCellValue(lirItemSize.get(a));
					    row.createCell(count++).setCellValue(lirUOM.get(a));
					    row.createCell(count++).setCellValue(lirPrice.get(a));
					    row.createCell(count++).setCellValue(lirItemSize.get(b));
					    row.createCell(count++).setCellValue(lirUOM.get(b));
					    row.createCell(count++).setCellValue(lirPrice.get(b));
						
					    int store_location_type_id = 5;
					    
					    int overallStrength = 0;
					    float criteriaCustPct = 0;
					    float aawab_cst_pct = 0;
					    float aorb_cst_pct = 0;
					    
					    float a_trx_pct = 0;
					    float b_trx_pct = 0;
					    float aandb_trx_pct = 0;

					    
					    // % cust satisfying criteria
					    if ( cAasWellasB.size() > 0 ) 
					    	criteriaCustPct = (criteriaCustCount * 100) / cAasWellasB.size();
					    
					    // % Customers who bought A as well as B
					    if ( cAorB.size() > 0 )
					    	aawab_cst_pct = (cAasWellasB.size() * 100) / cAorB.size();
					    
					    // % customers who bought A or B within the segment
					    if ( subcatCustCount > 0 )
					    	aorb_cst_pct = (cAorB.size() * 100) / subcatCustCount;

					    if ( trxCWithAorB > 0 )
					    {
					    	// % Trans with A within Trx involving A or B
					    	a_trx_pct = (trxCWithA * 100) / trxCWithAorB;
					    
					    	// % Trans with B within Trx involving A or B
					    	b_trx_pct = (trxCWithB * 100) / trxCWithAorB;
					    
					    	// % trax contining both A and B
					    	aandb_trx_pct = (trxCWithAandB * 100) / trxCWithAorB;
					    }
					    
					    
					    double sizediff_pct = 0;
					    double a_price_rate = 0;
					    double b_price_rate = 0;
					    double price_pct = 0;
					    
					    //% Size Diff					    
					    if ( lirPrice.get(a) > 0 )
					    	sizediff_pct = ( ( lirPrice.get(a) - lirPrice.get(b) ) * 100 ) / lirPrice.get(a);
					    
					    // Price per Unit (A)
					    if ( Double.parseDouble(lirItemSize.get(a)) > 0 )
					    	a_price_rate = lirPrice.get(a) / Double.parseDouble(lirItemSize.get(a));
					    
					    // Price per Unit (B)
					    if ( Double.parseDouble(lirItemSize.get(b)) > 0 )
					    	b_price_rate = lirPrice.get(b) / Double.parseDouble(lirItemSize.get(b));
					    
					    if ( a_price_rate > 0 )
					    	price_pct = ( ( a_price_rate - b_price_rate ) * 100 ) / a_price_rate;   

					    int aawab_cst_score = 0;
					    int trx_pct_score = 0;
					    int size_score = 0;
					    int price_score = 0;
					    int segment_score = 0;
					    int aandb_score = 0;
					    int overall_score = 0;

					    // score based on % A as well as B
					    if (aawab_cst_pct > 5) 
					    	aawab_cst_score = 2;
					    else if (aawab_cst_pct > 3) 
					    	aawab_cst_score = 1;
					    else
					    	aawab_cst_score = 0;
					    
					    // Score based on trx %					    
					    if ( b_trx_pct > 40 )
					    	trx_pct_score = 2;
					    else if ( b_trx_pct > 30 )
							trx_pct_score = 1;
						else
							trx_pct_score = 0;
					    	
					    // Score based on size diff
					    if ( sizediff_pct < 40 )
					    	size_score = 1; 
					    // Score based on price diff
					    if ( price_pct < 40 )
					    	price_score = 1;
					    
					    // Score based on same segment
					    //if ( lirListSeg.get(a) == lirListSeg.get(b) )
					    if ( lirListSegNo.get(a) == lirListSegNo.get(b) )
					    
					    	segment_score = 2;
					    else
					    	segment_score = 0;					    
					    
					    // Score based on AandB
					    if ( aandb_trx_pct < 5 )					    	
					    	aandb_score = 1;
					    else
					    	aandb_score = 0;
					    
					    // Overall score
					    overall_score = aawab_cst_score + 
					    		trx_pct_score +
					    		size_score +
					    		//price_score +
					    		segment_score +
					    		aandb_score;
					    
					    // Strength					    
					    if ( overall_score >= 8 )
					    	overallStrength = 3; // "high";
					    else if ( overall_score >= 6 )
					    	overallStrength = 2; // "medium";
					    else if ( overall_score >= 3 )
					    	overallStrength = 1; // "low";
					    else
					    	overallStrength = 0; // "???";					     
					    
					    StringBuffer sb = new StringBuffer();
					    
						sb.append(" INSERT INTO item_substitutes VALUES ( ITEM_SUBSTITUTES_SEQ.NEXTVAL, " + store_location_type_id + ", ");
						sb.append(storeId + ", " + item_type_lig + ", " + a + ", " + b + ", ");
						sb.append(lirItemSize.get(a) + ", '" + lirUOM.get(a) + "', ROUND(" + lirPrice.get(a) + ", 2), ");
						sb.append(lirItemSize.get(b) + ", '" + lirUOM.get(b) + "', ROUND(" + lirPrice.get(b) + ", 2), ");
						sb.append(overallStrength + ", "); // overall strength;
						sb.append(subcatTrxCount + ", " + subcatCustCount + ", ");
						sb.append(criteriaCustCount + ", ROUND(" + criteriaCustPct + ", 2), ");
						
						sb.append(cAorB.size() + ", " + cAasWellasB.size() + ", " + cAandB.size() + ", " + cAnotB.size() + ", ");
						sb.append(trxCWithAorB + ", " + trxCWithAandB + ", " + trxCWithA + ", " + trxCWithB + ", ");
						
						sb.append("ROUND(" + aawab_cst_pct + ", 2), ROUND(" + aorb_cst_pct + ", 2), ROUND(" + a_trx_pct + ", 2), ROUND(" + b_trx_pct + ", 2), ROUND(" + aandb_trx_pct + ", 2), ");						
						sb.append("ROUND(" + sizediff_pct + ", 2), ");
						sb.append("ROUND(" + a_price_rate + ", 2), ROUND(" + b_price_rate + ", 2), ROUND(" + price_pct + ", 2), ");
						
						sb.append(aawab_cst_score + ", " + trx_pct_score + ", " + size_score + ", " + price_score + ", " + segment_score + ", " + aandb_score + ", " + overall_score + ", " );
						
						sb.append( batchId + ", sysdate, 'Y'");
						sb.append(")");
						
						logger.debug("generateSubstituteAnalysis() - lig-insert-sql=" + sb);
						
						boolean result1 = oosCalcsDao.insertItemSubstitutes(_conn, sb);
						_conn.commit();

						
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
				
				
			}
		}

	    logger.info("generateSubstituteAnalysis() - Substitutes identification completed." );
	    
		for(int i=0;i<=8;i++){
			sheet.autoSizeColumn(i);
		}

		String filename = "subst_analysis_st" + storeNum + "_ca" + categoryId + "_sc" + subcategoryId + "_ad" + analysisDate.replace("/", "_") + ".xls";
		String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");		
	    FileOutputStream fileOut = new FileOutputStream(filePath + "/OOSreports/" + filename);
		
		wb.write(fileOut);
	    fileOut.close();
		
		logger.info("generateSubstituteAnalysis() - Excel generation ended.");
		_conn.close();
		
		}catch(GeneralException e){
			logger.error("generateSubstituteAnalysis() - " + e.getMessage());
			e.printStackTrace();
		}catch(Exception e){
			logger.error("generateSubstituteAnalysis() - " + e.getMessage());
			e.printStackTrace();
		}
		
		
		
	
	
	}

	
	
	/**
R	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Arguments=");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info("logCommand() - " + sb.toString());
	}
	
}
