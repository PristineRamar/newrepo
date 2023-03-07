package com.pristine.dataload;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/*
 * Step 1- Do item pairing - build potential_item_pair table
 * 
 * Step 2 - Do price match using the comp data - pricematching 40 80 09/13/09 12/26/09
 * 
 * Step 3 - Done outside using query
 *     a) delete from potential_item_pairs where price_match_count = 0 
 *     b) delete from potential_item_pairs where  price_match_Count/(price_notmatch_count+ price_match_Count) <0.9
 *     
 * Step 4 - build item hierarchy or Like_item_grouping 
 * 		
 *  * Step 6 - populate comp data2 
 *       - setupnoflavor 40 80 09/13/09 12/26/09
 *      a) get the list of schedules
 *      b) get competitive data for schedule
 *      c) for each item, get the like item if available
 *      d) Insert into comp_data2
 *      e) keep a mapping of like_items_inserted 
 *  
 */
public class ProductGrouping {
	private static Logger  logger = Logger.getLogger("ProductGrouping");
	private ProductGroupDAO pgdao = new ProductGroupDAO();

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		logger.info("Product Group analysis started");
		PropertyManager.initialize("analysis.properties");
		if ( args.length < 2 || args.length > 6 ){
			logger.info("Incorrect parameters - product group option, chain id, [check list id], [start date (mm/dd/yy)], [end date (mm/dd/yy)]");
			System.exit(-1);
		}
		int chainId = 0;
		chainId = Integer.parseInt(args[1]);
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			ProductGrouping prdGrouping = new ProductGrouping();
			if( args[0].equalsIgnoreCase("pairing")){
				prdGrouping.doItemPairing(conn, chainId);
			}else if( args[0].equalsIgnoreCase("classifyunknown")){
				UnknownClassification unc = new UnknownClassification();
				unc.categorizeUnknownItems(conn, chainId);
			}
			else if( args[0].equalsIgnoreCase("classifyunknownM2")){
				UnknownClassification unc = new UnknownClassification();
				unc.categorizeUnknownItemsMethod2(conn, chainId);
			}
			else if( args[0].equalsIgnoreCase("unknowntransfer")){
				prdGrouping.doTransferUnknownItem(conn, chainId);
			}
			else if ( args[0].equalsIgnoreCase("pricematching")){
				int checkListId = Integer.parseInt(args[2]);
				String startDate = args[3];
				String endDate = args[4];
				//prdGrouping.doPriceMatching(conn, chainId, checkListId, startDate, endDate);
				//prdGrouping.doPriceMatchingV2(conn, chainId, checkListId, startDate, endDate);
				PriceMatch pm = new PriceMatch();
				pm.doPriceMatching(conn, chainId, checkListId, startDate, endDate);
			}else if ( args[0].equalsIgnoreCase("setuplikeitems")){
				prdGrouping.setupLikeItems(conn, chainId);
			}else if (args[0].equalsIgnoreCase("setupnoflavor")){
				int checkListId = Integer.parseInt(args[2]);
				String startDate = args[3];
				String endDate = args[4];
				prdGrouping.setupCompDataWithNoFlavor(conn, chainId, checkListId, startDate, endDate);
			}else if (args[0].equalsIgnoreCase("flavorstats")){
				int checkListId = Integer.parseInt(args[2]);
				String startDate = args[3];
				String endDate = args[4];
				boolean carriedOnly = false;
				if ( args.length == 6 && args[5].equals("true"))
					carriedOnly = true;
				prdGrouping.computeFlavorStats(conn, chainId, checkListId, startDate, endDate, carriedOnly);
			}
			else{
				logger.info("Incorrect option - valid options are pairing, pricematching");
				System.exit(-1);
			}
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
		}catch(GeneralException ge){
			logger.error("General Exception in Analysis", ge);
		}catch(Exception ge){
			logger.error("Runtime Exception", ge);
		}finally{
			PristineDBUtil.close(conn);
			logger.info("Product Grouping Analysis completed");
		}
	}
	
	private void doItemPairing(Connection conn, int chainId)throws GeneralException {
		logger.info("Performing Item pairing");
		
		//For a given chain, identify the pairs
		//For each item, setup the potential list of pairs
		// sub cat match, uom and size
		
		//For each item, get the item, uom and size. If uom or size is blank, move to the next
		// otherwise find the list that matches the item. 
		//Add them to the probable table
		
		try {
			pgdao.performCleanup(conn, chainId);
			CachedRowSet itemList = pgdao.getItemList(conn, chainId,  false, 0);
			int count = 0;
			ItemDTO item = new ItemDTO();
			while( itemList.next()){
				item.clear();
				item.deptID = PrestoUtil.getIntValue(itemList, "DEPT_ID");
				item.catID = PrestoUtil.getIntValue(itemList, "CATEGORY_ID"); 
				item.subCatID= PrestoUtil.getIntValue(itemList, "SUB_CATEGORY_ID");
				item.size = itemList.getString("ITEM_SIZE");
				item.uomId= itemList.getString("UOM_ID");
				item.itemCode= itemList.getInt("ITEM_CODE");
				item.upc = itemList.getString("STANDARD_UPC");
				item.manufactCode = itemList.getString("MANUFACT_CODE");
				
				if( item.subCatID> 0 && item.uomId != null && !item.uomId.equals("")
						&& item.size != null && !item.size.equals("")){
				
					count++;
					//if ( count > 10) break;
					//Get Like items
					ArrayList<ItemDTO> likeItemList = pgdao.getLikeItems( conn, item);
					//Insert the like items
					//logger.debug("like item size is " + likeItemList.size());
					Iterator<ItemDTO> likeItemItr = likeItemList.iterator();
					while (likeItemItr.hasNext()){
						ItemDTO likeItem = likeItemItr.next();
						pgdao.createPotentialLikeitems ( conn, chainId, item.itemCode, likeItem.itemCode);
					}
					
					if ( count%5000 == 0)
						logger.info( count + " items processed");
				}
			}
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
		
	}
	
	private void setupLikeItems(Connection conn, int chainId) throws GeneralException{
		//get a list of base distinct items
		
		logger.info("Setting product group ....");
		try {
			HashMap<Integer, String> prdGrpMap = new HashMap<Integer, String> ();
			pgdao.performCleanupLikeItemGroup(conn, chainId);
			CachedRowSet relatedItemCrs = pgdao.getDisinctBaseItems(conn, chainId);
			int count = 0;
			while ( relatedItemCrs.next()){
				prdGrpMap.clear();
				prdGrpMap.put( relatedItemCrs.getInt("BASE_ITEM_CODE"), "N");
				createProductGroup( conn, chainId, prdGrpMap);
				count++;
				//if ( count >= 10) break;
				if ( count% 2000 == 0) logger.info(count + " records processed.");
			}
			
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
	}
	
	private void createProductGroup( Connection conn, int chainId, 
			HashMap<Integer, String> prdGrpMap) throws GeneralException,SQLException {
		
		boolean pendingItems = true;
		int currentItemCode = -1;
		
		while (pendingItems){
			pendingItems = false;
			Iterator<Integer> itemItr = prdGrpMap.keySet().iterator();
			
			while (itemItr.hasNext()){
				currentItemCode = itemItr.next();
				String proccessedFlag = prdGrpMap.get(currentItemCode);
				if( !proccessedFlag.equals("Y")){
					pendingItems = true;
					break;
				}
			}
			
			if(pendingItems){
				//Get related item grps using union qry
				// Add it to the prdGrpMap as N if it does not exist
				prdGrpMap.put(currentItemCode, "Y");
				CachedRowSet crs = pgdao.getAllRelatedItems(conn, chainId, currentItemCode);
				while (crs.next()){
					int relatedItemCode = crs.getInt("ITEM_CODE");
					if( !prdGrpMap.containsKey(relatedItemCode)){
						prdGrpMap.put(relatedItemCode, "N");
					}
				}
			}
		}
		
		//Get the minimal item code and make it the base item
		int minItemCode = 999999;
		Iterator<Integer> itemItr = prdGrpMap.keySet().iterator();
		
		while (itemItr.hasNext()){
			int tempItemCode = itemItr.next();
			if (tempItemCode < minItemCode)
				minItemCode = tempItemCode;
		}
		
		//setup product group mapping
		itemItr = prdGrpMap.keySet().iterator();
		
		StringBuffer csItemCodes = new StringBuffer();
		int i = 0;
		int noofItems = prdGrpMap.size();
		while (itemItr.hasNext()){
			i++;
			int relatedItemCode = itemItr.next();
			
			if(noofItems>1){
				//logger.debug("Related Item pairs " + minItemCode+  " : " + relatedItemCode);
				pgdao.createLikeItemGroup(conn, chainId, minItemCode,relatedItemCode);
			}
			
			
			csItemCodes.append(relatedItemCode);
			if ( i < noofItems )
				csItemCodes.append(",");
		}
		
		// delete the items from potential like group
		pgdao.deleteAllRelatedItems(conn, chainId, csItemCodes.toString());
		
	}

	private void setupCompDataWithNoFlavor(Connection conn, int chainId, int checkListId, 
			String startDate, String endDate) throws GeneralException {
		logger.info("setting up Comp Data with no flavor ....");
			
		ScheduleDAO schDao = new ScheduleDAO();
		CompetitiveDataDAO compDataDao = new CompetitiveDataDAO (conn);
		
		ArrayList<ScheduleInfoDTO> schList = schDao.getSchedulesByChain(conn, chainId, checkListId, startDate, endDate);
		Iterator <ScheduleInfoDTO> itr = schList.iterator();
		int count = 0;
		
		while (itr.hasNext()){
			ScheduleInfoDTO schInfo = itr.next();
			logger.info("setting up No Flavor Comp Data for schedule - " + schInfo.getScheduleId());
			//Get the competitiveData items
			ArrayList<CompetitiveDataDTO> compDataList = compDataDao.getCompData(conn,schInfo.getScheduleId(), -1,-1, true);
			count++;
			logger.info("# of items - " + compDataList.size() + " ( Record " + count + " of " + schList.size() + ") ");
			// delete CompDataNoFlavor for that schedule
			pgdao.cleanupCompDataNoFlavor( conn, schInfo.getScheduleId());
			populateCompDataNoFlavor(conn, chainId, compDataList);
			//if (count == 1) break;
			
		}
		logger.info("Number of schedule checked - " + count);
		
	}
	
	private void populateCompDataNoFlavor(Connection conn, int chainId, ArrayList<CompetitiveDataDTO> compDataList) 
		throws GeneralException  {
		
		int flavorCount = 0;
		HashMap<Integer, CompetitiveDataDTO> populatedItemMap = new HashMap<Integer, CompetitiveDataDTO>();
		Iterator <CompetitiveDataDTO> dataListItr = compDataList.iterator();
		while ( dataListItr.hasNext()){
			CompetitiveDataDTO compData = dataListItr.next();
			int likeItemCode = pgdao.getPrestoLikeItemCode(conn,chainId, compData.itemcode);
			//Not like item, no flavor
			if( likeItemCode == -1){
				likeItemCode  = compData.itemcode;
			}
			if( !populatedItemMap.containsKey(likeItemCode)){
				populatedItemMap.put(likeItemCode, compData);
				pgdao.populateCompDataNoFlavor(conn,compData.checkItemId, likeItemCode);
			}else{
				flavorCount++;
			}
		}
		logger.info("Number of Flavors - " + flavorCount);
	}
	
	private void doTransferUnknownItem(Connection conn, int chainId) throws GeneralException {
		try {
			//Get the list of Unknown Items
			CachedRowSet unknownItemList = pgdao.getDisinctRelatedItemCode(conn, chainId);
			int count = 0;
			while( unknownItemList.next()){
				int unknownItemCode = unknownItemList.getInt("RELATED_ITEM_CODE");
				CachedRowSet relationShipCrs = pgdao.getItemHierarchy(conn, chainId, unknownItemCode);
				if( relationShipCrs.next()){
					
					ItemDTO item = new ItemDTO();
					item.deptID = relationShipCrs.getInt("DEPT_ID");
					item.catID = relationShipCrs.getInt("CATEGORY_ID");
					item.subCatID = relationShipCrs.getInt("SUB_CATEGORY_ID");
					item.itemCode = unknownItemCode;
					pgdao.updateItemHierarchy(conn, item);
				}

				count++;
				//if ( count > 10) break;
					
				if ( count%50 == 0)
					logger.info( count + " items processed");

			}
			logger.info( count + " items processed");
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
	}
	
	private int noOfObservations =0;
	private int noOfObswithNoFlavor =0;
	private int noOfObswith1Flavor=0;
	private int noOfObswith2Flavor =0;
	private int noOfObswith3Flavor =0;
	private int noOfObswith4Flavor =0;
	private int noOfObswithMoreFlavor =0;
	
	
	private void computeFlavorStats(Connection conn, int chainId, int checkListId, 
			String startDate, String endDate, boolean carriedOnly) throws GeneralException {
		logger.info("Calculating stats for no flavor ....");
			
		ScheduleDAO schDao = new ScheduleDAO();
		CompetitiveDataDAO compDataDao = new CompetitiveDataDAO (conn);
		
		ArrayList<ScheduleInfoDTO> schList = schDao.getSchedulesByChain(conn, chainId, checkListId, startDate, endDate);
		Iterator <ScheduleInfoDTO> itr = schList.iterator();
		
		int count = 0;
		
		while (itr.hasNext()){
			ScheduleInfoDTO schInfo = itr.next();
			logger.info("Computing schedule - " + schInfo.getScheduleId());
			//Get the competitiveData items
			ArrayList<CompetitiveDataDTO> compDataList = compDataDao.getCompData(conn,schInfo.getScheduleId(), -1,-1, true);
			count++;
			
			computeFlavorStatsForSchedule(conn, chainId,schInfo.getScheduleId(), compDataList, carriedOnly);
			logger.info("# of items - " + compDataList.size() + " ( Record " + count + " of " + schList.size() + ") " + "Schedule Id = " + schInfo.getScheduleId());
			//if (count == 1) break;
		}
		logger.info("Summary Stats  For Chain - " + chainId );
		if (carriedOnly)
			logger.info("Note - Carried Items Only");
		logger.info("Number of schedule checked - " + count);
		logger.info("Number of Observations - " + noOfObservations);
		logger.info("Number of Observations with No Flavors - " + noOfObswithNoFlavor);
		logger.info("Number of Observations with 1 Flavors - " + noOfObswith1Flavor);
		logger.info("Number of Observations with 2 Flavors - " + noOfObswith2Flavor);
		logger.info("Number of Observations with 3 Flavors - " + noOfObswith3Flavor);
		logger.info("Number of Observations with 4 Flavors - " + noOfObswith4Flavor);
		logger.info("Number of Observations with 5 or more Flavors - " + noOfObswithMoreFlavor);
	}
	
	private void computeFlavorStatsForSchedule(Connection conn, int chainId, int scheduleId, ArrayList<CompetitiveDataDTO> compDataList, boolean carriedOnly) 
	throws GeneralException  {
	

	HashMap<Integer, CompetitiveDataDTO> populatedItemMap = new HashMap<Integer, CompetitiveDataDTO>();
	Iterator <CompetitiveDataDTO> dataListItr = compDataList.iterator();
	while ( dataListItr.hasNext()){
		CompetitiveDataDTO compData = dataListItr.next();
		if( carriedOnly) {
			boolean isCarried = pgdao.isCarriedItem(conn, compData.itemcode);
			if (!isCarried) continue;
		}
		noOfObservations++;
		int likeItemCode = pgdao.getPrestoLikeItemCode(conn,chainId, compData.itemcode);
		//Not like item, no flavor
		if( likeItemCode == -1){
			likeItemCode  = compData.itemcode;
			noOfObswithNoFlavor++;
		}else if( !populatedItemMap.containsKey(likeItemCode)){
			populatedItemMap.put(likeItemCode, compData);
			int flavorCount = pgdao.getFlavorCount(conn, chainId, scheduleId, likeItemCode, carriedOnly);
			switch( flavorCount ){
				case 1:
					noOfObswith1Flavor++;
					break;
				case 2:
					noOfObswith2Flavor++;
					break;
				case 3:
					noOfObswith3Flavor++;
					break;
				case 4:
					noOfObswith4Flavor++;
					break;
				default:
					if( flavorCount > 0)
						noOfObswithMoreFlavor++;
					else{
						logger.error("Error - In correct Flavor stats Like item code = " +likeItemCode+
								" Schedule Id = " + scheduleId + "Chain ID = " + chainId);
					}
					break;
				}
			}
		}

	}
}
