package com.pristine.dataload.riteaid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.riteaid.PostLIGSetupDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PostLIGSetup extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("PostLIGSetup");
	
	private Connection conn = null;
	
	public PostLIGSetup(){
		conn = getOracleConnection();
    }
	public static void main(String[] args) throws GeneralException {
		
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-post-lig-setup.properties");
		
		PostLIGSetup postLIGSetup = new PostLIGSetup();
		postLIGSetup.processRecords();
		
		}
	
		@SuppressWarnings("null")
		private void  processRecords() throws GeneralException{
			PostLIGSetupDAO postLigSetupdao = new PostLIGSetupDAO();
			List<ItemDTO> allItemsMapWithOriginalItemCode = postLigSetupdao.getAllActiveItemsWithOriginalItemCode(conn);
			
			HashMap<String, List<ItemDTO>> groupByOrigItemCode = (HashMap<String, List<ItemDTO>>) allItemsMapWithOriginalItemCode
					.stream().collect(Collectors.groupingBy(ItemDTO::getOrgItemCode));
			
			List<ItemDTO> ItemsNeedstobeUpdated = new ArrayList<ItemDTO>();
			
			//Looping all the Original Item Codes
			groupByOrigItemCode.forEach((orginalItemCode, itemList) -> {
				
				if(itemList.size() > 1)
				{
				int retLirid = 0;
				int itmCode = 0;
				boolean isPrestoLirInd = false;
				
				boolean itemsWithLir = false;
				 
				//Looping single Original Item Code.
				for(ItemDTO item: itemList){
					if(item.getLikeItemId() > 0){
						itemsWithLir = true;
						retLirid = item.getLikeItemId();
						itmCode = item.getItemCode();
						isPrestoLirInd = item.getPrestoAssignedLirInd();
						break;
					}
				}
				
				// If all the items are mapped with LIR, then this can be skipped.
				if(!itemsWithLir){
				}
				
				
				// If one item is mapped with LIR, then set the same RET_LIR_ID for other items and update PRESTO_ASSIGNED_LIR_IND as 'Y'
				if(itemsWithLir){
					for(ItemDTO item: itemList){
						if (item.getItemCode() != itmCode) // The item code which is mapped with ret lir id, other than that remaining shd get updated.
						{
							if(item.getLikeItemId() != retLirid)
							{
								item.setLikeItemId(retLirid);
								item.setPrestoAssignedLirInd(true);
							}
						}
						else
						{
							item.setLikeItemId(retLirid);
							item.setPrestoAssignedLirInd(isPrestoLirInd);
						}
					}
				}
				 
				//If the items are not mapped with LIR
				boolean itemsWithMissingLir = true;
				String itemName = "";
				
				//Looping single Original Item Code
				for(ItemDTO item: itemList){
					
					if(item.getActItemCode().equals(item.getRetailerItemCode()))
					{
						itemName = item.getItemName();
					}
					
					if(item.getLikeItemId() > 0){
						itemsWithMissingLir = false;
					}
				}
				
				if(itemsWithMissingLir)
				{
					String AddLIRCode = "ORG-" + orginalItemCode;
					String AddLIRName = itemName + "-ORG-" + orginalItemCode;
					int likeItemId = 0;
					try {
						likeItemId = postLigSetupdao.setupRetailerLikeItem(conn, AddLIRCode, AddLIRName);
					} catch (Exception|GeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					for(ItemDTO item: itemList){
						 item.setLikeItemId(likeItemId);
						 item.setPrestoAssignedLirInd(true);
						 item.setLirInd(true);
					}
				}
		
					ItemsNeedstobeUpdated.addAll(itemList);
				}
			});
			
		    //logger.debug("ItemsNeedstobeUpdated Count" + ItemsNeedstobeUpdated);
		    
		    try {
		    	postLigSetupdao.updateLikeItemIdAndPrestoLirInd(conn, ItemsNeedstobeUpdated);
		    	PristineDBUtil.commitTransaction(conn, "POST LIG Setup");
		    } catch (Exception|GeneralException e) {
				PristineDBUtil.rollbackTransaction(conn, "Error");
				logger.error("Error while executing UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND");
				throw new GeneralException("Error while executing UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND " + e);
			} finally {
			}
	 	}
		@Override
		public void processRecords(List listobj) throws GeneralException {
			// TODO Auto-generated method stub
			
		}

}
