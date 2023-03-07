package com.pristine.dataload.ahold;

import com.pristine.parsinginterface.PristineFileParser;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;






import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;



/* associated SQLs used with this program
alter table Item_lookup add PREPRICED_IND char(1)
alter table Item_lookup add ACTIVE_INDICATOR_OLD char(1)

update item_lookup set ACTIVE_INDICATOR_OLD = ACTIVE_INDICATOR

select count(*) from item_lookup where active_indicator = 'Y'

*/

public class ItemDetailsProcessor extends PristineFileParser {

	private static Logger logger = Logger.getLogger("ItemDetailsProcessor");

	private int recordCount = 0;
	private int stopCount = -1;
	private int updateCount = 0;
	ArrayList <ItemDTO> updateItemList = new ArrayList <ItemDTO> ();
	private Connection conn = null;
	 
	ItemDAO itemdao = new ItemDAO (); 
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-item-ahold.properties");
		PropertyManager.initialize("analysis.properties");
    	String subFolder = null;

		logger.info("ItemDetails Processor - Started");
		
		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
		}
    	
		ItemDetailsProcessor itemProcessor  = new ItemDetailsProcessor ();
		itemProcessor.processItemMaster(subFolder);
		

	}

	private void processItemMaster(String subFolder) {
		
		try 
		{
			ArrayList<String> fileList = getFiles(subFolder);
			
			conn  = getOracleConnection();
	    	String fieldNames[] = new String[26];
	    	
	    	setHeaderPresent(true);
	    	
	    	int i = 0;
	    	fieldNames[i] = "majDeptName";
	    	i++;
	    	
	    	fieldNames[i] = "majDeptCode";
	    	i++;
	    	
	    	fieldNames[i] = "deptName";
	    	i++;
	    	
	    	fieldNames[i] = "deptCode";
	    	i++;
	    	
    		fieldNames[i] = "deptShortName";
    		i++;

	    	fieldNames[i] = "catName";
	    	i++;
	    	
	    	fieldNames[i] = "catCode";
	    	i++;
	    	
	    	fieldNames[i] = "subCatName";
	    	i++;
	    	
	    	fieldNames[i] = "subCatCode";
	    	i++;
	    	
	    	fieldNames[i] = "segmentName";
	    	i++;
	    	
	    	fieldNames[i] = "segmentCode";  //11
	    	i++;
	    	
	    	
	    	fieldNames[i] = "retailerItemCode";
	    	i++;
	    	
	    	fieldNames[i] = "itemName";
	    	i++;
	    	
	    	fieldNames[i] = "size";
	    	i++;
	    	
	    	fieldNames[i] = "uom";
	    	i++;
	    	
	    	fieldNames[i] = "pack";
	    	i++;
	    	
	    	fieldNames[i] = "privateLabelCode";
	    	i++;
	    	
	    	fieldNames[i] = "likeItemGrp";
	    	i++;
	    	
	    	fieldNames[i] = "likeItemCode";
	    	i++;

	    	fieldNames[i] = "upc";   //20
	    	i++;
	    	
	    	fieldNames[i] = "levelType";
	    	i++;
	    	
	    	fieldNames[i] = "portfolio";
	    	i++;
	    	
	    	fieldNames[i] = "portfolioCode";
	    	i++;
	    	
	    	fieldNames[i] = "manufactName";
	    	i++;
	    
	    	fieldNames[i] = "brandName";
	    	i++;

	    	fieldNames[i] = "prePriceInd"; //26
	    	
	    	int ligItemCodeStart = Integer.parseInt(PropertyManager.getProperty("DATALOAD.LIG_ITEM_CODE_START", "920000"));
		    itemdao.updateActiveIndicatorFlagV2(conn, -1, ligItemCodeStart );

			for (int j = 0; j < fileList.size(); j++)
			{
	    		logger.info("processing - " + fileList.get(j));
	    		parseDelimitedFile(ItemDTO.class, fileList.get(j), '|',fieldNames, stopCount);
	    	}
			
			logger.info("# of records processed " + recordCount);
			logger.info("Array List size " + updateItemList.size());

			updateCount = itemdao.updateItemFlags( conn, updateItemList);
			logger.info("# of records made Active = " + updateCount );
			PristineDBUtil.commitTransaction(conn, "completed processing item flags");
		}catch(GeneralException e) 
		{
			logger.error("Exception in processing Item Details Records", e);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		}finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}
	
	

	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		for ( int jj = 0; jj <listobj.size();jj++)
		{
			ItemDTO itemDTO = (ItemDTO) listobj.get(jj);
			logger.debug(itemDTO.getItemName() +  " UPC - "  + itemDTO.getUpc() + " ICODE = " + itemDTO.getRetailerItemCode() + " PREIND " + itemDTO.getPrePriceInd()); 
			recordCount++;
			updateItemList.add(itemDTO);
		}

		
	}

}






