package com.pristine.dataload.prestoload;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ProductGroupInitialSetup {
	
	private Connection conn;
	private static Logger logger = Logger.getLogger("ProductGroupSetup");
	
	public static void main(String[] args) {
		ProductGroupInitialSetup setup = new ProductGroupInitialSetup();
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-product-group-setup.properties");
		setup.loadItemsFromItemLookup();
	}
	
	private void loadItemsFromItemLookup(){
		try{
			conn = DBManager.getConnection();
			
			ItemDAO itemDAO = new ItemDAO();
			List<ItemDTO> itemDTOList = itemDAO.getItemLookup(conn);
			
			logger.info("No of items to be setup - " + itemDTOList.size());
			
			int recordCount = 0;
			for(ItemDTO itemDto : itemDTOList){
				ProductGroupDAO productDAO = new ProductGroupDAO();
				productDAO.populateProductGroupHierarchy(conn, itemDto);
				recordCount++;
				
				if(recordCount % 25000 == 0){
					logger.info("No of records processed - " + recordCount);
					PristineDBUtil.commitTransaction(conn, "Product Group Setup Complete");
				}
			}
			
			logger.info("No of records processed - " + recordCount);
			logger.info("Product Group Setup Complete");
			PristineDBUtil.commitTransaction(conn, "Product Group Setup Complete");
		}catch(GeneralException exception){
			logger.error("Error in Product Group Initial Setup - " + exception.toString());
			exception.printStackTrace();
		}
	}
}
