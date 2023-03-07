/*
 * Title: TOPS  Product Group Loader
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 19-04-2012	    Dinesh Kumar V	     Initial  Version 
 *******************************************************************************
 */
package com.pristine.dataload.salesanalysis;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ProductGroupLoader {

    static Logger logger =  Logger.getLogger("ProductGroupLoader");
    Connection _conn     =  null;    // hold the database connection
    String  _filePath    = null;     // hold the input file path
    String _fileName     = null;     // hold the input file name
    String _productId    = null;     // hold the process productId
	
    /*
     * Class Constructor 
     * Intialize the database connection , input validation
     * Argument 1 : FilePath
     * Argument 2 : FileName
     * Argument 3 : ProductId
     * @catch gendral Exception
     */
	public ProductGroupLoader(String filePath, String fileName, 
			String productId) {

		// check the input parameters
		// check filepath null or not
		if (filePath == null) {
			logger.error("Process file path is missing");
			System.exit(1);
		}

		// check filename null or not
		if (fileName == null) {
			logger.equals("Process file name is missing");
			System.exit(1);
		}

		// check product id null or not
		if (productId == null) {
			logger.error("Process product id missing");
			System.exit(1);
		}
		
		// check the given file excel or not
		String fileExtension = fileName.substring(fileName.lastIndexOf("."),fileName.length()).trim();
		if( ! fileExtension.toUpperCase().equalsIgnoreCase(".XLS")){
			logger.error("File Format Error");
			System.exit(1);
		}
		
		_filePath = filePath;
		_fileName = fileName;
		_productId = productId;
						
		try{
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();
			
		}catch(GeneralException exe){
			logger.error(exe.getMessage());
		}
	}

	/*
	 * Main Method of the batch.method mainly used get the intial arguments .
	 * Argument 1 : 
	 * Argument 2 : 
	 * call the class constructor and intialize the db connection
	 * @catch Exception
	*/

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-productgroup-loader.properties");
		logger.info("Product-group-loader starts");
		logCommand(args);
		String filePath = null; // hold the input file path
		String fileName = null; // hold the input file name
		String productId = null; // hold the process product id
		ProductGroupLoader productLoader = null;

		try {

			// get the file path and file name from user
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];
				
				
				// get the file path
				if (arg.startsWith("FILEPATH")) {
					filePath = arg.substring("FILEPATH=".length());
				}

				// get the file name
				if (arg.startsWith("FILENAME")) {
					fileName = arg.substring("FILENAME=".length());
				}

				// get the process productId
				if (arg.startsWith("PRODUCTID")) {
					productId = arg.substring("PRODUCTID=".length());
				}

			}
			
			// call the class constructor
			productLoader = new ProductGroupLoader(filePath, fileName,
					productId);
			// method used to read the excel file and insert the values into table			
			productLoader.loadProductGroup();

		} catch (Exception exe) {
			logger.error(exe.getMessage());
		} finally {
			try {
				PristineDBUtil.close(productLoader._conn);
			} catch (Exception exe) {
				logger.error(exe.getMessage());
			}

		}

	}
	
	/*
	 * method mainly used read the input excel file and insert the excel values into
	 * table
	 * Argument 1 : File Name
	 * Argument 2 : File Path
	 * Argument 3 : Product Id
	 * Argument 4 : Connection
	 * @catch FileNotFoundException ,Exception
	 */
	
	private void loadProductGroup() {

		// hold the productList
		List<ProductDTO> productList = new ArrayList<ProductDTO>();

		// Check the file avilability
		File inputFile = new File(_filePath + "\\" + _fileName);
		try {
			if (inputFile.exists()) {

				// read the excel file
				FileInputStream inputFileStream = new FileInputStream(_filePath
						+ "\\" + _fileName);
				HSSFWorkbook inputExcel = new HSSFWorkbook(inputFileStream);

				HSSFSheet sheet = inputExcel.getSheet("Portfolio Mapping");
				int rows = sheet.getPhysicalNumberOfRows();
				for (int ii = 1; ii < rows; ii++) {

					// object for ProductDTO
					ProductDTO objProductDto = new ProductDTO();

					// get the Row
					HSSFRow row = sheet.getRow(ii);

					// get the cell values
					HSSFCell productId = row.getCell(0);
					// HSSFCell majorCategory = row.getCell(1);
					HSSFCell childId = row.getCell(2);
					// HSSFCell category = row.getCell(3);
					// HSSFCell catManger = row.getCell(4);
					objProductDto.setProductId((int) productId
							.getNumericCellValue());
					objProductDto.setChildProductId((int) childId
							.getNumericCellValue());
					productList.add(objProductDto);
			}
				
			// find the product hierarchy level and child level
				
				

			} else {
				logger.error("File not exist" + _filePath + "\\" + _fileName);
			}
		} catch (FileNotFoundException exe) {
			logger.error(exe.getMessage());
		} catch (IOException exe) {
			logger.error(exe.getMessage());
		}

	}
		 
	/**
	 *****************************************************************
	* Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args)
    {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Product Group Loader ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }

}
