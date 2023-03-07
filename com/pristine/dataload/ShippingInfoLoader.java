/*
 * Title: Shipping Info Loader
 * Loads shipping information
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/09/2013	Ganapathy		Created
 *******************************************************************************
 */

package com.pristine.dataload;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ShippingInfoDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ShippingInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.MoveFiles;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ShippingInfoLoader extends PristineFileParser<ShippingInfoDTO>{
	private static Logger  logger = Logger.getLogger("ShippingInfoLoader");
	private Connection 	_Conn = null;
	private String filePath="";
	private List<ShippingInfoDTO> sIL = new ArrayList<ShippingInfoDTO>();
	ItemDAO itemDao = new ItemDAO();
	Hashtable<String,ShippingInfoDTO> uHash = new Hashtable<String,ShippingInfoDTO>();
	private Hashtable<String, Integer> _storeIdList = new Hashtable<String, Integer>();
	CompetitiveDataDAO 	objcompDataDAO;

	public ShippingInfoLoader(){
        try
		{
        	//Create DB connection
        	_Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	        objcompDataDAO= new CompetitiveDataDAO(_Conn);
	    }
		catch (GeneralException gex) {
	        logger.error(gex);
	    } catch (SQLException se) {
	        logger.error(se);
		}

	}
	
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-shipping-info-loader.properties");

        PropertyManager.initialize("analysis.properties");
        
        //Get the file path to locate the Excel file
        String filePath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
        if( !filePath.equals("")) {
			filePath = "/ShipmentData/";
        }
        else {
			logger.error("Invalid configuration: File path missing");
			System.exit(0);        	
        }
		
		String fileName = "";
		

		ShippingInfoLoader sILoader = new ShippingInfoLoader();
		
		sILoader.filePath = filePath;
		
		try {
			sILoader.loadShippingInfo();
			sILoader.processShippingInfo();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
		// Close the connection
		PristineDBUtil.close(sILoader._Conn);
	}

	private void loadShippingInfo() {

		try {
			List<String> zipFileList = getZipFiles(filePath);
			System.out.println(zipFileList.size());
		    String[] fieldNames = new String[10];
		    fieldNames[0] = "_invoiceDate";
		    fieldNames[1] = "_itemDesc"; // Dummy
		    fieldNames[2] = "_storeNum"; 
		    fieldNames[3] = "_shipmentDate";
		    fieldNames[4] = "_itemDesc"; // Dummy
		    fieldNames[5] = "_itemDesc";
		    fieldNames[6] = "_retailerCode";
		    fieldNames[7] = "_upc";
		    fieldNames[8] = "_storePack";
		    fieldNames[9] = "_casesShipped";
			
			for(String zF:zipFileList){
			if(!zF.contains("pristine_whse_shpmt_")){
				continue;
			}
			PrestoUtil.unzip(zF, getRootPath()+filePath) ;

			List<String> fileList = getFiles(filePath);
			for (int i = 0; i < fileList.size(); i++) {
				
			    String file = fileList.get(i);
			    System.out.println(file);
			    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			    
			    
				List<ShippingInfoDTO> records = parseDelimitedFile(ShippingInfoDTO.class, file, '|', fieldNames, 0);
				System.out.println(records.size());
			    processRecords(records);
			    
			    File f = new File(file);
			    try{f.delete();}catch(Exception e){e.printStackTrace();}
			}
			File zipf = new File(zF);
			zipf.renameTo(new File(getRootPath()+filePath+"/completed/"+zipf.getName()));
			}
		} catch (GeneralException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}	
		
	}
	
	private void processShippingInfo() throws GeneralException{
		ShippingInfoDAO sIDAO = new ShippingInfoDAO(); 
		sIDAO.insertShippingInfoWrap(_Conn, sIL);		
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		for(ShippingInfoDTO s: (List<ShippingInfoDTO>)listobj){
			
			try{
				s.setStoreID(getCompStore(s.getStoreNum()));
			}catch(Exception e){
				e.printStackTrace();
			}
			
			ItemDTO itemIN = new ItemDTO();
			itemIN.setUpc(s.getUPC());
			ItemDTO item = itemDao.getItemDetails(_Conn,itemIN);
			if(item!=null && item.getItemCode() != -1){
				if(item.getDeptID() != 19){
					 Calendar cal = Calendar.getInstance();
				        cal.setTime(s.getShipmentDate());
				        cal.add(Calendar.DATE, 1); //minus number would decrement the days
				        cal.getTime();
					s.setShipmentDate(cal.getTime());
				}
				s.setItemCode(item.getItemCode());
				s.setQuantity(s.getStorePack()*s.getCasesShipped());
				//if not in unique hash then insert, else sum
				String uKey = s.getItemCode()+":"+s.getStoreID()+":"+s.getShipmentDate();
				if(uHash.containsKey(uKey)){
					ShippingInfoDTO sI0 = uHash.get(uKey);
					sI0.setCasesShipped(sI0.getCasesShipped()+s.getCasesShipped());
					sI0.setQuantity(sI0.getQuantity()+s.getQuantity());
				}else{
					uHash.put(uKey, s);
					sIL.add(s);
				}
			}	
		
		}
		
	}

	@Override
    protected Field setFieldValue(Field field, Object dtoObject, String value) 
    throws GeneralException {
		
        try {
        	if(field.getName().compareToIgnoreCase("_storeNum")==0){
            	if( value!= null && !value.equals(""))
            		field.set(dtoObject, value.substring(7));
            	else
            		field.set(dtoObject, "");
        		
        	} else if (field.getName().compareToIgnoreCase("_upc")==0) {
            	if( value!= null && value.length()>=12)
            		field.set(dtoObject, value.substring(value.length()-12));
            	else
            		field.set(dtoObject, "");
            	
            } else if (field.getType() == String.class) {
                field.set(dtoObject, value);
            } else if (field.getType() == float.class) {
            	if( value!= null && !value.equals(""))
            		field.setFloat(dtoObject, Float.parseFloat(value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)));
            	else
            		field.setFloat(dtoObject, 0);
            	
            } else if (field.getType() == double.class) {
            	if( value!= null && !value.equals(""))
            		field.setDouble(dtoObject, Double.parseDouble(value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)));
            	else
            		field.setDouble(dtoObject, 0);
            		
            } else if (field.getType() == Date.class) {
            	if( value!= null && !value.equals("")){
            		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            		field.set(dtoObject, df.parse(value));
            	}
            	else{
            		field.set(dtoObject, new Date());
            	}
            		
            }
            else if (field.getType() == int.class) {
            	if( value!= null && !value.equals(""))
            		field.setInt(dtoObject, Integer.parseInt(value.replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY)));
            } else if (field.getType() == boolean.class) {
                field.setBoolean(dtoObject, Boolean.parseBoolean(value));
            }
        } catch (Exception e) {
        	throw new GeneralException( "File Parser - Setting Values", e);
        }
        return field;
    }

    private int getCompStore(String storeNum)
    {
    	int storeID = -1;
		try {
			
			/*Check the input Store Number exist in the collection 
			  if exist then get the Store ID from the collection */
			if ((_storeIdList.size() > 0 ) && (_storeIdList.containsKey(storeNum))) {
				
				storeID = _storeIdList.get(storeNum);
				//logger.debug("Get Store Number form collection....");
				
			}
			else {
				
				//logger.debug("Get store Number from DB............");
				//Call the DAO to get the Store ID 
				ArrayList<String> storeIdList = objcompDataDAO.getStoreIdList(_Conn, storeNum);
				if (storeIdList.size() >0 ) {
					//Assign the Store ID into private variable
					storeID = Integer.parseInt(storeIdList.get(0));
					//Store the Store Id in the collection with Store Number as key
					_storeIdList .put(storeNum, storeID);
				}
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}    		
    return storeID;
    }
	
}