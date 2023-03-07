package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PerishablesTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");

	/**
	 * @param args
	 * @throws OfferManagementException 
	 */
	public static void main(String[] args) throws OfferManagementException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		PerishablesTest perishablesTest = new PerishablesTest();
		perishablesTest.intialSetup();
		//perishablesTest.testProdGrpProperties();
		perishablesTest.testOrderCodeRecommendation();
	}
	
	private void testOrderCodeRecommendation() throws OfferManagementException{	
		Integer errorCode = RecommendationErrorCode.GENERAL_EXCEPTION.getErrorCode();
		List<ProductDTO> products = new ArrayList<ProductDTO>();
		List<Integer> locationList = new ArrayList<Integer>();
		List<PRProductGroupProperty> productGroupProperties;
		ArrayList<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		int runId = 2641;
		int locationLevelId = PRConstants.ZONE_LEVEL_TYPE_ID;
		int locationId = 66;
		
		ProductDTO productDTO = new ProductDTO();
		productDTO.setProductLevelId(4);
		productDTO.setProductId(558);
		products.add(productDTO);
		
		locationList.add(locationId);
		productGroupProperties = testProdGrpProperties(products); 
		
		PRItemDTO prItem = new PRItemDTO();
		prItem.setItemCode(137399);
		prItem.setChildLocationLevelId(locationLevelId);
		prItem.setChildLocationId(locationId);
//		prItem.setRecommendedRegPrice(10.31);	
		prItem.setRecommendedRegPrice(new MultiplePrice(1, 10.31));
		itemListWithRecPrice.add(prItem);
		
		prItem = new PRItemDTO();
		prItem.setItemCode(110847);
		prItem.setChildLocationLevelId(locationLevelId);
		prItem.setChildLocationId(locationId);
//		prItem.setRecommendedRegPrice(3.31);		
		prItem.setRecommendedRegPrice(new MultiplePrice(1, 3.31));
		itemListWithRecPrice.add(prItem);
		
		prItem = new PRItemDTO();
		prItem.setItemCode(117543);
		prItem.setChildLocationLevelId(locationLevelId);
		prItem.setChildLocationId(locationId);
//		prItem.setRecommendedRegPrice(8.31);	
		prItem.setRecommendedRegPrice(new MultiplePrice(1, 8.31));
		itemListWithRecPrice.add(prItem);
		
		try {
//			new PricingEngineWS().recommendOrderCodePrice(conn, errorCode, products, locationLevelId, 
//					locationList, productGroupProperties, itemListWithRecPrice);
			
			try {
				PristineDBUtil.commitTransaction(conn, "Commit Price Recommendation");
			} catch (GeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int a = 10;
			if(a == 10){
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private List<PRProductGroupProperty> testProdGrpProperties(List<ProductDTO> products) throws OfferManagementException{		
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<PRProductGroupProperty> productGroupProperties = null;
	  
		
		try {
			productGroupProperties = pricingEngineDAO.getProductGroupProperties(conn, products);
		 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return productGroupProperties;
	}
	
	 
	
	public void intialSetup() {
		initialize();
	}

	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
