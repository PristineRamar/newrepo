package com.pristine.analytics.substitutes;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.analytics.SubstitutesDAO;
import com.pristine.dao.analytics.SubstitutePair;

//import com.pristine.dao.StoreDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class Substitutes {
	
	static Logger logger = Logger.getLogger("Substitutes");
	private static final int PRODUCT_LEVEL_ID_LIG_OR_ITEM = 111;
	private static final String OP_FIND = "FIND";
	private static final String OP_MERGE = "MERGE";
	
	private Connection _conn = null; // DB connection
	
	//private String storeNum = null; // Processing store number
	private String analysisEndDate = null;
	private int locationLevelId = -1;
	private int locationId = -1;
	private int categoryId = -1; 
	private int subcategoryId = -1;
	private int productId = -1;
	private int substituteLevelId = -1;
	private String mergeLocationIds = null;
	

	
	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-substitute-analysis.properties");

		logger.info("main() - Started");		
		// logger.info("main() - Comment: Modified to support substitutes generation at CATEGORY Level.");
		// logger.info("main() - Comment: Modified to support at DIVISION level.");
		// logger.info("main() - Comment: Modified table name to product_substitutes.");
		// logger.info("main() - Comment: Modified SQL using WITH clause for Performance");
		// logger.info("main() - Comment: aonly_cst_count and bonly_cst_count calc modified");
		// logger.info("main() - Comment: tlog table is configurable.  Configurable items renamed.");
		// logger.info("main() - Comment: Temporary tables created dynamic and used.  Yet to be tested at item level.");
		// logger.info("main() - Comment: Added condition customer_id > 0.");
		// logger.info("main() - Comment: Modified score criteria thresholds.");
		// logger.info("main() - Comment: Added additional revenue columns and calcs.");
		// logger.info("main() - Comment: Skipped suspected customer Ids based on high $ net_amt and high trx_cnt.");
		logger.info("main() - Comment: Item can be substitute of LIG and vice versa.");
		logger.info("main() - Comment: Substitutes can be grouped.");
		logger.info("main() - Comment: Merge two market areas.");
		logger.info("main() - Comment: aawab_cst_pct_score and aawab_aandb_li_pct_score slabs changed for 'Not a Substitute'");
		logger.info("main() - Comment: Size and Price Score when -1 will not be treated as substitute.");
		logger.info("main() - Comment: Issue Fixed: Quantity changed to Quantity + Weight!  UOM is compared.");
		logger.info("main() - Comment: Scoring modified for price, size and grouping.");
		logger.info("main() - Comment: 2015-05-06: At customer level, his sub-category's purchases are not considered if the number of trx in it is <= 3.");
		logger.info("main() - Comment: 2015-05-15: Group Item type is stored in PRODUCT_SUBSTITUTES_1 table");
		logger.info("main() - Comment: 2015-07-07: Table renamed back to PRODUCT_SUBSTITUTES.");
		
		logCommand(args);
		
		String operationId = "";
		String strAnalysisDate = "";
		//String storeNum = "";
		String locationLevelId = "";
		String locationId = "";
		String categoryId = "";
		String subCategoryId = "";
		String substituteLevelId = "";
		String mergeLocationIds = "";
		
		Substitutes sA = new Substitutes();
		
		logger.info("main() - Substitutes object created.");
		logger.info("main() - Package.SpecificationTitle=" + sA.getClass().getPackage().getSpecificationTitle());
		logger.info("main() - Package.SpecificationVersion=" + sA.getClass().getPackage().getSpecificationVersion());

		try {
			
			// Read the 5 input parameters 
			
			//Example LOCATION_LEVEL_ID=5 LOCATION_ID=6083 ANALYSIS_DATE=08/02/2014 SUBSTITUTELEVEL_ID=2 CAT_ID=28
			
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
				
				// Get the operation
				if (arg.startsWith("OP")) {
					operationId = arg.substring("OP=".length());
				}

				// Get the Location type from command line
				if (arg.startsWith("LOCATION_LEVEL_ID")) {
					//storeNum = arg.substring("STORE_ID=".length());
					locationLevelId = arg.substring("LOCATION_LEVEL_ID=".length());
				}

				// Get the Location from command line
				if (arg.startsWith("LOCATION_ID")) {
					//storeNum = arg.substring("STORE_ID=".length());
					locationId = arg.substring("LOCATION_ID=".length());
				}

				// Get the Category Id from command line
				if (arg.startsWith("CAT_ID")) {
					categoryId = arg.substring("CAT_ID=".length());
				}

				// Get the Sub-category Id from command line
				if (arg.startsWith("SUBCAT_ID")) {
					subCategoryId = arg.substring("SUBCAT_ID=".length());
				}

				// Get the Analysis Date from command line
				if (arg.startsWith("ANALYSIS_DATE")) {
					strAnalysisDate = arg.substring("ANALYSIS_DATE=".length());
				}
				
				// Get the product level at which substitute is to be found
				if (arg.startsWith("SUBSTITUTELEVEL_ID")) {
					substituteLevelId = arg.substring("SUBSTITUTELEVEL_ID=".length());
				}
				
				// Get the Location Ids to be merged
				if (arg.startsWith("MERGE_LOCATION_IDS")) {
					mergeLocationIds = arg.substring("MERGE_LOCATION_IDS=".length());
				}

			}
			
			if ( locationLevelId.length() > 0 ) {
				sA.locationLevelId = Integer.parseInt(locationLevelId);
			}
			if ( locationId.length() > 0 ) {
				sA.locationId = Integer.parseInt(locationId);
			}
			
			if ( operationId.equals(OP_FIND) ) {
				if ( categoryId.length() > 0 ) {
					sA.categoryId = Integer.parseInt(categoryId);
				}
				if ( subCategoryId.length() > 0 ) {
					sA.subcategoryId = Integer.parseInt(subCategoryId);
				}

				if ( substituteLevelId.length() > 0 ) {
					sA.substituteLevelId = Integer.parseInt(substituteLevelId);
				}
				sA.analysisEndDate = strAnalysisDate;
				
				sA.doSubstituteAnalysis();
			}
			if ( operationId.equals(OP_MERGE) ) {
				if ( mergeLocationIds.length() > 0 ) {
					sA.mergeLocationIds = mergeLocationIds;
				}
				
				sA.mergeSubstitutes();
			}
				
			
			

		} catch (Exception e) {
			logger.error("main() - " + e.getMessage());

		} finally {
			PristineDBUtil.close(sA._conn);
			logger.info("main() - Ended");
		}

	}

	
	public Substitutes(){
		try {
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Substitutes() - " + exe);
			System.exit(1);
		}

	}
	
	
	public void doSubstituteAnalysis(){
		logger.info("doSubstituteAnalysis() - Started");
		try
		{
			// Validations
			//StoreDAO strdao = new StoreDAO();
			
			// Get store Id corresponding to store number.
			//locationId  = strdao.getlocationId(_conn, storeNum, null, -1);
			if( locationLevelId <= 0){
				logger.error("doSubstituteAnalysis() - Invalid Location Level Id!");
				throw new GeneralException ("Invalid Location Level Id!");
			}			
			if( locationId <= 0){
				logger.error("doSubstituteAnalysis() - Invalid Location Id!");
				throw new GeneralException ("Invalid Location Id!");
			}			

		    String batchId = getBatchId();		    

		    // Check whether substitute should be found at item level  
		    if (substituteLevelId == 0 || substituteLevelId == Constants.ITEMLEVELID)
		    {
		    	logger.info("doSubstituteAnalysis() - Invoke Processing at ITEM Level.");
		    	doSubstituteAnalysis(batchId, Constants.ITEMLEVELID); // Item level
		    }
		    // Check whether substitute should be found at LIG level		    
		    if (substituteLevelId == 0 || substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG)
		    {
			    logger.info("doSubstituteAnalysis() - Invoke Processing at LIG Level.");
				doSubstituteAnalysis(batchId, Constants.PRODUCT_LEVEL_ID_LIG); // LIG level
		    }			
		    // Check whether substitute should be found at LIG level		    
		    if (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM)
		    {
			    logger.info("doSubstituteAnalysis() - Invoke Processing at LIG/Item Level.");
				doSubstituteAnalysis(batchId, PRODUCT_LEVEL_ID_LIG_OR_ITEM); // LIG/Item level
		    }		    
		    
		}catch(GeneralException e){
			logger.error("doSubstituteAnalysis() - GenralException=" + e.getMessage());
			e.printStackTrace();
		}catch(Exception e){
			logger.error("doSubstituteAnalysis() - Exception=" + e.getMessage());
			e.printStackTrace();
		}finally {
			logger.info("doSubstituteAnalysis() - Ended");
		}
	}
	
	public void mergeSubstitutes(){
		logger.info("mergeSubstitute() - Started");
		try
		{
			if( locationLevelId <= 0){
				logger.error("mergeSubstitute() - Invalid Location Level Id!");
				throw new GeneralException ("Invalid Location Level Id!");
			}			
			if( locationId <= 0){
				logger.error("mergeSubstitute() - Invalid Location Id!");
				throw new GeneralException ("Invalid Location Id!");
			}			

		    String batchId = getBatchId();		    
		    
		    logger.info("mergeSubstitute() - batchId=" + batchId);
		    
		    SubstitutesDAO substitutesData = new SubstitutesDAO();
		    substitutesData.conn = _conn;
		    substitutesData.batchId = batchId;
		    substitutesData.setLocation(locationLevelId, locationId);		    
		    
		    boolean result = false;
		    result = substitutesData.mergeSubstitutesData(mergeLocationIds);

		    // Store the substitute pairs		    
		    result = substitutesData.storeProductSubstitutes();
		    
		    // Group the substitutes and store
		    groupSubstitutes(substitutesData);

		    
		}catch(GeneralException e){
			logger.error("mergeSubstitute() - GenralException=" + e.getMessage());
			e.printStackTrace();
		}catch(Exception e){
			logger.error("mergeSubstitute() - Exception=" + e.getMessage());
			e.printStackTrace();
		}finally {
			logger.info("mergeSubstitute() - Ended");
		}
	}
	
	private String getBatchId()
	{
		// Generate unique id for every batch RUN.
	    Date dNow = new Date();
	    SimpleDateFormat ft = new SimpleDateFormat ("yyMMddHHmm");		    
	    String batchId = ft.format(dNow);
	    
	    return batchId;
	}
	
	private void doSubstituteAnalysis(String batchId, int substituteLevelId){
		logger.info("doSubstituteAnalysis() - Started.  batchId=" + batchId + ", substituteLevelId=" + substituteLevelId);
		
		try{
		    String itemFilter = null;
		    boolean result = false;
		    
		    SubstitutesDAO substitutesData = new SubstitutesDAO();
		    
		    substitutesData.conn = _conn;
		    substitutesData.batchId = batchId;
		    substitutesData.analysisEndDate = analysisEndDate;
		    substitutesData.setLocation(locationLevelId, locationId);		    
		    substitutesData.setProperties(substituteLevelId);

		    String substituteBrand = null;
		    int brandId = -1;			    
		    substituteBrand = PropertyManager.getProperty("SUBS_SUBSTITUTE_BRAND");
		    if (! substituteBrand.isEmpty()) {
		    	brandId = substitutesData.getBrandId(substituteBrand);
		    }
		    substitutesData.brandId = brandId;

		    String updateOutlierCustomerIds = PropertyManager.getProperty("SUBS_UPDATE_OUTLIER_CUSTOMER_IDS");
		    
		    if (updateOutlierCustomerIds.equalsIgnoreCase("Y")) {
		    	result = substitutesData.prepareIneligibleCustomerIds(substituteLevelId);		    	
		    }		    
		    
		    // Processing
		    if (subcategoryId > 0)
		    {
		    	// Process for the sub category id only.
		    	itemFilter = " sub_category_id = " + subcategoryId + " ";
		    	
		    	logger.info("doSubstituteAnalysis() - Preparing data for subcategoryId=" + subcategoryId + "..");
		    	
		    	result = substitutesData.prepareEligibleProductsData(substituteLevelId, itemFilter);
		    	result = substitutesData.prepareEligibleTrxData(substituteLevelId, itemFilter);
		    	
		    	logger.info("doSubstituteAnalysis() - Processing for subcategoryId=" + subcategoryId + "..");		    	
		    	result = substitutesData.storeProductPairDetail(substituteLevelId, itemFilter);
		    	//result = substitutesData.getSubstitutes(substituteLevelId, itemFilter);
		    }
		    else if (categoryId > 0)
		    {
		    	// Process for the category id only.
		    	itemFilter = " category_id = " + categoryId + " ";
		    	
		    	logger.info("doSubstituteAnalysis() - Preparing data for categoryId=" + categoryId + "..");
		    	
		    	result = substitutesData.prepareEligibleProductsData(substituteLevelId, itemFilter);
		    	result = substitutesData.prepareEligibleTrxData(substituteLevelId, itemFilter);

		    	logger.info("doSubstituteAnalysis() - Processing for categoryId=" + categoryId + "..");
		    	result = substitutesData.storeProductPairDetail(substituteLevelId, itemFilter);
		    	//result = substitutesData.getSubstitutes(substituteLevelId, itemFilter);
		    }
		    else
		    {
				String categoryIds = null;
				categoryIds = PropertyManager.getProperty("SUBS_CATEGORYID_LIST");
				if ( categoryIds != null )
				{
					logger.info("doSubstituteAnalysis() - Properties Info: SUBS_CATEGORYID_LIST=" + categoryIds);
					itemFilter = " category_id IN (" + categoryIds + ") ";								
				}
		    	// Process for all active categories.
		    	List<Integer> productIds = substitutesData.getEligibleCategories(brandId, categoryIds);
		    	int productCount = productIds.size();
		    	
		    	logger.info("doSubstituteAnalysis() - Preparing data for all categories..");
		    	result = substitutesData.prepareEligibleProductsData(substituteLevelId, itemFilter);
		    	

		    	logger.info("doSubstituteAnalysis() - Processing for categories.. categories_size=" + productIds.size());
		    	for (int i = 0; i < productCount; i++)
		    	{
		    		productId = productIds.get(i);
		    		/*itemFilter = " segment_id = " + productId + " ";*/
		    		//  Analysis to find substitutes in other categories also 
		    		itemFilter = " category_id = " + productId + " ";
		    		
		    		result = substitutesData.prepareEligibleTrxData(substituteLevelId, itemFilter);
		    		
		    		logger.info("doSubstituteAnalysis() - Processing " + (i + 1) + " of " + productCount + " for categoryid=" + productId + "..");
		    		
		    		// (1) Analysis, (2) filters and (3) stores the substitutes in database
		    		result = substitutesData.storeProductPairDetail(substituteLevelId, itemFilter);
		    		//result = substitutesData.getSubstitutes(substituteLevelId, itemFilter);
		    		
		    		if (!result)
		    			logger.info("doSubstituteAnalysis() - FAILED for categoryid=" + productId + "!");
		    	}
		    }

		    // Store the substitute pairs		    
		    result = substitutesData.storeProductSubstitutes();
		    
		    // Group the substitutes and store
		    groupSubstitutes(substitutesData);

		    //_conn.close();	??? Is this ok?
		    
		}catch(Exception e){
			logger.error("doSubstituteAnalysis() - Exception=" + e.getMessage());
			e.printStackTrace();
		}
		catch (GeneralException e) {
	    	logger.error("doSubstituteAnalysis() - GeneralException=" + e.getMessage());
			e.printStackTrace();
	    }
		finally {
			logger.info("doSubstituteAnalysis() - Ended.  batchId=" + batchId + ", substituteLevelId=" + substituteLevelId);			
		}
	}
	
	
	public void groupSubstitutes(SubstitutesDAO substitutesData)
	{
		logger.info("groupSubstitutes() - Started");
		try
		{
		
			ArrayList<SubstitutePair> pairs = new ArrayList<SubstitutePair>();
			ArrayList<Integer> grp = new ArrayList<Integer>();
			ArrayList<ArrayList<Integer>> grps = new ArrayList<ArrayList<Integer>>();
			
			pairs = substitutesData.getPairList();
			logger.info("groupSubstitutes() - Grouping the substitute pairs..  Pairs Count=" + pairs.size());
			
			int j = 0;
			int i = 0;
			
			while (pairs.size() > 0)
			{
				grp = new ArrayList<Integer>();
				grp.add(pairs.get(0).a);
				//System.out.print("New Group:");
				//System.out.print(pairs.get(0).a + ",");
				
				i = 0;
				while (i < grp.size()){
				
					j = 0;
					while (j < pairs.size()) {
						if (pairs.get(j).a.equals(grp.get(i))) {
							
							if (!grp.contains(pairs.get(j).b)) {
								grp.add(pairs.get(j).b);							
								//System.out.print(pairs.get(j).b + ",");
							}
							pairs.remove(j);
						}
						else if (pairs.get(j).b.equals(grp.get(i))) {				
							if (!grp.contains(pairs.get(j).a)) {
								grp.add(pairs.get(j).a);
								//System.out.print(pairs.get(j).a + ",");
							}
							pairs.remove(j);
						}
						else
							j++;
					}
					//if (!newMemberFound)			
					i++;
				}
				//System.out.println();
				
				grps.add(grp);
				//logger.info("groupSubstitutes() - Groups Count=" + grp.size());
			}		
			
		    // Store the substitute groups
			boolean result = false;
			
			logger.info("groupSubstitutes() - Storing Groups..  No. of Groups=" + grps.size());
		    result = substitutesData.storeGroups(grps);
		    
		    if (!result) {
		    	logger.error("groupSubstitutes() - Failure in storing substitution group information!");
		    }

		}catch(Exception e){
			logger.error("groupSubstitutes() - Exception=" + e.getMessage());
			e.printStackTrace();
		}
		finally {
			logger.info("groupSubstitutes() - Ended.");			
		}
		
	}
	

	
	// TODO why is this method here???
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
