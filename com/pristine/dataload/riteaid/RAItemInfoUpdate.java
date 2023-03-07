package com.pristine.dataload.riteaid;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dto.riteaid.RAItemInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public  class RAItemInfoUpdate extends PristineFileParser{
	 static Logger logger = Logger.getLogger("RAItemInfoUpdate");
	
	 int updateCount = 0;
	 int recordCount=0;
	 int ignoredCount = 0;
	 int updateFailedCount = 0;
	 private int stopCount = -1;
	 String rootpath;
	 private HashMap<String,Integer> upcItemCodeMap;
	 ItemDAO itemdao = new ItemDAO ();
	 Connection conn = null; 
	 public RAItemInfoUpdate (){
		 super ();
	 }
	 
	 
	 
	public static void main(String[] args) {
		
		String relativePath = "ItemSetupNew" ;
		if( args.length > 0 )
			relativePath = args[0];
		
		String FilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j.properties");
		

		RAItemInfoUpdate itemUpdateRA= new RAItemInfoUpdate();
		itemUpdateRA.processItemFile(relativePath);
		
		//Close Connection ......
	 }

	

	private void  processItemFile(String relativePath ){
		try{

		headerPresent  = true;
		
		conn = super.getOracleConnection();
		upcItemCodeMap = itemdao.getItemCodeAndUpc(conn);
		
		
		String fieldNames[] = new String[139];
    	fieldNames[0] = "Upc";
    	fieldNames[1] = "uncode";
    	fieldNames[2] = "orangebookcode";
    	fieldNames[3] = "vendor";
    	fieldNames[4] = "corpdesc";
    	fieldNames[5] = "deptdesc";
    	fieldNames[6] = "distrdeptdesc";
    	fieldNames[7] = "itemno";
    	fieldNames[8] = "generic_ind";
    	fieldNames[9] = "labelname";
    	fieldNames[10] = "labelerid";
    	fieldNames[11] = "mfraddr1";
     	fieldNames[12] = "mfrcity";
    	fieldNames[13] = "mfrname"; 
    	fieldNames[14] = "genericdesc";
    	fieldNames[15] = "genericequiv";
    	fieldNames[16] = "genericprod_ind";
    	fieldNames[17] = "genericcode";
    	fieldNames[18] = "genericcodeseq";
    	fieldNames[19] = "genericcodenum";
    	fieldNames[20] = "genericname";
    	fieldNames[21] = "hospital_ind";
    	fieldNames[22] = "return_ind";
    	fieldNames[23] = "replacementndc";
    	fieldNames[24] = "drugspecificcode";
    	fieldNames[25] = "drugspecificdesc";
    	fieldNames[26] = "hostcontroller_ind";
    	fieldNames[27] = "innovator_ind";
    	fieldNames[28] = "itemdesc";
    	fieldNames[29] = "activationdate";
    	fieldNames[30] = "adminroute";
    	fieldNames[31] = "arcos";
    	fieldNames[32] = "brandname";
    	fieldNames[33] = "buyingcat";
    	fieldNames[34] = "catmgr";
    	fieldNames[35] = "cat";
    	fieldNames[36] = "catdesc";
    	fieldNames[37] = "cclass";
    	fieldNames[38] = "corporation";
    	fieldNames[39] = "compositioncode";
    	fieldNames[40] = "chaincommrx_ind";
    	fieldNames[41] = "currentitem_ind";
    	fieldNames[42] = "classdesc";
    	fieldNames[43] = "drugformularycode";
    	fieldNames[44] = "standardcode";
    	fieldNames[45] = "standarddesc";
    	fieldNames[46] = "standardpackage_ind";
    	fieldNames[47] = "itemstatus";
    	fieldNames[48] = "subclass";
    	fieldNames[49] = "mfrareacode";
    	fieldNames[50] = "mfrphone7";
    	fieldNames[51] = "mfrstate";
    	fieldNames[52] = "mfrzip";
    	fieldNames[53] = "itemtype";
    	fieldNames[54] = "subclassdesc";
    	fieldNames[55] = "therapequivcode";
    	fieldNames[56] = "unitdosage_ind";
    	fieldNames[57] = "deaclass";
    	fieldNames[58] = "dosagecode";
    	fieldNames[59] = "dosagecodedesc";
    	fieldNames[60] = "drugcatcode";
    	fieldNames[61] = "dept";
    	fieldNames[62] = "discontdate";
    	fieldNames[63] = "distrdept";
    	fieldNames[64] = "drugclass";
    	fieldNames[65] = "expensetypecode";
    	fieldNames[66] = "obsdate";
    	fieldNames[67] = "originalitemno";
    	fieldNames[68] = "whs_ind";
    	fieldNames[69] = "prevndc";
    	fieldNames[70] = "itemuomqty";
    	fieldNames[71] = "primarydisposalcode";
    	fieldNames[72] = "primarydisposaldesc";
    	fieldNames[73] = "lob";
    	fieldNames[74] = "lobdesc";
    	fieldNames[75] = "fob";
    	fieldNames[76] = "fobdesc";
    	fieldNames[77] = "findept";
    	fieldNames[78] = "findeptdesc";
    	fieldNames[79] = "fincat";
    	fieldNames[80] = "fincatdesc";
    	fieldNames[81] = "catdept";
    	fieldNames[82] = "catdeptdesc";
    	fieldNames[83] = "startdt_prd";
    	fieldNames[84] = "enddt_prd";
    	fieldNames[85] = "createdt_prd";
    	fieldNames[86] = "lastupddt_prd";
    	fieldNames[87] = "lastupdstpid_prd";
    	fieldNames[88] = "lastupdby_prd";
    	fieldNames[89] = "actv_ind_prd";
    	fieldNames[90] = "batchid_prd";
    	fieldNames[91] = "currndcupc";
    	fieldNames[92] = "curruncode";
    	fieldNames[93] = "drugsource";
    	fieldNames[94] = "brandorgeneric";
    	fieldNames[95] = "vpcatmgmtid";
    	fieldNames[96] = "vpcatmgmt";
    	fieldNames[97] = "genericcodenum_ra";
    	fieldNames[98] = "genericprodid_ra";
    	
    	fieldNames[99] = "labelname_ra";
    	fieldNames[100] = "orangebookcode_ra";
    	fieldNames[101] = "replacementndc_ra";
    	fieldNames[102] = "unitdosage_ind_ra";
    	fieldNames[103] = "pl_ind";
    	fieldNames[104] = "catlongdesc";
    	fieldNames[105] = "classlongdesc";
    	fieldNames[106] = "subclasslongdesc";
    	fieldNames[107] = "itemnolongdesc";
    	fieldNames[108] = "mfrvendorlongdesc";
    	fieldNames[109] = "catlongdesc2";
    	fieldNames[110] = "vplongdesc";
    	fieldNames[111] = "drugmfrname";
    	fieldNames[112] = "drugmfraddr1";
    	fieldNames[113] = "drugmfrcity";
    	fieldNames[114] = "drugmfrstate";
    	fieldNames[115] = "drugmfrzip";
    	fieldNames[116] = "drugmfrphone";
    	fieldNames[117] = "catdeptlongdesc";
    	fieldNames[118] = "deptlongdesc";
    	fieldNames[119] = "mmp_ind";
    	fieldNames[120] = "cpn_ind";
    	fieldNames[121] = "fsa_ind";
    	fieldNames[122] = "plx_ind";
    	fieldNames[123] = "isfe";
    	fieldNames[124] = "isfe_fin";
    	fieldNames[125] = "brand";
    	fieldNames[126] = "subbrand";
    	fieldNames[127] = "upcmasked";
    	fieldNames[128] = "delete_ind";
    	fieldNames[129] = "activeitemindex";
    	fieldNames[130] = "activeitemno";
    	fieldNames[131] = "sub_dept";
    	fieldNames[132] = "sub_dept_desc";
    	fieldNames[133] = "iswp";
    	fieldNames[134] = "catalyst_filter_ind";
    	fieldNames[135] = "pkgsize";
    	fieldNames[136] = "standardpackagesize";
    	fieldNames[137] = "retailpackqty";
    	fieldNames[138] = "itemuom";
    	
    
    		
		
		ArrayList<String> fileList = getFiles(relativePath);
    	for (int j = 0; j < fileList.size(); j++){
    		logger.info("processing - " + fileList.get(j));
    		parseDelimitedFile(RAItemInfoDTO.class, fileList.get(j), '|',fieldNames, stopCount);
    	}
    	
    	   logger.info( "# of Records Updated = " + updateCount);
    	   logger.info( "# of Records Ignored = " + ignoredCount);
    	   
		} catch(GeneralException ge ){
			logger.error("Error Parsing Files" +ge.getMessage() );
			logger.error("Exception Stack trace", ge);
			
		}
		
		catch(Exception e ){
			logger.error("Error Parsing Files" +e.getMessage() );
			logger.error("Exception Stack trace", e);
		} finally{
			PristineDBUtil.close(conn);
		}
		
	}
	
	public void processRecords(List listObject) throws GeneralException {
		ArrayList <RAItemInfoDTO> updateItemList = new ArrayList <RAItemInfoDTO> (); 
		for ( int jj = 0; jj <listObject.size();jj++)
		{
			RAItemInfoDTO raupdt = (RAItemInfoDTO) listObject.get(jj);
			recordCount++;
			raupdt.setUpc( PrestoUtil.castUPC(raupdt.getUpc(), false));
			
			if( upcItemCodeMap.containsKey(raupdt.getUpc())){
				raupdt.setPrestoItemcode(upcItemCodeMap.get(raupdt.getUpc()));
				updateItemList.add(raupdt);
			} else {
				ignoredCount++;
			}
			
			/*
			logger.debug("UPC " + raupdt.getUpc());
			logger.debug("NDC? " + raupdt.getUncode());
			logger.debug("Item # " + raupdt.getItemno());
			logger.debug("Label " + raupdt.getLabelname());
			logger.debug("Standard Desc " + raupdt.getStandarddesc());
			logger.debug("Org item# " + raupdt.getOriginalitemno());
			logger.debug("Active Item # " + raupdt.getActiveitemno());
			*/
			
		}
		
		int recordsUpdated = itemdao.updateSecondaryItemInfo(conn, updateItemList);
		logger.info("# of Records Processed " + recordCount);
		updateCount += recordsUpdated;
		PristineDBUtil.commitTransaction(conn, "batch record update");
	
	}

				
}
		
	


// Alter Script
// alter table ITEM_LOOKUP ADD ALT_RETAILER_ITEM_CODE VARCHAR2(20);