package com.pristine.dataload.tops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.LigDTO;
import com.pristine.util.Constants;


/**
 * This program uses the following steps
 * 
 * Input is the Category ID and LIG file (i.e. path containing the LIG file).
 * 
 * Step1 - Read the records in the file and ignore records where the size family and brand family are blank
 * This steps member items in LIG as  because it meets the above condition and reduces the # of records to process
 * 
 * Step 2- Identify items belonging to the category and ignore other items
 * 
 * Step 3 - Create Group based on Size relationship. If Size relationship is , assign it to its own PG
 * 
 * Step 4 - Assign Size Lead based on whether item belongs to KVI or K2 or smaller size
 * 
 * Step 5 - Create Price Group based on Brand Family
 * 
 * Step 6 - Assign PG Lead Item
 * 
 * Step 7 - Assign PG Name
 * 
 * Step 8 - Build Tier Relationship
 * 
 * Step 9 - PRint to excel
 */
public class PriceGroupFormatterV2 extends PriceGroupFormatter{
	private Logger logger = Logger.getLogger("PriceGroupFormatter");
	
	public PriceGroupFormatterV2(){
		super();
	}
	

	//assign Size lead

	public void identifySizeLead() {
		
		int sizeLeadCount = 0;
		
	
		HashMap<String, List<LigDTO>>initialPG  = getInitialPriceGroup();
		for( List<LigDTO> candidatePGList: initialPG.values()) {
			
			
			String sizeLeadItemCode = null;
			float sizeLeadItemSize = -1;
			boolean isKVI = false;
			boolean isK2 = false;
			int itemRank = 99;
			
			for( LigDTO valueDTO : candidatePGList){
				
				String kviCode = valueDTO.getKviCode().trim();
				boolean currentItemAsSizeLead = false;
				boolean doSizeCheck = false;
				//logger.debug("Size Family : " + valueDTO.getSizeFamily());
				if(valueDTO.getSizeFamily().isEmpty()) continue;
				if( sizeLeadItemCode == null){
					currentItemAsSizeLead = true;
				}else if(kviCode.equals(Constants.KVI)) {
					if( !isKVI) currentItemAsSizeLead = true;
					else doSizeCheck = true;
				
				}else if( !isKVI && kviCode.equals(Constants.K2)){
					if( !isK2) currentItemAsSizeLead = true;
					else doSizeCheck = true;
				} else {
					//size match
					if( !kviCode.isEmpty()){
						int rank = Integer.parseInt(kviCode);
						if( rank <  itemRank){
							currentItemAsSizeLead = true;
						}
						else if( rank ==  itemRank)
							doSizeCheck = true;						
					} else {
						doSizeCheck = true;
					}
						
					
				}
				
				if( doSizeCheck ){
					float currentItemSize = -1;
					if(!valueDTO.getItemSize().isEmpty())
						currentItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					if( sizeLeadItemSize > 0 &&  currentItemSize > 0 &&  currentItemSize < sizeLeadItemSize)
						currentItemAsSizeLead = true;
				}
				
				if( currentItemAsSizeLead){
					sizeLeadItemCode = valueDTO.getRetailerItemCodeNoVer();
					isKVI = valueDTO.getKviCode().trim().equals(Constants.KVI);
					isK2 = valueDTO.getKviCode().trim().equals(Constants.K2);
					
					if(!kviCode.equals(Constants.KVI) && !kviCode.equals(Constants.K2) && !kviCode.isEmpty()){
						int rank = Integer.parseInt(kviCode);
						if( rank <  itemRank){
							itemRank = rank;
						}
					}
					
					if(! valueDTO.getItemSize().isEmpty())
						sizeLeadItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					else 
						sizeLeadItemSize = -1;
						
				}
			}
			
			if( sizeLeadItemCode != null ){
				for( LigDTO valueDTO : candidatePGList){
					if( valueDTO.getRetailerItemCodeNoVer().equals(sizeLeadItemCode)) {
						valueDTO.setSizeLead("Y");
						sizeLeadCount++;
						break;
					}
				}
			}
				
		}
		logger.info( "No of Size Leads Identified : " + sizeLeadCount);
		
	}
	
	
	
	
	
	
	
	public void AssignPGLead() {
		
		int pgLeadCount = 0;
		HashMap<String, List<LigDTO>>finalPG  = getFinalPriceGroup();
		for( String pgGroupName: finalPG.keySet()) {
		
			List<LigDTO> finalPGList = finalPG.get(pgGroupName);
			//1 - if PG name is same as Size lead, then, use it to assign PG Lead
			// continue the loop
			
			LigDTO firstDTO = finalPGList.get(0);
				
			if(!firstDTO.getSizeFamily().isEmpty() && pgGroupName.equals(firstDTO.getSizeFamily())){
				setPriceGroupLead(finalPGList,firstDTO.getSizeFamily());
				pgLeadCount++;
				continue;
 			}

			boolean isNationalPresent = false;
			boolean isCorpBrandPresent = false;
			//- 2 Indicate presence of any NB or CORP_BRAND items
			for(  LigDTO valueDTO : finalPGList){
				if( valueDTO.getTier() == 1){
					isCorpBrandPresent = true;
				}
				else if( valueDTO.getTier() == 0){
					isNationalPresent = true;
				}
			}

			//3 - Assign the tier for PG
			int tierForPGLead = -1;
			if( isNationalPresent ) tierForPGLead = 0;
			else if (isCorpBrandPresent) tierForPGLead = 1;
			
			// Are there KVI or K2 or not within the tier
		
			String pgLeadItemCode = null;
			float pgLeadItemSize = -1;
			boolean isKVI = false;
			boolean isK2 = false;
			int itemRank  = 99;
				
			for( LigDTO valueDTO : finalPGList){
				
				if( !valueDTO.isPrintRow()) continue;
				boolean currentItemAsPGLead = false;
				boolean doSizeCheck = false;
				if(valueDTO.getTier() !=  tierForPGLead) continue;
				
				String kviCode = valueDTO.getKviCode().trim();
				
				if( pgLeadItemCode == null){
					currentItemAsPGLead = true;
				}else if(kviCode.equals(Constants.KVI)) {
					if( !isKVI) currentItemAsPGLead = true;
					else doSizeCheck = true;
				
				}else if( !isKVI && kviCode.equals(Constants.K2)){
					if( !isK2) currentItemAsPGLead = true;
					else doSizeCheck = true;
				} else {
					//size match
					if( !kviCode.isEmpty()){
						int rank = Integer.parseInt(kviCode);
						if( rank <  itemRank){
							currentItemAsPGLead = true;
						}
						else if( rank ==  itemRank)
							doSizeCheck = true;						
					} else {
						doSizeCheck = true;
					}
					
				}
				
				if( doSizeCheck ){
					float currentItemSize = -1;
					if(!valueDTO.getItemSize().isEmpty())
						currentItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					if( pgLeadItemSize > 0 &&  currentItemSize > 0 &&  currentItemSize < pgLeadItemSize)
						currentItemAsPGLead = true;
				}
				
				if( currentItemAsPGLead){
					pgLeadItemCode = valueDTO.getRetailerItemCodeNoVer();
					isKVI = valueDTO.getKviCode().trim().equals(Constants.KVI);
					isK2 = valueDTO.getKviCode().trim().equals(Constants.K2);
					
					if(!kviCode.equals(Constants.KVI) && !kviCode.equals(Constants.K2) && !kviCode.isEmpty()){
						int rank = Integer.parseInt(kviCode);
						if( rank <  itemRank){
							itemRank = rank;
						}
					}
						
					if(! valueDTO.getItemSize().isEmpty())
						pgLeadItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					else 
						pgLeadItemSize = -1;
				}
				
			}
			
			//For that tier, pick the KVI or K2 or lowest size in the family and assign it as Lead
			//If size Family, ensure PG lead is same as Size lead
			
			if( pgLeadItemCode != null ){
				for( LigDTO valueDTO : finalPGList){
					if( valueDTO.getRetailerItemCodeNoVer().equals(pgLeadItemCode) && valueDTO.isPrintRow() ) {
						if( valueDTO.getSizeFamily().isEmpty())
							valueDTO.setPriceGroupLead("Y");
						else
							setPriceGroupLead(finalPGList,valueDTO.getSizeFamily());
						pgLeadCount++;
						break;
					}
				}
			}

		}
		
		logger.info("No of Price Groups Leads Identified - " + pgLeadCount);
	    
	}


	// If there is only one size and Brand Relationship in the final PG, remove the brand relation
	//and add it to size relation
	public void mergeSizeBrandRows() {
		
		
		HashMap<String, Integer> sizeCountMap = new HashMap<String, Integer>(); 
		HashMap<String, Integer> brandCountMap = new HashMap<String, Integer>(); 
		
		HashMap<String, List<LigDTO>>finalPG  = getFinalPriceGroup();
		Set<String> keyList = finalPG.keySet();
		
		HashMap<String, LigDTO> brandRelation  = new HashMap<String, LigDTO>(); 
		
		
		for( String key : keyList){
			List<LigDTO> al = finalPG.get(key);
			for( LigDTO ligRec : al){
				String itemKey = "";
				
				if( ligRec.getLirId() > 0 )
					itemKey = ligRec.getLineGroupIdentifier(); 
				else
					itemKey = ligRec.getRetailerItemCodeNoVer();
				
				
				if(!ligRec.getBrandFamily().isEmpty()) {
					if( brandCountMap.containsKey(itemKey)){
						int count = brandCountMap.get(itemKey).intValue();
						count++;
						brandCountMap.put(itemKey, Integer.valueOf(count));
						
					}
					else 
						brandCountMap.put(itemKey, Integer.valueOf(1));
					
					brandRelation.put(itemKey, ligRec);
				}
				else if ( !ligRec.getSizeFamily().isEmpty()){
					if( sizeCountMap.containsKey(itemKey)){
						int count = sizeCountMap.get(itemKey).intValue();
						count++;
						sizeCountMap.put(itemKey, Integer.valueOf(count));
						
					}
					else 
						sizeCountMap.put(itemKey, Integer.valueOf(1));
				}
					
			}
		}
		
		
		HashSet<String> ignoreBrandFamilySet = new HashSet<String> ();
		keyList = finalPG.keySet();
		for( String key : keyList){
			List<LigDTO> al = finalPG.get(key);
			for( LigDTO ligRec : al){
				String itemKey = "";
				
				if( ligRec.getLirId() > 0 )
					itemKey = ligRec.getLineGroupIdentifier(); 
				else
					itemKey = ligRec.getRetailerItemCodeNoVer();
				
				if ( !ligRec.getSizeFamily().isEmpty()){
					
					if( brandRelation.containsKey(itemKey)){
						LigDTO brandRec = brandRelation.get(itemKey);
						
						ligRec.setBrandClass(brandRec.getBrandClass());
						ligRec.setBrandFamily(brandRec.getBrandFamily());
						ligRec.setDependentItemName(brandRec.getDependentItemName());
						ligRec.setDependentLIG(brandRec.getDependentLIG());
						ligRec.setDependentRetailerItemCode(brandRec.getDependentRetailerItemCode());
						ligRec.setTierOverride(brandRec.getTierOverride());
						ligRec.setTier(brandRec.getTier());
						
						brandRec.setPrintRow(false);
						brandRelation.remove(itemKey);
						ignoreBrandFamilySet.add(itemKey);
					}
				} else {
					if( sizeCountMap.containsKey(itemKey))
						ligRec.setPrintRow(false);
					else if( ignoreBrandFamilySet.contains(itemKey))
						ligRec.setPrintRow(false);
					ignoreBrandFamilySet.add(itemKey);
					
				}
				
				/*
				if(!ligRec.getBrandFamily().isEmpty()) {
					if( brandCountMap.containsKey(itemKey)){
						int count = brandCountMap.get(itemKey).intValue();
						if( count > 1)
							ligRec.setItemLevelBrandRelationship(true);
						else
							ligRec.setItemLevelBrandRelationship(false);
					}
				}
				*/
				
				if( ligRec.getLirId() > 0 ){
					ligRec.setItemLevelSizeRelationship(false);
					ligRec.setItemLevelBrandRelationship(false);
				}
				
				if(!ligRec.getSizeFamily().isEmpty()) {
					if( sizeCountMap.containsKey(itemKey)){
						int count = sizeCountMap.get(itemKey).intValue();
						if( count > 1)
							ligRec.setItemLevelSizeRelationship(true);
						else
							ligRec.setItemLevelSizeRelationship(false);
					}
				}
				ligRec.setItemLevelRelationship(ligRec.isItemLevelBrandRelationship() || ligRec.isItemLevelSizeRelationship());
				
				
			}
		}
		
		//
		
		
	}

	

	// When Items are both in Size and Brand relationship and if the size relationship is in Item level and Brand at LIR level, 
	// the price group does not have the LIR row. This method will fix the issue
	
	//Setup HashMap <<Lir Name, Brand Family Name>> if the item is in Brand relationship
	//check if there are rows representing the LIR not at item level
	//If No, add a row the PG. Use Set Dependentship Logic to clone one of thr rows and clear size lead, size family 
	public void addLIGRows() {
		
		HashMap<String, List<LigDTO>>finalPG  = getFinalPriceGroup();
		
		for( String pgGroupName: finalPG.keySet()) {
		
			List<LigDTO> finalPGList = finalPG.get(pgGroupName);

			HashMap<String, String> lirBrandFamilyMap = new HashMap<String, String> ();

			for(  LigDTO valueDTO : finalPGList){
				if( !valueDTO.getBrandFamily().isEmpty() && valueDTO.getDependentLIG() != null && !valueDTO.getDependentLIG().trim().isEmpty() 
						&&  valueDTO.isPrintRow()) 
					lirBrandFamilyMap.put( valueDTO.getDependentLIG(), valueDTO.getBrandFamily());
			}
			
			if( lirBrandFamilyMap.size() == 0) continue; // nothing to add
			
			List <LigDTO> lirWithNoRows = new ArrayList <LigDTO> (); 
			
			for ( String lirName : lirBrandFamilyMap.keySet()) {
				for(  LigDTO valueDTO : finalPGList){
					
					if( valueDTO.getLirId()> 0 && valueDTO.getLineGroupIdentifier().equalsIgnoreCase(lirName) && valueDTO.isPrintRow()){
						
						if( ! valueDTO.getBrandFamily().isEmpty() && valueDTO.getBrandFamily().equals(lirBrandFamilyMap.get(lirName)) &&
								valueDTO.isItemLevelRelationship()) {
								lirWithNoRows.add(valueDTO);
								break;
						}
					}
				}
				
			}
			
			for(LigDTO baseDTO : lirWithNoRows){
				 LigDTO dependentDTO = cloneDTO(baseDTO);
				
				 dependentDTO.setSizeClass("");
				 dependentDTO.setSizeFamily("");
				 dependentDTO.setTier(0);
				 dependentDTO.setTierOverride("");
				 dependentDTO.setItemLevelRelationship(false);
				 dependentDTO.setPriceGroupLead("");
				 dependentDTO.setPrintRow(true);
				 dependentDTO.setSizeLead("");
				 dependentDTO.setBrandFamily(lirBrandFamilyMap.get(baseDTO.getLineGroupIdentifier()));
				 
				 finalPGList.add(dependentDTO);
			}
			
			//logger.info("# of rows added - " + lirWithNoRows.size());
		}

			
	}
	public LigDTO cloneDTO(LigDTO baseDTO) {
		 LigDTO dependentDTO  = new LigDTO(); 
		 dependentDTO.setBrandClass(baseDTO.getBrandClass());
		 dependentDTO.setBrandFamily(baseDTO.getBrandFamily());
		 dependentDTO.setBrandName(baseDTO.getBrandName());
		 dependentDTO.setInternalItemNo(baseDTO.getInternalItemNo());
		 dependentDTO.setItemCode(baseDTO.getItemCode());
		 dependentDTO.setItemName(baseDTO.getItemName());
		 dependentDTO.setItemSize(baseDTO.getItemSize());
		 dependentDTO.setKviCode(baseDTO.getKviCode());
		 dependentDTO.setLineGroupIdentifier(baseDTO.getLineGroupIdentifier());
		 dependentDTO.setRetailerItemCode(baseDTO.getRetailerItemCode());
		 dependentDTO.setRetailerItemCodeNoVer(baseDTO.getRetailerItemCodeNoVer());
		 dependentDTO.setSizeClass(baseDTO.getSizeClass());
		 dependentDTO.setSizeFamily(baseDTO.getSizeFamily());
		 dependentDTO.setUomName(baseDTO.getUomName());
		 dependentDTO.setUpc(baseDTO.getUpc());
		 dependentDTO.setTier(baseDTO.getTier());
		 dependentDTO.setTierOverride(baseDTO.getTierOverride());
		 dependentDTO.setItemLevelRelationship(baseDTO.isItemLevelRelationship());
		 
		 return (dependentDTO);
		 
	}
	
		
}

