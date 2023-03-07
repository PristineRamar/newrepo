package com.pristine.test.offermgmt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.service.offermgmt.oos.OOSCandidateItemImpl;
import com.pristine.util.DateUtil;


public class OOSUniqueItemsTest {
	private static Logger logger = Logger.getLogger("OOSUniqueItemsTest");
	public static void main(String[] args) {
		OOSUniqueItemsTest oosUniqueItemsTest = new OOSUniqueItemsTest();
		oosUniqueItemsTest.getUniqueItems();

	}
	
	private void getUniqueItems(){
		OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
		logger.info("Size of item list -- " + getTestItems().size());
		List<PRItemDTO> uniqeItems = oosCandidateItemImpl.clearDuplicates(getTestItems());
		logger.info("# of unique items found -- " + uniqeItems.size());
	}

	private List<PRItemDTO> getTestItems() {

		// Same promotions appears in more than one page or block in the weekly
		// ad
		// 1. If the promotion appears more than once in the same page, then any
		// one of the block is picked
		// (there is no definite logic in picking the block)
		List<PRItemDTO> test = new ArrayList<PRItemDTO>();
		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400491);
		prItemDTO.setPromoDefinitionId(12345);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(1);
		prItemDTO.setBlockNumber(13);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400491);
		prItemDTO.setPromoDefinitionId(12345);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(1);
		prItemDTO.setBlockNumber(18);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);
		// 2. If the promotions appears more than once in different page,
		// then any one of the page is picked (there is no definite logic in
		// picking the page)
		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400492);
		prItemDTO.setPromoDefinitionId(12346);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(2);
		prItemDTO.setBlockNumber(18);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400492);
		prItemDTO.setPromoDefinitionId(12346);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(1);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);
		// 2. If the promotions appears more than once in different page, then
		// any one of the page is picked
		// (there is no definite logic in picking the page)
		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400493);
		prItemDTO.setPromoDefinitionId(12346);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(1);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400493);
		prItemDTO.setPromoDefinitionId(12346);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(2);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(400493);
		prItemDTO.setPromoDefinitionId(12346);
		prItemDTO.setRetLirId(5353);
		prItemDTO.setPageNumber(3);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		// Same item are in different promotions for the same week in our system
		// 1. If same ret_lir_id is used by different promotions, then pick the
		// promotion which is modified recently,
		// if the promotions are modified on same days, then pick any one (there
		// is no definite logic here)
		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(930693);
		prItemDTO.setPromoDefinitionId(12348);
		prItemDTO.setRetLirId(5354);
		prItemDTO.setPageNumber(3);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(null);
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(930693);
		prItemDTO.setPromoDefinitionId(12349);
		prItemDTO.setRetLirId(5354);
		prItemDTO.setPageNumber(1);
		prItemDTO.setBlockNumber(2);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode(null);
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -18));
		prItemDTO.setPromoModifiedDate(DateUtil.incrementDate(new Date(), -6));
		setval(prItemDTO);
		test.add(prItemDTO);

		// 2. If same items appears in more than one different promotions, then
		// pick the promotion which is modified recently,
		// if the promotions are modified on same days, then pick any one (there
		// is no definite logic here)
		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(930694);
		prItemDTO.setPromoDefinitionId(12349);
		prItemDTO.setRetLirId(-1);
		prItemDTO.setPageNumber(2);
		prItemDTO.setBlockNumber(8);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode("012345");
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -12));
		prItemDTO.setPromoModifiedDate(DateUtil.incrementDate(new Date(), -6));
		setval(prItemDTO);
		test.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(930694);
		prItemDTO.setPromoDefinitionId(12350);
		prItemDTO.setRetLirId(-1);
		prItemDTO.setPageNumber(3);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode("012345");
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -14));
		prItemDTO.setPromoModifiedDate(DateUtil.incrementDate(new Date(), -9));
		setval(prItemDTO);
		test.add(prItemDTO);
		
		
		prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(220571);
		prItemDTO.setPromoDefinitionId(12350);
		prItemDTO.setRetLirId(-1);
		prItemDTO.setPageNumber(3);
		prItemDTO.setBlockNumber(20);
		prItemDTO.setPromoTypeId(9);
		prItemDTO.setRetailerItemCode("012345");
		prItemDTO.setPromoCreatedDate(DateUtil.incrementDate(new Date(), -14));
		prItemDTO.setPromoModifiedDate(DateUtil.incrementDate(new Date(), -9));
		setval(prItemDTO);
		test.add(prItemDTO);
		return test;

	}

	private void setval(PRItemDTO prItemDTO) {
		prItemDTO.setWeeklyAdLocationLevelId(1);
		prItemDTO.setWeeklyAdLocationId(50);
		prItemDTO.setRegMPack(1);
		prItemDTO.setRegPrice(3.99);
		prItemDTO.setRegMPrice(0.00);
		prItemDTO.setSaleMPack(2);
		prItemDTO.setSalePrice(3.99);
		prItemDTO.setSaleMPrice(0.00);
		prItemDTO.setDisplayTypeId(1);
		prItemDTO.setAdjustedUnits(3000);

	}
}
