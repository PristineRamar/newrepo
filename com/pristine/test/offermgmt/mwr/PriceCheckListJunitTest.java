package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.CheckListService;

public class PriceCheckListJunitTest {

	CheckListService criteriaService = new CheckListService();

	@Test
	// Test case to get the latest priceCheckList Id for item belonging to multiple
	// lists
	public void Case1() throws GeneralException {
		List<PriceCheckListDTO> itemList = new ArrayList<>();

		PriceCheckListDTO PriceCheckListDto1 = new PriceCheckListDTO();
		PriceCheckListDto1.setCreateDate("08/23/2021");
		PriceCheckListDto1.setUpdateDate("08/23/2021");
		PriceCheckListDto1.setPriceCheckListTypeId(4);
		PriceCheckListDto1.setPriceCheckListId(100);
		itemList.add(PriceCheckListDto1);

		PriceCheckListDTO PriceCheckListDto2 = new PriceCheckListDTO();
		PriceCheckListDto2.setCreateDate("08/23/2021");
		PriceCheckListDto2.setUpdateDate("08/24/2021");
		PriceCheckListDto2.setPriceCheckListTypeId(4);
		PriceCheckListDto2.setPriceCheckListId(125);
		itemList.add(PriceCheckListDto2);

		/*
		 * int priceCheckListId = criteriaService.getLatestItemList(itemList);
		 * 
		 * int expectedId = 125;
		 * assertEquals("PriceCheckListId not matching  not matching!!!", expectedId,
		 * priceCheckListId);
		 */
	}
	
	@Test
	// Test case to get the latest priceCheckList Id for item belonging to multiple
	// lists
	public void Case2() throws GeneralException {
		List<PriceCheckListDTO> itemList = new ArrayList<>();

		PriceCheckListDTO PriceCheckListDto1 = new PriceCheckListDTO();
		PriceCheckListDto1.setCreateDate("08/23/2021");
		PriceCheckListDto1.setUpdateDate("08/23/2021");
		PriceCheckListDto1.setPriceCheckListTypeId(4);
		PriceCheckListDto1.setPriceCheckListId(100);
		itemList.add(PriceCheckListDto1);

		PriceCheckListDTO PriceCheckListDto2 = new PriceCheckListDTO();
		PriceCheckListDto2.setCreateDate("05/27/2021");
		PriceCheckListDto2.setUpdateDate("05/27/2021");
		PriceCheckListDto2.setPriceCheckListTypeId(4);
		PriceCheckListDto2.setPriceCheckListId(125);
		itemList.add(PriceCheckListDto2);

		/*
		 * int priceCheckListId = criteriaService.getLatestItemList(itemList);
		 * 
		 * int expectedId = 100;
		 * assertEquals("PriceCheckListId not matching  not matching!!!", expectedId,
		 * priceCheckListId);
		 */
	}
	
	
	
	@Test
	// Test case to get the latest priceCheckList Id for item belonging to multiple
	// lists
	public void Case3() throws GeneralException {
		List<PriceCheckListDTO> itemList = new ArrayList<>();

		PriceCheckListDTO PriceCheckListDto1 = new PriceCheckListDTO();
		PriceCheckListDto1.setCreateDate("08/23/2021");
		PriceCheckListDto1.setUpdateDate("08/23/2021");
		PriceCheckListDto1.setPriceCheckListTypeId(4);
		PriceCheckListDto1.setPriceCheckListId(100);
		itemList.add(PriceCheckListDto1);

		PriceCheckListDTO PriceCheckListDto2 = new PriceCheckListDTO();
		PriceCheckListDto2.setCreateDate("08/23/2021");
		PriceCheckListDto2.setUpdateDate("08/23/2021");
		PriceCheckListDto2.setPriceCheckListTypeId(4);
		PriceCheckListDto2.setPriceCheckListId(125);
		itemList.add(PriceCheckListDto2);

		/*
		 * int priceCheckListId = criteriaService.getLatestItemList(itemList);
		 * 
		 * int expectedId = 100;
		 * assertEquals("PriceCheckListId not matching  not matching!!!", expectedId,
		 * priceCheckListId);
		 */

	}
}
