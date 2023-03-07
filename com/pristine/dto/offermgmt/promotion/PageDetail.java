package com.pristine.dto.offermgmt.promotion;

import java.util.HashMap;

public class PageDetail {
	
	private int pageNo = 0;
	private HashMap<Integer, BlockDetail> blockMap = new HashMap<Integer, BlockDetail>();

	public HashMap<Integer, BlockDetail> getBlockMap() {
		return blockMap;
	}

	public void setBlockMap(HashMap<Integer, BlockDetail> blockMap) {
		this.blockMap = blockMap;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	@Override
	public String toString() {
		return "PageDetail [pageNo=" + pageNo + ", blockMap=" + blockMap + "]";
	}
	
	
	
}
