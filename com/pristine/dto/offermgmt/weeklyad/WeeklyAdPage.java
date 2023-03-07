package com.pristine.dto.offermgmt.weeklyad;

import java.util.TreeMap;

public class WeeklyAdPage{
	private long pageId;
	private long adId;
	private int pageNumber;
	private int totalBlocks;
	private int status;
	
	private TreeMap<Integer, WeeklyAdBlock> adBlocks = new TreeMap<Integer, WeeklyAdBlock>();

	public long getPageId() {
		return pageId;
	}

	public void setPageId(long pageId) {
		this.pageId = pageId;
	}

	public long getAdId() {
		return adId;
	}

	public void setAdId(long adId) {
		this.adId = adId;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public int getTotalBlocks() {
		return totalBlocks;
	}

	public void setTotalBlocks(int totalBlocks) {
		this.totalBlocks = totalBlocks;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public TreeMap<Integer, WeeklyAdBlock> getAdBlocks() {
		return adBlocks;
	}

	public void setAdBlocks(TreeMap<Integer, WeeklyAdBlock> adBlocks) {
		this.adBlocks = adBlocks;
	}

	public void addBlock(WeeklyAdBlock adBlock){
		this.adBlocks.put(adBlock.getBlockNumber(), adBlock);
	}
}
