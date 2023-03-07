package com.pristine.dto.offermgmt.promotion;

public class PageBlockNoKey {

	private int pageNumber;
	private int blockNumber;
	
	public PageBlockNoKey(int pageNumber, int blockNumber) {
		super();
		this.pageNumber = pageNumber;
		this.blockNumber = blockNumber;
	}
	
	public int getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	public int getBlockNumber() {
		return blockNumber;
	}
	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + blockNumber;
		result = prime * result + pageNumber;
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageBlockNoKey other = (PageBlockNoKey) obj;
		if (blockNumber != other.blockNumber)
			return false;
		if (pageNumber != other.pageNumber)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PageBlockNoKey [pageNumber=" + pageNumber + ", blockNumber=" + blockNumber + "]";
	}	
}
