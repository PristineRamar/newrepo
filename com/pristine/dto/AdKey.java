package com.pristine.dto;

import org.apache.commons.lang.builder.CompareToBuilder;
public class AdKey implements Comparable<AdKey>{
	int pageNumber;
	int blockNumber;

	public AdKey(int pageNumber, int blockNumber) {
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
		AdKey other = (AdKey) obj;
		if (blockNumber != other.blockNumber)
			return false;
		if (pageNumber != other.pageNumber)
			return false;
		return true;
	}

	@Override
	public int compareTo(AdKey o) {
		return  new CompareToBuilder()
			     .append(getPageNumber(), o.pageNumber)
			     .append(getBlockNumber(), o.blockNumber)
			     .toComparison();
	}

	@Override
	public String toString() {
		return "AdKey [pageNumber=" + pageNumber + ", blockNumber=" + blockNumber + "]";
	}
	
	
}
