package com.pristine.dto;

public class AdStartAndEndDateKey {
	
	String PRC_STRT_DTE;
	String PRC_END_DTE;
	
	public String getPRC_STRT_DTE() {
		return PRC_STRT_DTE;
	}

	public void setPRC_STRT_DTE(String pRC_STRT_DTE) {
		PRC_STRT_DTE = pRC_STRT_DTE;
	}

	public String getPRC_END_DTE() {
		return PRC_END_DTE;
	}

	public void setPRC_END_DTE(String pRC_END_DTE) {
		PRC_END_DTE = pRC_END_DTE;
	}
	
	public AdStartAndEndDateKey(String pRC_STRT_DTE, String pRC_END_DTE) {
		this.PRC_STRT_DTE = pRC_STRT_DTE;
		this.PRC_END_DTE = pRC_END_DTE;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((PRC_END_DTE == null) ? 0 : PRC_END_DTE.hashCode());
		result = prime * result + ((PRC_STRT_DTE == null) ? 0 : PRC_STRT_DTE.hashCode());
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
		AdStartAndEndDateKey other = (AdStartAndEndDateKey) obj;
		if (PRC_END_DTE == null) {
			if (other.PRC_END_DTE != null)
				return false;
		} else if (!PRC_END_DTE.equals(other.PRC_END_DTE))
			return false;
		if (PRC_STRT_DTE == null) {
			if (other.PRC_STRT_DTE != null)
				return false;
		} else if (!PRC_STRT_DTE.equals(other.PRC_STRT_DTE))
			return false;
		return true;
	}
	
}
