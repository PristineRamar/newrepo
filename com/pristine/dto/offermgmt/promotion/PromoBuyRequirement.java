package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;
import java.util.List;

public class PromoBuyRequirement implements Cloneable {
	private long promoBuyReqId;
	private long promoDefnId;
	private String buyAndGetIsSame;
	private Integer buyX;
	private Integer mustBuyQty;
	private Double mustBuyAmt;
	private Integer minQtyReqd;
	private Double minWeightReqd;
	private Double minAmtReqd;
	private Double manfCoupon;
	private Double storeCoupon;
	
	private List<PromoOfferDetail> offerDetail = new ArrayList<PromoOfferDetail>(); 
	
	public long getPromoBuyReqId() {
		return promoBuyReqId;
	}
	public void setPromoBuyReqId(long promoBuyReqId) {
		this.promoBuyReqId = promoBuyReqId;
	}
	public long getPromoDefnId() {
		return promoDefnId;
	}
	public void setPromoDefnId(long promoDefnId) {
		this.promoDefnId = promoDefnId;
	}
	public String getBuyAndGetIsSame() {
		return buyAndGetIsSame;
	}
	public void setBuyAndGetIsSame(String buyAndGetIsSame) {
		this.buyAndGetIsSame = buyAndGetIsSame;
	}
	public Integer getBuyX() {
		return buyX;
	}
	public void setBuyX(Integer buyX) {
		this.buyX = buyX;
	}
	public Integer getMustBuyQty() {
		return mustBuyQty;
	}
	public void setMustBuyQty(Integer mustBuyQty) {
		this.mustBuyQty = mustBuyQty;
	}
	public Double getMustBuyAmt() {
		return mustBuyAmt;
	}
	public void setMustBuyAmt(Double mustBuyAmt) {
		this.mustBuyAmt = mustBuyAmt;
	}
	public Integer getMinQtyReqd() {
		return minQtyReqd;
	}
	public void setMinQtyReqd(Integer minQtyReqd) {
		this.minQtyReqd = minQtyReqd;
	}
	public Double getMinWeightReqd() {
		return minWeightReqd;
	}
	public void setMinWeightReqd(Double minWeightReqd) {
		this.minWeightReqd = minWeightReqd;
	}
	public Double getMinAmtReqd() {
		return minAmtReqd;
	}
	public void setMinAmtReqd(Double minAmtReqd) {
		this.minAmtReqd = minAmtReqd;
	}
	public Double getManfCoupon() {
		return manfCoupon;
	}
	public void setManfCoupon(Double manfCoupon) {
		this.manfCoupon = manfCoupon;
	}
	public Double getStoreCoupon() {
		return storeCoupon;
	}
	public void setStoreCoupon(Double storeCoupon) {
		this.storeCoupon = storeCoupon;
	}
	public List<PromoOfferDetail> getOfferDetail() {
		return offerDetail;
	}
	public void setOfferDetail(List<PromoOfferDetail> offerDetail) {
		this.offerDetail = offerDetail;
	}
	public void addOfferDetail(PromoOfferDetail offerDetail) {
		this.offerDetail.add(offerDetail);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((buyAndGetIsSame == null) ? 0 : buyAndGetIsSame.hashCode());
		result = prime * result + ((buyX == null) ? 0 : buyX.hashCode());
		result = prime * result + ((manfCoupon == null) ? 0 : manfCoupon.hashCode());
		result = prime * result + ((minAmtReqd == null) ? 0 : minAmtReqd.hashCode());
		result = prime * result + ((minQtyReqd == null) ? 0 : minQtyReqd.hashCode());
		result = prime * result + ((minWeightReqd == null) ? 0 : minWeightReqd.hashCode());
		result = prime * result + ((mustBuyAmt == null) ? 0 : mustBuyAmt.hashCode());
		result = prime * result + ((mustBuyQty == null) ? 0 : mustBuyQty.hashCode());
		result = prime * result + ((offerDetail == null) ? 0 : offerDetail.hashCode());
		result = prime * result + (int) (promoBuyReqId ^ (promoBuyReqId >>> 32));
		result = prime * result + (int) (promoDefnId ^ (promoDefnId >>> 32));
		result = prime * result + ((storeCoupon == null) ? 0 : storeCoupon.hashCode());
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
		PromoBuyRequirement other = (PromoBuyRequirement) obj;
		if (buyAndGetIsSame == null) {
			if (other.buyAndGetIsSame != null)
				return false;
		} else if (!buyAndGetIsSame.equals(other.buyAndGetIsSame))
			return false;
		if (buyX == null) {
			if (other.buyX != null)
				return false;
		} else if (!buyX.equals(other.buyX))
			return false;
		if (manfCoupon == null) {
			if (other.manfCoupon != null)
				return false;
		} else if (!manfCoupon.equals(other.manfCoupon))
			return false;
		if (minAmtReqd == null) {
			if (other.minAmtReqd != null)
				return false;
		} else if (!minAmtReqd.equals(other.minAmtReqd))
			return false;
		if (minQtyReqd == null) {
			if (other.minQtyReqd != null)
				return false;
		} else if (!minQtyReqd.equals(other.minQtyReqd))
			return false;
		if (minWeightReqd == null) {
			if (other.minWeightReqd != null)
				return false;
		} else if (!minWeightReqd.equals(other.minWeightReqd))
			return false;
		if (mustBuyAmt == null) {
			if (other.mustBuyAmt != null)
				return false;
		} else if (!mustBuyAmt.equals(other.mustBuyAmt))
			return false;
		if (mustBuyQty == null) {
			if (other.mustBuyQty != null)
				return false;
		} else if (!mustBuyQty.equals(other.mustBuyQty))
			return false;
		if (offerDetail == null) {
			if (other.offerDetail != null)
				return false;
		} else if (!offerDetail.equals(other.offerDetail))
			return false;
		if (promoBuyReqId != other.promoBuyReqId)
			return false;
		if (promoDefnId != other.promoDefnId)
			return false;
		if (storeCoupon == null) {
			if (other.storeCoupon != null)
				return false;
		} else if (!storeCoupon.equals(other.storeCoupon))
			return false;
		return true;
	}
}
