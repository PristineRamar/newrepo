package com.pristine.dto.offermgmt.promotion;

public class PredictionMetrics {
	private double predRev;
	private double predMov;
	private double predMar;
	
	public double getPredRev() {
		return predRev;
	}
	public void setPredRev(double predRev) {
		this.predRev = predRev;
	}
	public double getPredMov() {
		return predMov;
	}
	public void setPredMov(double predMov) {
		this.predMov = predMov;
	}
	public double getPredMar() {
		return predMar;
	}
	public void setPredMar(double predMar) {
		this.predMar = predMar;
	}
	
	@Override
	public String toString() {
		return "PredictionMetrics [predRev=" + predRev + ", predMov=" + predMov + ", predMar=" + predMar + "]";
	}
	
	
}
