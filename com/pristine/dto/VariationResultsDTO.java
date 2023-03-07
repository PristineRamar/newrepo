package com.pristine.dto;

import com.pristine.util.Constants;

public class VariationResultsDTO implements IValueObject {
	private int 	resultID;
	private int 	chainID;
	private int 	storeID;
	private int 	deptID;
	private int 	comparedToChainID;
	private int 	comparedToStoreID;
	private int 	comparedToDeptID;
	private String 	weekStartDate;
	private String 	weekEndDate;
	private String 	analysisDate;
	private float 	aiv = 0.0F;
	private float 	sale= 0.0F;;
	public float getAivXPct() {
		return aivXPct;
	}
	public void setAivXPct(float aivXPct) {
		this.aivXPct = aivXPct;
	}
	private float 	aivXPct;
	
	public float getRegUp() {
		return regUp;
	}
	public void setRegUp(float regUp) {
		this.regUp = regUp;
	}
	public float getRegDown() {
		return regDown;
	}
	public void setRegDown(float regDown) {
		this.regDown = regDown;
	}
	private float 	netReg= 0.0F;;
	private float 	depth = 0.0F;;
	private float 	regUp = 0.0F;;
	private float 	regDown = 0.0F;;
	private char  	aivNoteworthyInd = Constants.NO;
	private char  	depthNoteworthyInd= Constants.NO;
	private char  	saleNoteworthyInd= Constants.NO;
	private char  	regNoteworthyInd= Constants.NO;
	private char  	regUpNoteworthyInd= Constants.NO;
	private char  	regDownNoteworthyInd= Constants.NO;
	public char getRegUpNoteworthyInd() {
		return regUpNoteworthyInd;
	}
	public void setRegUpNoteworthyInd(char regUpNoteworthyInd) {
		this.regUpNoteworthyInd = regUpNoteworthyInd;
	}
	public char getRegDownNoteworthyInd() {
		return regDownNoteworthyInd;
	}
	public void setRegDownNoteworthyInd(char regDownNoteworthyInd) {
		this.regDownNoteworthyInd = regDownNoteworthyInd;
	}
	private int 	baselineID;
	private int 	numOfStores;
	private int 	numOfItems;
	
	public float getAiv() {
		return aiv;
	}
	public void setAiv(float aiv) {
		this.aiv = aiv;
	}
	public char getAivNoteworthyInd() {
		return aivNoteworthyInd;
	}
	public void setAivNoteworthyInd(char aivNoteworthyInd) {
		this.aivNoteworthyInd = aivNoteworthyInd;
	}
	public String getAnalysisDate() {
		return analysisDate;
	}
	public void setAnalysisDate(String analysisDate) {
		this.analysisDate = analysisDate;
	}
	public int getBaselineID() {
		return baselineID;
	}
	public void setBaselineID(int baselineID) {
		this.baselineID = baselineID;
	}
	public int getChainID() {
		return chainID;
	}
	public void setChainID(int chainID) {
		this.chainID = chainID;
	}
	public int getComparedToChainID() {
		return comparedToChainID;
	}
	public void setComparedToChainID(int comparedToChainID) {
		this.comparedToChainID = comparedToChainID;
	}
	public int getComparedToDeptID() {
		return comparedToDeptID;
	}
	public void setComparedToDeptID(int comparedToDeptID) {
		this.comparedToDeptID = comparedToDeptID;
	}
	public int getComparedToStoreID() {
		return comparedToStoreID;
	}
	public void setComparedToStoreID(int comparedToStoreID) {
		this.comparedToStoreID = comparedToStoreID;
	}
	public float getDepth() {
		return depth;
	}
	public void setDepth(float depth) {
		this.depth = depth;
	}
	public char getDepthNoteworthyInd() {
		return depthNoteworthyInd;
	}
	public void setDepthNoteworthyInd(char depthNoteworthyInd) {
		this.depthNoteworthyInd = depthNoteworthyInd;
	}
	public int getDeptID() {
		return deptID;
	}
	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}
	public float getNetReg() {
		return netReg;
	}
	public void setNetReg(float netReg) {
		this.netReg = netReg;
	}
	public char getRegNoteworthyInd() {
		return regNoteworthyInd;
	}
	public void setRegNoteworthyInd(char regNoteworthyInd) {
		this.regNoteworthyInd = regNoteworthyInd;
	}
	public int getResultID() {
		return resultID;
	}
	public void setResultID(int resultID) {
		this.resultID = resultID;
	}
	public float getSale() {
		return sale;
	}
	public void setSale(float sale) {
		this.sale = sale;
	}
	public char getSaleNoteworthyInd() {
		return saleNoteworthyInd;
	}
	public void setSaleNoteworthyInd(char saleNoteworthyInd) {
		this.saleNoteworthyInd = saleNoteworthyInd;
	}
	public int getStoreID() {
		return storeID;
	}
	public void setStoreID(int storeID) {
		this.storeID = storeID;
	}
	public String getWeekEndDate() {
		return weekEndDate;
	}
	public void setWeekEndDate(String weekEndDate) {
		this.weekEndDate = weekEndDate;
	}
	public String getWeekStartDate() {
		return weekStartDate;
	}
	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}
	public int getNumOfItems() {
		return numOfItems;
	}
	public void setNumOfItems(int numOfItems) {
		this.numOfItems = numOfItems;
	}
	public int getNumOfStores() {
		return numOfStores;
	}
	public void setNumOfStores(int numOfStores) {
		this.numOfStores = numOfStores;
	}
}

