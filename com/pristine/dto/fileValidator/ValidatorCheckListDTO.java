package com.pristine.dto.fileValidator;

public class ValidatorCheckListDTO {

	private String inputType;
	private boolean checkWithPresto;
	private String columnName;
	private String prestoValidatorMapName;
	
	public String getInputType() {
		return inputType;
	}
	public void setInputType(String inputType) {
		this.inputType = inputType;
	}
	public boolean isCheckWithPresto() {
		return checkWithPresto;
	}
	public void setCheckWithPresto(boolean checkWithPresto) {
		this.checkWithPresto = checkWithPresto;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getPrestoValidatorMapName() {
		return prestoValidatorMapName;
	}
	public void setPrestoValidatorMapName(String prestoValidatorMapName) {
		this.prestoValidatorMapName = prestoValidatorMapName;
	}
}
