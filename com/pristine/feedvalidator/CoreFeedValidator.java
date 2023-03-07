package com.pristine.feedvalidator;

import java.util.List;

public abstract class CoreFeedValidator<T> {
	abstract public void validateFeed(List<T> inputList) throws Exception;
		
	abstract public List<T> getErrorRecordsAlone(List<T> inputList) throws Exception;
	
	abstract public List<T> getNonErrorRecords(List<T> inputList) throws Exception;

}
