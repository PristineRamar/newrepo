package com.pristine.service.offermgmt.prediction;

public class PredictionException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String message = null;
    
    public PredictionException() {
        super();
    }
 
    public PredictionException(String message) {
        super(message);
        this.message = message;
    }
 
    public PredictionException(Throwable cause) {
        super(cause);
    }
 
    @Override
    public String toString() {
        return message;
    }
 
    @Override
    public String getMessage() {
        return message;
    }	
	
}
