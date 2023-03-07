package com.pristine.test.offermgmt;

import java.util.ArrayList;
import java.util.List;

import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.service.offermgmt.multiWeekPrediction.MultiWeekPredictionEngine;
import com.pristine.service.offermgmt.prediction.PredictionEngine;
import com.pristine.service.offermgmt.prediction.PredictionEngineBatchImpl;
import com.pristine.service.offermgmt.prediction.PredictionEngineImpl;
import com.pristine.service.offermgmt.prediction.PredictionEngineInput;
import com.pristine.service.offermgmt.prediction.PredictionException;

public class SynchronizeTest implements Runnable {

	public void run() {
//		PredictionEngine p = new PredictionEngineBatchImpl("", "", 1, true);
//		p.test();
	}

	public static void main(String args[]) {

//		weekly prediction
		new Thread(new Runnable() {
			public void run() {
				PredictionEngineImpl p = new PredictionEngineImpl("", "", 1, true);
				try {
					p.weeklyPrediction(new PredictionEngineInput(), false, false, false);
				} catch (PredictionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
//
//		 try{Thread.sleep(10000);}catch(InterruptedException e){System.out.println(e);}  
//		//multi week prediction
		new Thread(new Runnable() {
			public void run() {
				PredictionEngineImpl mp = new PredictionEngineImpl("", "", 1, true);
				try {
					mp.multiWeekPrediction(new ArrayList<MultiWeekPredEngineItemDTO>());
				} catch (PredictionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();

	}

}