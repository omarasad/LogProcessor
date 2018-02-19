package edu.mcgill.disl.analytics;

import org.apache.commons.math3.stat.regression.*;

public class TestLinearRegrssn {
	
	public static void main(String args[])
	{
	    double[] features  = new double[5];
	    double[] result = new double[5];
	    SimpleRegression sm = new SimpleRegression();
	    
	    for (int x=0;x<5;x++)
	    {
	    	sm.addData(x,x+1);
    	
	    }
	    
	    System.out.println("slope ="+ sm.getSlope());
	    System.out.println("intercept ="+ sm.getIntercept());
	    
        System.out.println("prediction for 1.5 = " + sm.predict(1.5));
	    
		
		
		
		
		
		
		
	}
	

}
