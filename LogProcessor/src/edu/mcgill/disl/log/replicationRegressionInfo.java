package edu.mcgill.disl.log;

import java.awt.image.ReplicateScaleFilter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.math3.stat.regression.*;

import edu.mcgill.disl.analytics.Analyser;


public class replicationRegressionInfo {
	
	public static final Map<Integer, String> featuresIndexingByKeys = new HashMap<Integer, String>(3);
	public static final Map<String, Integer> featuresIndexingByValues = new HashMap<String, Integer>(3);
	public int maxIndex=-1;
	public HashMap<String, Integer> objectKeys;
	public double[] regressionParameters; 
	double pearson;
	int dataSize=100;
	
	public int totalObjectDistFreq=0;
	public int totalObjectFreq=0;
	
	
	public static final int readFreq=0;
	//public static final int writeFreq=1;
	public static final int distFreq=1;
	
	public static final int updateFreq=2;
	
	public static final int resTime=3;
	
	
	
	public final String  filename="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/training.csv";
	
	public final String  filenameAll="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/trainingAll.csv";

	
	

	public replicationRegressionInfo(int objectNo)
	{
		
		objectKeys= new HashMap<String, Integer>(objectNo);
		
		/*
		featuresIndexingByKeys.put(0, readFreq);
		featuresIndexingByValues.put(readFreq,0);
		
		featuresIndexingByKeys.put(1, writeFreq);
		featuresIndexingByValues.put(writeFreq,1);
		
		featuresIndexingByKeys.put(2, distFreq);
		featuresIndexingByValues.put(distFreq,2);
		*/
	}
	
	
	
	
	//public HashMap<String, double []> objectsTrainingInput= new HashMap<String, double []>(1000); // the key is the object string the hashset of double is the features
	//public HashMap<String, double []> objectsTrainingOutput= new HashMap<String, double []>(1000); // the key is the object string the hashset of double is the output
	
	
	public double[] y = new double [dataSize];  //y array contains the output values
	public double[][] x = new double [dataSize][4]; // x array contains the input (features)
	
	public double[][] yy= new double [dataSize][4]; //yy is a temporary array contains the two output values of cache hit 1 and cache hit 2
	public double[][] yyRemote= new double [dataSize][4]; //yy is a temporary array contains the two output values of cache hit 1 and cache hit 2
	

	public int getKey(String obj)
		{		
			if (objectKeys==null)
			{
				maxIndex++;
				objectKeys.put(obj, maxIndex);
				return maxIndex;
			}	
			if (!objectKeys.containsKey(obj))
			{
				maxIndex++;
				objectKeys.put(obj, maxIndex);
				return maxIndex;
					
			}
				else 
					return objectKeys.get(obj);
			}
	public void addFeature(String object, int featureIndex, double featureValue)
	{
		int index= getKey(object);
	//	Analyser.log.info("addFeature index="+index+"featureIndex"+featureIndex+"x[0][0]"+x[0][0]);
		
		x[index][featureIndex]=featureValue;
	}
	
	public void addOutput(String object, int outputIndex, double outputValue)
	{
		int index= getKey(object);
		yy[index][outputIndex]=outputValue;			
	}
	
	public void addOutputRemote(String object, int outputIndex, double outputValue)
	{
		int index= getKey(object);
		yyRemote[index][outputIndex]=outputValue;			
	}
	
	
	
	
	private static double calculateEstimation(double x, double[] coe) {
	    double result = 0;
	    for(int i = 0; i < coe.length; ++i)
	        result += coe[i] * Math.pow(x, i); // 1
	    return result;
	}
	
	public static void dumpEstimation(double[] coe) {
	    if(coe == null)
	        return;
	 
	    for(double d : coe)
	        System.out.print(d + " ");
	    System.out.println();
	 
	    System.out.println("Estimations:");
	    System.out.println("x = 1, y = " + calculateEstimation(1, coe));
	    System.out.println("x = 2, y = " + calculateEstimation(2, coe));
	    System.out.println("x = 3, y = " + calculateEstimation(3, coe));
	    System.out.println("x = 4, y = " + calculateEstimation(4, coe));
	}
	
	public void printData()
	{
		for(String obj:objectKeys.keySet())
		{
			int key=objectKeys.get(obj);
			Analyser.log.info(obj + " " +x[key][0]+"==="+"==="+x[key][1]+"=y1="+yy[key][0]+"=y2="+yy[key][1]+"=yyRemote1="+yyRemote[key][0]+"=yyRemote2="+yyRemote[key][1]+"==y=="+y[key]);
			
			
			
			
		} 
	}
	
	@SuppressWarnings("null")
	public void writeToFile(String toPrint,String toPrintAll, String filePath,String filePathAll)
	{
		 
		
		Analyser.log.info("writeToFileNEWWW");
		PrintWriter out1 = null;
		PrintWriter out2 = null;
		try {
			out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
			out2 = new PrintWriter(new BufferedWriter(new FileWriter(filenameAll, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		out1.println(toPrint);
		out2.println(toPrintAll);
		out1.close();
		out2.close();
	}
	
	/*
	public void printAsCVS()
	{
		StringBuilder sb = null;
		
		for(String obj:objectKeys.keySet())
		{
			int key=objectKeys.get(obj);
		
			sb.append(y[key]+","+x[key][0]+","+x[key][1]+"\n");
			
		}
		writeToFile(sb.toString(), filename);
	}
	*/
	public void doRegression()
	{
		/*
		for (int x=0;x<yy.length;x++)
		{
			y[x]=yy[x][1]-yy[x][0];
			
		}
		*/
		for (int x=0;x<yy.length;x++)
		{
			//y[x]=yy[x][0]*0.2+ yyRemote[x][0]*0.2 - (yy[x][1]*2.0+ yyRemote[x][1]*2.0);
			
		//y[x]=(yy[x][1]- yy[x][0])*0.2 + (yyRemote[x][0]- yyRemote[x][1])*2.0; //work fine
		
		y[x]=yy[x][0]- yy[x][1];
			
			
			
			//y[x]=(yy[x][0]*0.2+ yyRemote[x][0]*2.0) - (yy[x][1]*0.2+ yyRemote[x][1]*2.0); // work fine but with - for popularity
			
			
			 
			
			
			
		}
		printData();
		
		
		//printAsCVS();
		
		
		
		
		
		OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
		regression.newSampleData(y, x);
	
	        
		 Analyser.log.info("regression.estimateRegressandVariance()"+regression.estimateRegressandVariance());

		 double[] coe=regression.estimateRegressionParameters();
	        
	       // dumpEstimation(coe);
	        
		 regressionParameters=coe.clone();
	        ////////////////////////
	       //  regressionParameters = regression.estimateRegressionParameters();
		     pearson = Math.sqrt(regression.calculateRSquared());
		    //LOG.info("coefficients are " + simlarityCoefficients.toString());
		    Analyser.log.info("pearson for multiple regression is " + pearson);
		    for (int i = 0; i < coe.length; i++) 
		    {
		        double regressionParameter = coe[i];
		        Analyser.log.info(i + " " + regressionParameter);
		    }
		    
		   printObjectPredictedInfoNew();

	}
	
	double calculateValue(double[] card, double[] regressionParameters){
        //regression param 0 is y intercept
        //1 = atk
        //2 = health
        //3 = charge
        //4 = shield

        return card[0] * regressionParameters[1] //attack
                    + card[1] * regressionParameters[2]; //health
               // + card[2] * regressionParameters[3] * regressionParameters[1] //charge
              //  + card[3] * regressionParameters[4];// * regressionParameters[1] * 1.1; //divine shield // * attack * extra health
}
	
	
	public void printObjectPredictedInfoNew()
	{
		StringBuffer sb=new StringBuffer();
		StringBuffer sbAll=new StringBuffer();
		for(String obj:objectKeys.keySet())
		{
			int key=objectKeys.get(obj);
			double total=x[key][0];
			double remote=x[key][1];
			double update=x[key][2];
			
			double local=total-(remote+update);
			
			
			
			remote=remote/total;
			local=local/total;
			update=update/total;
			
			double tot=x[key][0]/(double)totalObjectFreq;
			
			DecimalFormat df = new DecimalFormat("#.######"); 
			df.setRoundingMode(RoundingMode.CEILING); //Optional
			Double totNew = new Double(tot);
			Double totalNew=Double.parseDouble(df.format(totNew));
			
			local=Math.round(local*100.0)/100.0;
			remote=Math.round(remote*100.0)/100.0;
			update=Math.round(update*100.0)/100.0;
			
			
			
			//System.out.println(df.format(d));
			
			
			
			//Analyser.log.info("totAl="+ totalNew + "obj="+obj + "pop= " +x[key][0]+"=Freq="+x[key][1]+"=time0=="+x[key][2]+"=time1=="+yy[key][0]+"=time2=="+yy[key][1]+"===estimated Score==="+calculateValue(x[key], regressionParameters) );
			Analyser.log.info(obj + "=" +x[key][0]+" loc="+local+" rem="+remote+" upd="+update+" t1="+Math.round(yy[key][0]*100.0)/100.0+" t2="+Math.round(yy[key][1]*100.0)/100.0+" TDf="+Math.round(y[key]*100.0)/100.0);
			
			/*
			sbAll.append(Math.round(y[key]*100.0)/100.0+","+Math.round(x[key][0]*100.0)/100.0+","+Math.round(x[key][1]*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");
			if (x[key][1]>0)
				sb.append(Math.round(y[key]*100.0)/100.0+","+Math.round(x[key][0]*100.0)/100.0+","+Math.round(x[key][1]*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");
			*/

			 sbAll.append(Math.round(y[key]*10000000.0)/10000000.0+","+totalNew+","+Math.round(local*100.0)/100.0+","+Math.round(remote*100.0)/100.0+","+Math.round(update*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");
			//if (remote>0 && update>0) 
			 if (remote==0 && update>0)
				sb.append(Math.round(y[key]*10000000.0)/10000000.0+","+totalNew+","+Math.round(local*100.0)/100.0+","+Math.round(remote*100.0)/100.0+","+Math.round(update*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");

			
		}
		String toWrite=sb.toString();
		String toWriteAll=sbAll.toString();
		writeToFile(toWrite,toWriteAll, filename,filenameAll);
	}
	
	public void printObjectPredictedInfo()
	{
		StringBuffer sb=new StringBuffer();
		StringBuffer sbAll=new StringBuffer();
		for(String obj:objectKeys.keySet())
		{
			
			
					
			
			int key=objectKeys.get(obj);
			double total=x[key][0];
			double remote=x[key][1];
			double local=total-remote;
			
			Analyser.log.info(obj + " " +x[key][0]+"==="+"==="+x[key][1]+"=y1="+yy[key][0]+"=y2="+yy[key][1]+"===estimated Score==="+calculateValue(x[key], regressionParameters) );
			if (x[key][1]>0)
				sb.append(Math.round(y[key]*100.0)/100.0+","+Math.round(x[key][0]*100.0)/100.0+","+Math.round(x[key][1]*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");
			sbAll.append(Math.round(y[key]*100.0)/100.0+","+Math.round(x[key][0]*100.0)/100.0+","+Math.round(x[key][1]*100.0)/100.0+","+ totalObjectDistFreq +","+ totalObjectFreq+"\n");
			
			
		}
		String toWrite=sb.toString();
		String toWriteAll=sb.toString();
		writeToFile(toWrite,toWriteAll, filename,filenameAll);
		//writeToFile(toWriteAll, filenameAll);
	}
	

}
