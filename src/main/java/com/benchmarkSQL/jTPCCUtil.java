package com.benchmarkSQL;
/*
 * jTPCCUtil - utility functions for the Open Source Java implementation of 
 *    the TPC-C benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.*;
 
public class jTPCCUtil implements jTPCCConfig 
{
    

  public static String getSysProp(String inSysProperty, String defaultValue) {
    
    String outPropertyValue = null;

    try {
      outPropertyValue = System.getProperty(inSysProperty, defaultValue);
      if (inSysProperty.equals("password")) {
        System.out.println(inSysProperty + "=*****");
      } else {
        System.out.println(inSysProperty + "=" + outPropertyValue);
      } 
    } catch (Exception e) {
        System.out.println("Error Reading Required System Property '" + 
          inSysProperty + "'");
    }

    return(outPropertyValue);

  } // end getSysProp
  
  
  public static String randomStr(long strLen){

    char freshChar;
    String freshString;
    freshString="";

    while(freshString.length() < (strLen - 1)){

      freshChar= (char)(Math.random()*128);
      if(Character.isLetter(freshChar)){
        freshString += freshChar;
      }
    }

    return (freshString);

  } // end randomStr


    public static String getCurrentTime()
    {
        return dateFormat.format(new java.util.Date());
    }

    public static String formattedDouble(double d)
    {
        String dS = ""+d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }    

    public static int getItemID(Random r)
    {
        return nonUniformRandom(8191, 1, 100000, r);
    }
    
    public static int getItemID(Random r, float factor)
    {
    	int max = (int) (configItemCount * factor);
    	if (max < 1) { max = 1;}
        return nonUniformRandom((int)(max * 0.082), 1, max, r);
    }

    public static int getCustomerID(Random r)
    {
        return nonUniformRandom(1023, 1, 3000, r);
    }
    
    public static int getCustomerID(Random r, float factor)
    {
    	int max = (int)(configCustPerDist * factor);
    	if (max < 1) { max = 1;}
        return nonUniformRandom((int)(max * 0.3), 1, max, r);
    }

    public static String getLastName(Random r)
    {
        int num = (int)nonUniformRandom(255, 0, 999, r);
        return nameTokens[num/100] + nameTokens[(num/10)%10] + nameTokens[num%10];
    }

    public static int randomNumber(int min, int max, Random r)
    {
        return (int)(r.nextDouble() * (max-min+1) + min);
    }


    public static int nonUniformRandom(int x, int min, int max, Random r)
    {
        return (((randomNumber(0, x, r) | randomNumber(min, max, r)) + randomNumber(0, x, r)) % (max-min+1)) + min;
    }
    
    /**
     * NegativeExceponential 
     * 
     * @param cycleMin in nanosec
     * @param cycleMean in nanosec
     * @param cycleMax in nanosec
     * @param random
     * @param truncate
     * @return
     */
    public static long getDelay(long cycleMin, long cycleMean, long cycleMax, Random random, boolean truncate) {
    	long delay = 0;
    	long mean = cycleMean;
    	long shift = 0;

    	if (!truncate) {
    		shift = cycleMin;
    		mean -= shift;
    	}   

    	if (cycleMean > 0) {
    		double x = drandom(0.0, 1.0, random);
    		if (x == 0) {
    			x = 1e-20d;
    		}   
    		delay = shift + (long)(mean * -Math.log(x));
    		if (delay < cycleMin) {
    			delay = cycleMin;
    		} else if (delay > cycleMax) {
    			delay = cycleMax;
    		}   
    	}   
    	return delay;
    }   

    public static double drandom(double x, double y, Random r) {
    	return (x + (r.nextDouble() * (y - x)));
    }

} // end jTPCCUtil 
