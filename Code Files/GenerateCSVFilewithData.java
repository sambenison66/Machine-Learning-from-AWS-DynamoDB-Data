/*
 * Samuel Benison
 * sambenison66@gmail.com
 */

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import au.com.bytecode.opencsv.CSVWriter;
 
public class GenerateCSVFilewithData
{
 
   public static void generateCsvFile(String sFileName, Map<Integer, String> nosqlData)
   {
	   System.out.println("Generating CSV file with the DynamoDB Data");
	   
	   long startTime = System.currentTimeMillis(); // Start Time
	   
		try
		{
			FileWriter fileWriter=new FileWriter(sFileName);
			CSVWriter writer=new CSVWriter(fileWriter,',');
			
			String[] lineContent = new String[10];
		    
		    // CSV Header Content
		    lineContent[0] = "STATION";
		    lineContent[1] = "STATIONNAME";
		    lineContent[2] = "DATE";
		    lineContent[3] = "EVAP";
		    lineContent[4] = "PRCP";
		    lineContent[5] = "TOBS";
		    lineContent[6] = "AWND";
		    
		    writer.writeNext(lineContent);
		    
		    // Converting the HashMap to CSV file
		    for(int i=1; i<=nosqlData.size(); i++) {
		    	String value = nosqlData.get(i);
		    	lineContent = value.split(",");
		    	writer.writeNext(lineContent);
		    }
	 
		    writer.flush();
		    fileWriter.flush();
		    writer.close();
		    fileWriter.close();
		    
		    System.out.println("CSV File generated successfully");
		}
		catch(IOException e)
		{
		     e.printStackTrace();
		} finally {
        	// Time Calculation
        	long endTime = System.currentTimeMillis();
        	long timeTaken = endTime - startTime;
        	System.out.println("Time Taken for generating DynamoDB data to CSV file : " + timeTaken + " ms");
        }
    }
}