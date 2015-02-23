/*
 * Samuel Benison
 * sambenison66@gmail.com
 */

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
 


import java.io.File;
 
public class CSVtoARFFConverter {

  public static void converter(String csvInputFile, String arffInputFile) throws Exception {
 
	long startTime = System.currentTimeMillis(); // Start Time
	  
	System.out.println("Generating ARFF file from CSV...");
    // load CSV
    CSVLoader loader = new CSVLoader();
    loader.setSource(new File(csvInputFile));
    Instances data = loader.getDataSet();
 
    // save ARFF
    ArffSaver saver = new ArffSaver();
    saver.setInstances(data);
    saver.setFile(new File(arffInputFile));
    saver.setDestination(new File(arffInputFile));
    saver.writeBatch();
    
    System.out.println("ARFF file generated successfully...");
    
    // Time Calculation
	long endTime = System.currentTimeMillis();
	long timeTaken = endTime - startTime;
	System.out.println("Time Taken for Generate ARFF file from CSV file : " + timeTaken + " ms");

  }
}