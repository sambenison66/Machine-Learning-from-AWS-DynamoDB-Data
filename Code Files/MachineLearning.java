/*
 * Samuel Benison
 * sambenison66@gmail.com
 */

import java.util.HashMap;
import java.util.Map;


public class MachineLearning {
	
	static Map<Integer, String> nosqlData = new HashMap<Integer,String>();
	static String csvInputFile = "c:\\PA4\\weather-data-dynamodb-1.csv";
	static String arffInputFile = "c:\\PA4\\weather-data-weka-1.arff";

	public static void main(String[] args) throws Exception {
		
		nosqlData = GetFromDynamoDB.getAWSData();
		GenerateCSVFilewithData.generateCsvFile(csvInputFile, nosqlData);
		CSVtoARFFConverter.converter(csvInputFile, arffInputFile);

	}

}
