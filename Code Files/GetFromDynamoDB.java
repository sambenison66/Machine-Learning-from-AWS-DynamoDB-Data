/*
 * Samuel Benison
 * sambenison66@gmail.com
 */

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;


public class GetFromDynamoDB {
	
	static AmazonDynamoDBClient client;
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    static Map<Integer, String> nosqlData = new HashMap<Integer,String>();
    
    public static Map<Integer, String> getAWSData() throws Exception {
    	
    	AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("C:\\Users\\XXX\\.aws\\AWSCredentials.properties", "XXX").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\XXX\\.aws\\AWSCredentials.properties), and is in valid format.",
                    e);
        }
        client = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        client.setRegion(usWest2);
        
        long startTime = System.currentTimeMillis(); // Start Time
        
        try {

            DynamoDBMapper mapper = new DynamoDBMapper(client);
              
            // Scan a table and find book items priced less than specified value.
            GetWeatherStatisticsReport(mapper);
            
            
            System.out.println("Reading from DynamoDB complete!");
            
        } catch (Throwable t) {
            System.err.println("Error running the GetFromDynamoDB: " + t);
            t.printStackTrace();
        } finally {
        	// Time Calculation
        	long endTime = System.currentTimeMillis();
        	long timeTaken = endTime - startTime;
        	System.out.println("Time Taken for Reading data from DynamoDB : " + timeTaken + " ms");
        }
        return nosqlData;
    }
    
    private static void GetWeatherStatisticsReport(
            DynamoDBMapper mapper) throws Exception {
 
        System.out.println("GetWeatherStatisticsReport: Scan new-weather-statistics.");
                
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        
        List<Weather> scanResult = mapper.scan(Weather.class, scanExpression);
        
        int count = 0;
        for (Weather details : scanResult) {
            count = count + 1;
            nosqlData.put(count, details.toString());
        }
    }
    
    @DynamoDBTable(tableName="new-weather-statistics-1")
    public static class Weather {
        private int date;
        private String station;
        private String stationname;
        private int water = -9999;
        private int temp = -9999;
        private int prcp = -9999;
        private int wind = -9999;
        
        @DynamoDBAttribute(attributeName="STATION")
        public String getSTATION() { return station; }
        public void setSTATION(String station) { this.station = station; }
        
        @DynamoDBAttribute(attributeName="STATIONNAME")
        public String getSTATIONNAME() { return stationname; }
        public void setSTATIONNAME(String stationname) { this.stationname = stationname; }
        
        @DynamoDBHashKey(attributeName="DATE")
        public int getDATE() { return date; }
        public void setDATE(int date) { this.date = date; }
 
        @DynamoDBHashKey(attributeName="EVAP")
        public int getEVAP() { return water; }
        public void setEVAP(int water) { this.water = water; }
        
        @DynamoDBHashKey(attributeName="PRCP")
        public int getPRCP() { return prcp; }
        public void setPRCP(int prcp) { this.prcp = prcp; }
        
        @DynamoDBHashKey(attributeName="TOBS")
        public int getTOBS() { return temp; }
        public void setTOBS(int temp) { this.temp = temp; }
        
        @DynamoDBHashKey(attributeName="AWND")
        public int getAWND() { return wind; }
        public void setAWND(int wind) { this.wind = wind; }
      
        @Override
        public String toString() {
        	String output = "";
        	output = station + "," + stationname + "," + date + ",";
        	if(water == -9999) {
        		output = output + ",";
        	} else {
        		output = output + water + ",";
        	}
        	if(temp == -9999) {
        		output = output + ",";
        	} else {
        		output = output + temp + ",";
        	}
        	if(prcp == -9999) {
        		output = output + ",";
        	} else {
        		output = output + prcp + ",";
        	}
        	if(wind == -9999) {
        		// do nothing
        	} else {
        		output = output + wind;
        	}
            return output;            
        }

    }

}
