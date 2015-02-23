/*
 * Samuel Benison
 * sambenison66@gmail.com
 */

/*
 * Retrieve csv file from AWS S3 bucket to DynamoDB
 */
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3toDynamoDBUpload {
	
	//Declaring a global DynamoDB instance
	static AmazonDynamoDBClient dynamoDB;
	
	//DynamoDB table name
	static String tableName = "new-weather-statistics-1";
	
	//S3 Bucket and File Name
	static String bucketName = "pa4-bucket";
    static String key = "PA4dataset2.csv";
	
	public static void main(String[] args) throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [Sam]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Samuel\\.aws\\AWSCredentials.properties).
         */
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

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);
        
        dynamoDB = new AmazonDynamoDBClient(credentials);
        dynamoDB.setRegion(usWest2);
        
        long startTime = System.currentTimeMillis(); // Start Time

        try {

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            
            createDynamoDBTable();
            procesCSVtoDynamoDB(object.getObjectContent());
            /*procesCSVtoDynamoDB(object.getObjectContent(), 1000);
            procesCSVtoDynamoDB(object.getObjectContent(), 2000);
            procesCSVtoDynamoDB(object.getObjectContent(), 3000);*/

            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("PA4"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } finally {
        	// Time Calculation
        	long endTime = System.currentTimeMillis();
        	long timeTaken = endTime - startTime;
        	System.out.println("Time Taken for Importing data : " + timeTaken + " ms");
        }
    }
	
	/*
	 * Create a table to DynamoDB
	 */
	private static void createDynamoDBTable() throws Exception {

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("STATION").withKeyType(KeyType.HASH),
                    		new KeySchemaElement().withAttributeName("DATE").withKeyType(KeyType.RANGE))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("STATION").withAttributeType(ScalarAttributeType.S))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("DATE").withAttributeType(ScalarAttributeType.N))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L));
                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
            }

            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);
	}

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void procesCSVtoDynamoDB(InputStream input) throws IOException {
        
        CSVReader csvReader = null;
        System.out.println("Reading the CSV File...");
		try {
			// Read the CSV file
			csvReader = new CSVReader(new InputStreamReader(input));

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// CSV File header
		String[] headerRow = csvReader.readNext();
		
		String[] nextLine = null;

		if (headerRow == null) {
			throw new FileNotFoundException(
					"No columns defined in given CSV file." +
					"Please check the CSV file format.");
		}
		int count = 0;
		System.out.println("Inserting the Rows from CSV to Amazon DynamoDB.");
		System.out.println("  This might take serveral minutes.. Please wait..");
		// Reading the each Row one by one
		while ((nextLine = csvReader.readNext()) != null) {
			String station = nextLine[0]; // station id
			String station_Name = nextLine[1]; // station name
			int date = Integer.parseInt(nextLine[2]); // date
			int water = Integer.parseInt(nextLine[3]); // evaporation water
			int precp = Integer.parseInt(nextLine[4]); // precipitation
			int tobs = Integer.parseInt(nextLine[5]); // average temperature
			int wind = Integer.parseInt(nextLine[6]); // average wind
			count = count + 1;
			
			// Add an item to DynamoDB table
            Map<String, AttributeValue> item = newItem(station, station_Name, date,
            											water, precp, tobs, wind);
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            //System.out.println("Result: " + putItemResult);
            if((count%100) == 0) {
            	System.out.println("Processed " + count + " rows..");
            }
		}
		csvReader.close();
		System.out.println("CSV to NoSQL Import completed successfully..!!");
		System.out.println("Total Number of Tuples Inserted : " +count);
        System.out.println();
    }
    
    /*
     * Process each Item attributes and create a new item
     */
    private static Map<String, AttributeValue> newItem(String station, String station_Name, 
    						int date, int water, int precp, int tobs, int wind) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("STATION", new AttributeValue(station));
        item.put("STATIONNAME", new AttributeValue(station_Name));
        item.put("DATE", new AttributeValue().withN(Integer.toString(date)));
        if(water != -9999) {
        	item.put("EVAP", new AttributeValue().withN(Integer.toString(water)));
        } /*else {
        	water = 0;
        	item.put("EVAP", new AttributeValue().withN(Integer.toString(water)));
        }*/
        if(precp != -9999) {
        	item.put("PRCP", new AttributeValue().withN(Integer.toString(precp)));
        } /*else {
        	precp = 0;
        	item.put("PRCP", new AttributeValue().withN(Integer.toString(precp)));
        }*/
        if(tobs != -9999) {
        	item.put("TOBS", new AttributeValue().withN(Integer.toString(tobs)));
        } /*else {
        	tobs = 0;
        	item.put("TOBS", new AttributeValue().withN(Integer.toString(tobs)));
        }*/
        if(wind != -9999) {
        	item.put("AWND", new AttributeValue().withN(Integer.toString(wind)));
        } else {
        	wind = 0;
        	item.put("AWND", new AttributeValue().withN(Integer.toString(wind)));
        }
        return item;
    }

}
