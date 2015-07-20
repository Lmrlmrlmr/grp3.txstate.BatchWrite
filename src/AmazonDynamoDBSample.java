import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class AmazonDynamoDBSample {

    /*
     * FROM API EXAMPLES
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (C:\\Users\\PaleToys\\.aws\\credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void initialize() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\PaleToys\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try
        {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        }
        catch (Exception e)
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\PaleToys\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
    }

    public static void main(String[] args) throws Exception {
        initialize();

        try {
            String tableName = "WorkingDatabaseV0.1";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName))
            {
                System.out.println("Table " + tableName + " is already ACTIVE");
            }
            else
            {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest()
                	.withTableName(tableName)
                    .withKeySchema(new KeySchemaElement()
                    .withAttributeName("name")
                    .withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition()
                    .withAttributeName("name")
                    .withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput()
                    .withReadCapacityUnits(1L)
                    .withWriteCapacityUnits(1L));
                    
                TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);
                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.awaitTableToBecomeActive(dynamoDB, tableName);
            }

            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);

            
            //Let's open up a file
            //loop to read file
            BufferedReader in = new BufferedReader(new FileReader("src\\input.txt"));
            for(int i = 0; i < 11; i++){
            String name = in.readLine();
            String phone_number = in.readLine();
            String description = in.readLine();
            String location0 = in.readLine();
            String location1 = in.readLine();
            String food_type = in.readLine();
            String picture_url = in.readLine();
            String address = in.readLine();
            String menu = in.readLine();
            String menu2 = in.readLine();
            String menu3 = in.readLine();
            
            // Add an item
            //String name, String phone_number, String description, String location0, String location1,String food_type, String... fans
            Map<String, AttributeValue> item = newItem(name,//Name
            		phone_number,//Phone Number
            		description,//Description
            		location0,//location0
            		location1,//location1
            		food_type,//Food Type
            		picture_url,//Picture
            		address,//Addresss
            		menu,menu2,menu3);//Menu
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            System.out.println("Result: " + putItemResult);
            }
            in.close();
            
        }
        /*
         * Error messages from .api Examples provided with the sdk. 
         */
        catch (AmazonServiceException ase) //From SDK example
        {
            System.out.println("FUDGE! Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace)//From SDK example
        {
            System.out.println("FUDGE! Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static Map<String, AttributeValue> newItem(String name,
    		String phone_number, 
    		String description, 
    		String location0, 
    		String location1,
    		String food_type,
    		String URL, 
    		String address, 
    		String... menu)
    	{
    		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    		item.put("name", new AttributeValue(name));
    		item.put("phone_number", new AttributeValue(phone_number));
    		item.put("description", new AttributeValue(description));
    		item.put("location0", new AttributeValue(location0));
        	item.put("location1", new AttributeValue(location1));
        	item.put("location1", new AttributeValue(food_type));
        	item.put("photo", new AttributeValue(URL));
        	item.put("address", new AttributeValue(address));
        	item.put("menu", new AttributeValue().withSS(menu));
        	return item;
    }
}