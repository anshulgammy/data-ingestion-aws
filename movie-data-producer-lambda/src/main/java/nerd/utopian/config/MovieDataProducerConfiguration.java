package nerd.utopian.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public final class MovieDataProducerConfiguration {

  public static final class Constants {

    private static final String AWS_ACCESS_KEY = "";
    private static final String AWS_SECRET_KEY = "";
    private static final Regions AWS_REGION = Regions.AP_SOUTH_1;
    public static final long RECORDS_COUNT = 10;
    public static final String AWS_KINESIS_STREAM_NAME = "movie-data-kinesis-stream";
    public static final String MOVIE_DATA_PRODUCER_LAMBDA_NAME = "movie-data-producer-lambda";
    public static final String AWS_S3_BUCKET_NAME = "movie-data-s3-bucket";
    public static final long AWS_S3_TTL_HRS = 48;
    public static final String AWS_DYNAMO_DB_TABLE = "movie-data-table";
    public static final String DESTINATION_S3 = "pushToS3";
    public static final String DESTINATION_KINESIS = "pushToKinesis";
    public static final String DESTINATION_DYNAMODB = "pushToDynamoDB";
  }

  private static AmazonKinesis amazonKinesis = null;
  private static AmazonDynamoDB amazonDynamoDB = null;
  private static DynamoDB dynamoDB = null;

  public static AmazonKinesis getAmazonKinesisClient() {

    if (amazonKinesis == null) {

      amazonKinesis = AmazonKinesisClientBuilder
          .standard()
          .withRegion(Constants.AWS_REGION)
          .withCredentials(getAWSCredentialsProvider())
          .build();
    }

    return amazonKinesis;
  }

  public static AmazonS3 getAmazonS3Client() {

    AmazonS3 s3client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(getAWSCredentialsProvider())
        .withRegion(Regions.US_EAST_2)
        .build();

    return s3client;
  }

  public static DynamoDB getDynamoDB() {

    if (dynamoDB == null) {
      dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder
          .standard()
          .withRegion(Constants.AWS_REGION)
          .withCredentials(getAWSCredentialsProvider())
          .build());
    }

    return dynamoDB;
  }

  private static AWSCredentialsProvider getAWSCredentialsProvider() {
    AWSCredentials awsCredentials = new BasicAWSCredentials(Constants.AWS_ACCESS_KEY,
        Constants.AWS_SECRET_KEY);
    AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(
        awsCredentials);
    return awsCredentialsProvider;
  }
}
