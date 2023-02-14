package nerd.utopian.util;

import static java.util.Objects.requireNonNull;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.AWS_DYNAMO_DB_TABLE;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.AWS_KINESIS_STREAM_NAME;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.AWS_S3_BUCKET_NAME;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.AWS_S3_TTL_HRS;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.RECORDS_COUNT;
import static nerd.utopian.config.MovieDataProducerConfiguration.getAmazonKinesisClient;
import static nerd.utopian.config.MovieDataProducerConfiguration.getAmazonS3Client;
import static nerd.utopian.config.MovieDataProducerConfiguration.getDynamoDB;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.CollectionUtils;
import com.google.common.net.MediaType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import nerd.utopian.model.BoxOffice;
import nerd.utopian.model.IngestionRequest;
import nerd.utopian.model.IngestionResponse;
import nerd.utopian.model.Movie;
import nerd.utopian.repo.CityRepository;
import nerd.utopian.repo.MovieRepository;

public final class MovieDataProducerUtil {

  private static final Random random = new Random();

  public static List<PutRecordsRequestEntry> prepareKinesisRecords(
      List<BoxOffice> boxOfficeCollection) {

    if (CollectionUtils.isNullOrEmpty(boxOfficeCollection)) {
      throw new RuntimeException("boxOfficeCollection list for ingestion is empty");
    }

    Gson recordGson = new GsonBuilder().setPrettyPrinting().create();
    List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();

    for (BoxOffice boxOffice : boxOfficeCollection) {
      PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
      putRecordsRequestEntry.setData(ByteBuffer.wrap(recordGson.toJson(boxOffice).getBytes(
          StandardCharsets.UTF_8)));
      putRecordsRequestEntry.setPartitionKey(UUID.randomUUID().toString());
      putRecordsRequestEntries.add(putRecordsRequestEntry);
    }

    return putRecordsRequestEntries;
  }

  public static PutObjectRequest prepareS3PutObjectRequest(
      List<BoxOffice> boxOfficeCollection) {

    if (CollectionUtils.isNullOrEmpty(boxOfficeCollection)) {
      throw new RuntimeException("boxOfficeCollection list for ingestion is empty");
    }

    Date expirationTime = Date.from(LocalDateTime.now().plusHours(AWS_S3_TTL_HRS).atZone(
        ZoneId.systemDefault()).toInstant());

    Gson recordGson = new GsonBuilder().setPrettyPrinting().create();

    byte[] objectDocumentBytes = recordGson.toJson(boxOfficeCollection).getBytes(
        StandardCharsets.UTF_8);
    ByteArrayInputStream objectDocumentByteStream = new ByteArrayInputStream(objectDocumentBytes);

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setExpirationTime(expirationTime);
    objectMetadata.setContentLength(objectDocumentBytes.length);
    //objectMetadata.setUserMetadata();
    objectMetadata.setContentType(MediaType.PLAIN_TEXT_UTF_8.toString());

    PutObjectRequest putObjectRequest = new PutObjectRequest(AWS_S3_BUCKET_NAME,
        UUID.randomUUID().toString(), objectDocumentByteStream, objectMetadata);

    return putObjectRequest;
  }

  public static TableWriteItems prepareDynamoDBItems(
      List<BoxOffice> boxOfficeCollection) {

    if (CollectionUtils.isNullOrEmpty(boxOfficeCollection)) {
      throw new RuntimeException("boxOfficeCollection list for ingestion is empty");
    }

    List<Item> itemsList = new ArrayList<>();

    for (BoxOffice boxOffice : boxOfficeCollection) {
      Gson movieJson = new GsonBuilder().setPrettyPrinting().create();
      Item itemEntry = new Item()
          .withPrimaryKey("order_id", boxOffice.getOrderId())
          .withString("city", boxOffice.getCity())
          .withString("movie", movieJson.toJson(movieJson));
      itemsList.add(itemEntry);
    }

    return new TableWriteItems(AWS_DYNAMO_DB_TABLE)
        .withItemsToPut(itemsList);
  }

  public static List<BoxOffice> getBoxOfficeCollection() {

    List<BoxOffice> boxOfficeCollection = new ArrayList<>();

    for (long i = 1; i < RECORDS_COUNT; i++) {

      int moviesCount = MovieRepository.getMovieList().size();
      int citiesCount = CityRepository.getCityList().size();

      Movie movie = MovieRepository.getMovieList().get(random.nextInt(moviesCount));
      String city = CityRepository.getCityList().get(random.nextInt(citiesCount));

      BoxOffice boxOffice = new BoxOffice(random.nextLong(),
          city, movie);
      boxOfficeCollection.add(boxOffice);
    }

    return boxOfficeCollection;
  }

  public static PutRecordsResult pushToKinesis(Context context) {

    List<PutRecordsRequestEntry> totalRecords = prepareKinesisRecords(
        getBoxOfficeCollection());

    if (CollectionUtils.isNullOrEmpty(totalRecords)) {
      context.getLogger().log("records list for ingestion is empty");
      throw new RuntimeException("records list for ingestion is empty");
    }

    PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
    putRecordsRequest.setRecords(totalRecords);
    putRecordsRequest.setStreamName(AWS_KINESIS_STREAM_NAME);

    PutRecordsResult putRecordsResult = getAmazonKinesisClient()
        .putRecords(putRecordsRequest);

    logCloudWatch(context.getLogger(), putRecordsResult.toString());

    return putRecordsResult;
  }

  public static PutObjectResult pushToS3(Context context) {

    PutObjectRequest putObjectRequest = prepareS3PutObjectRequest(getBoxOfficeCollection());
    requireNonNull(putObjectRequest, "putObjectRequest is required and cannot be null.");

    PutObjectResult putObjectResult = getAmazonS3Client().putObject(putObjectRequest);

    logCloudWatch(context.getLogger(), putObjectResult.toString());

    return putObjectResult;
  }

  public static BatchWriteItemResult pushToDynamoDB(Context context) {

    requireNonNull(context, "context is required and cannot be null.");

    BatchWriteItemOutcome batchWriteItemOutcome = getDynamoDB().batchWriteItem(
        prepareDynamoDBItems(getBoxOfficeCollection()));

    requireNonNull(batchWriteItemOutcome, "batchWriteItemOutcome is required and cannot be null.");
    logCloudWatch(context.getLogger(), batchWriteItemOutcome.getBatchWriteItemResult().toString());

    return batchWriteItemOutcome.getBatchWriteItemResult();
  }

  public static IngestionResponse prepareIngestionResponse(Object ingestionResult,
      IngestionRequest ingestionRequest) {

    requireNonNull(ingestionResult, "ingestionResult is required and cannot be null.");
    requireNonNull(ingestionRequest, "ingestionRequest is required and cannot be null.");

    IngestionResponse ingestionResponse = new IngestionResponse(ingestionResult.toString(),
        ingestionRequest);
    return ingestionResponse;
  }

  private static void logCloudWatch(LambdaLogger logger, String message) {
    logger.log("######## Producer Stats ########");
    logger.log(message);
  }
}
