package nerd.utopian.handler;

import static java.util.Objects.requireNonNull;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.DESTINATION_DYNAMODB;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.DESTINATION_KINESIS;
import static nerd.utopian.config.MovieDataProducerConfiguration.Constants.DESTINATION_S3;
import static nerd.utopian.util.MovieDataProducerUtil.prepareIngestionResponse;
import static nerd.utopian.util.MovieDataProducerUtil.pushToDynamoDB;
import static nerd.utopian.util.MovieDataProducerUtil.pushToKinesis;
import static nerd.utopian.util.MovieDataProducerUtil.pushToS3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import nerd.utopian.model.IngestionRequest;
import nerd.utopian.model.IngestionResponse;

public class MovieDataProducerHandler implements
    RequestHandler<IngestionRequest, IngestionResponse> {

  @Override
  public IngestionResponse handleRequest(IngestionRequest ingestionRequest, Context context) {

    requireNonNull(ingestionRequest, "ingestionRequest is required and cannot be null.");
    requireNonNull(context, "context is required and cannot be null.");

    String destination = ingestionRequest.getDestination();

    try {

      switch (destination) {

        case DESTINATION_KINESIS: {
          return prepareIngestionResponse(pushToKinesis(context), ingestionRequest);
        }
        case DESTINATION_S3: {
          return prepareIngestionResponse(pushToS3(context), ingestionRequest);
        }
        case DESTINATION_DYNAMODB: {
          return prepareIngestionResponse(pushToDynamoDB(context), ingestionRequest);
        }
        default:
          System.exit(0);
      }

    } catch (Exception ex) {
      return prepareIngestionResponse(ex.getMessage(), ingestionRequest);
    }

    return null;
  }
}
