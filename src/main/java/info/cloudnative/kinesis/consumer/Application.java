package info.cloudnative.kinesis.consumer;

// import com.amazonaws.auth.BasicAWSCredentials;
// import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;

import java.net.InetAddress;
import java.util.UUID;
/**
 * Spring-boot application context entry point.
 *
 * @since 1.0
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

	private Logger log = LoggerFactory.getLogger(Application.class);

	@Value("${aws.access.key.id}")
	protected String awsAccessKey;

	@Value("${aws.secret.key}")
	protected String awsSecretKey;

	@Value("${aws.region}")
	protected String awsRegion;

	@Value("${system.config.streamName}")
	protected String SAMPLE_APPLICATION_STREAM_NAME;

	@Value("SampleKinesisApplication")
	protected String SAMPLE_APPLICATION_NAME;

  // Initial position in the stream when the application starts up for the first time.
  // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
  private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
          InitialPositionInStream.LATEST;

	@Autowired
	AWSCredentialsProvider awsCredentialsProvider;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	public void run(String... args) throws Exception {
		log.info("Starting...");

    String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    KinesisClientLibConfiguration kinesisClientLibConfiguration =
            new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                    SAMPLE_APPLICATION_STREAM_NAME,
                    awsCredentialsProvider,
                    workerId);
    kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);

    IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
    Worker worker = new Worker.Builder()
	    .recordProcessorFactory(recordProcessorFactory)
	    .config(kinesisClientLibConfiguration.withRegionName(awsRegion))
	    .build();

    log.info("Running {} to process stream {} as worker {}...", 
      SAMPLE_APPLICATION_NAME,
      SAMPLE_APPLICATION_STREAM_NAME,
      workerId);

    int exitCode = 0;
    try {
        worker.run();
    } catch (Throwable t) {
        log.error("Caught throwable while processing data.");
        t.printStackTrace();
        exitCode = 1;
    }
    System.exit(exitCode);

	}

	@Bean
	public AWSCredentialsProvider awsCredentialsProvider() {
		AWSCredentialsProvider provider = new ProfileCredentialsProvider();
    try {
        provider.getCredentials();
    } catch (Exception e) {
        throw e;
    }
		return provider;
	}

}
