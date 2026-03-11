package com.amazonaws.cloudwatch.log4j2;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;

@Plugin(name = "CloudWatchAppender", category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE, printObject = true)
public class CloudWatchAppender extends AbstractAppender {

    private final CloudWatchLogsClient client;
    private final String logGroupName;
    private final String logStreamName;
    private volatile boolean streamReady = false;

    protected CloudWatchAppender(String name, Filter filter,
                                 Layout<? extends Serializable> layout,
                                 boolean ignoreExceptions, Property[] properties,
                                 String logGroupName, String logStreamName,
                                 CloudWatchLogsClient client) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
        this.client = client;
        ensureStream();
    }

    private void ensureStream() {
        try {
            client.createLogStream(r -> r
                    .logGroupName(logGroupName)
                    .logStreamName(logStreamName));
            streamReady = true;
        } catch (ResourceAlreadyExistsException e) {
            streamReady = true;
        } catch (Exception ignored) {
            // Will retry on next log event
        }
    }

    @Override
    public void append(LogEvent event) {
        if (!streamReady) ensureStream();
        if (!streamReady) return;
        try {
            String message = new String(getLayout().toByteArray(event));
            client.putLogEvents(r -> r
                    .logGroupName(logGroupName)
                    .logStreamName(logStreamName)
                    .logEvents(Collections.singletonList(
                            InputLogEvent.builder()
                                    .timestamp(event.getTimeMillis())
                                    .message(message.trim())
                                    .build()
                    )));
        } catch (Exception ignored) {
            // Never break the application due to a logging failure
        }
    }

    @PluginFactory
    public static CloudWatchAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginAttribute("logGroupName") String logGroupName,
            @PluginAttribute("logStreamName") String logStreamName,
            @PluginAttribute("awsAccessKeyId") String awsAccessKeyId,
            @PluginAttribute("awsSecretKey") String awsSecretKey,
            @PluginAttribute("awsRegion") String awsRegion,
            @PluginAttribute("cloudWatchEndpoint") String cloudWatchEndpoint) {

        if (layout == null) layout = PatternLayout.createDefaultLayout();

        // If Log4j2 didn't resolve the placeholders, fall back to System env
        if (awsRegion == null || awsRegion.contains("${")) {
            awsRegion = System.getenv("AWS_REGION");
        }
        if (awsAccessKeyId == null || awsAccessKeyId.contains("${")) {
            awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        }
        if (awsSecretKey == null || awsSecretKey.contains("${")) {
            awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        if (cloudWatchEndpoint == null || cloudWatchEndpoint.contains("${")) {
            cloudWatchEndpoint = System.getenv("CLOUDWATCH_ENDPOINT");
        }

        String region = (awsRegion != null && !awsRegion.isBlank()) ? awsRegion : "us-east-1";
        String accessKey = (awsAccessKeyId != null && !awsAccessKeyId.isBlank()) ? awsAccessKeyId : "test";
        String secretKey = (awsSecretKey != null && !awsSecretKey.isBlank()) ? awsSecretKey : "test";

        StaticCredentialsProvider creds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey));

        CloudWatchLogsClient client;
        if (cloudWatchEndpoint != null && !cloudWatchEndpoint.isBlank()) {
            client = CloudWatchLogsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(creds)
                    .endpointOverride(URI.create(cloudWatchEndpoint))
                    .build();
        } else {
            client = CloudWatchLogsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(creds)
                    .build();
        }

        return new CloudWatchAppender(name, filter, layout, true, null,
                logGroupName, logStreamName, client);
    }
}
