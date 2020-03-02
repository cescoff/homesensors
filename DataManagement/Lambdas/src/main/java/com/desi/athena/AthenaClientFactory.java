package com.desi.athena;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;

/**
 * AthenaClientFactory
 * -------------------------------------
 * This code shows how to create and configure an Amazon Athena client.
 */
public class AthenaClientFactory {

    /**
     * AmazonAthenaClientBuilder to build Athena with the following properties:
     * - Set the region of the client
     * - Use the instance profile from the EC2 instance as the credentials provider
     * - Configure the client to increase the execution timeout.
     */
    private final AmazonAthenaClientBuilder builder = AmazonAthenaClientBuilder.standard()
            .withRegion(Regions.EU_WEST_3)
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(100000));

    public AmazonAthena createClient() {
        return builder.build();
    }


}