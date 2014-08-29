package au.com.juxta.kinesis;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

public class Fetcher {

    private static AmazonKinesisClient client;

    public static void main(String[] args) {
        
        client = new AmazonKinesisClient();
        client.setEndpoint("kinesis.ap-southeast-2.amazonaws.com", "kinesis", "ap-southeast-2");
        
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(20);
        ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();

        System.out.println("streams:");
        for (Iterator iterator = streamNames.iterator(); iterator.hasNext();) {
            String streamName = (String) iterator.next();
            System.out.println(streamName);
        }
        
    }

}
