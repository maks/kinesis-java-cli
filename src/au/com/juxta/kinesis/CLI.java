package au.com.juxta.kinesis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class CLI {

    private static AmazonKinesisClient client;

    public static void main(String[] args) {

        client = new AmazonKinesisClient();
        client.setEndpoint("kinesis.ap-southeast-2.amazonaws.com", "kinesis", "ap-southeast-2");

        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(20);
        ListStreamsResult listStreamsResult = client
                .listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();

        if ((args.length < 1)
                || ((args.length != 0) && "-h".equals(args[0].trim()))) {
            showHelp(args[0].trim());
            return;
        }
        if (args[0].equalsIgnoreCase("-l")) {
            System.out.println("streams:");
            for (Iterator<String> iterator = streamNames.iterator(); iterator.hasNext();) {
                String streamName = iterator.next();
                System.out.println(streamName);
            }
        }
        if (args[0].equalsIgnoreCase("-s")) {
            //list shards
            List<Shard> shards = getShards(args[1]);
            System.out.println("shards:");
            for (Iterator<Shard> iterator = shards.iterator(); iterator.hasNext();) {
                Shard s = iterator.next();
                System.out.println(s);
            }
        }
        if (args[0].equalsIgnoreCase("-g")) {
            //get records
            List<Record> records = getRecords(args[1], getShards(args[1]).get(0));
            System.out.println("records:");
            for (Iterator<Record> iterator = records.iterator(); iterator.hasNext();) {
                Record r = iterator.next();
                System.out.println(r.getSequenceNumber()+":"+new String(r.getData().array()));
            }
        }
    }

    private static List<Shard> getShards(String streamname) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName( streamname );
        List<Shard> shards = new ArrayList<Shard>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
            DescribeStreamResult describeStreamResult = client.describeStream(describeStreamRequest);
            shards.addAll( describeStreamResult.getStreamDescription().getShards() );
            if (describeStreamResult.getStreamDescription().getHasMoreShards() &&
                    shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while ( exclusiveStartShardId != null );
        return shards;
    }

    private static List<Record> getRecords(String streamname, Shard shard) {
        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(streamname);
        getShardIteratorRequest.setShardId(shard.getShardId());
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(1000);
        GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
        return getRecordsResult.getRecords();
    }
    
    
    private static void showHelp(String badarg) {
        System.out.println("not valid arg:" + badarg);
        System.out.println("args:");
        System.out.println("-h  help");
        System.out.println("-l  list streams");
    }
    
}
