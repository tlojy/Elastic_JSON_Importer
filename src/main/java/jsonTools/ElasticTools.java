package jsonTools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;



public class ElasticTools {
	
	public static final String HOST = "192.168.137.72";
	public static final int PORT = 9300;
	
	public static void main(String[] args) throws Exception {
		TransportClient client = getClient(HOST, PORT);
		GetResponse response = client.prepareGet("data", "sgk", "88232506").get();
		Map<String, Object> map = response.getSource();
		closeClient(client);
	}
	
	/**
	 * 生成一个新的es连接客户端
	 * @return
	 * @throws Exception
	 */
	public static TransportClient getClient(String host, int port) throws Exception{
		
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		return client;
	}
	
	/**
	 * 关闭Client
	 * @param client
	 */
	public static void closeClient(TransportClient client){
		if(client!=null){
			client.close();
		}
	}

	public static BulkProcessor getBulkProcessor(TransportClient client) throws Exception{
		BulkProcessor bulkProcessor = BulkProcessor.builder(
				client,  
		        new BulkProcessor.Listener() {
		           
					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
						// TODO Auto-generated method stub
						//client.close();
						System.out.println("afterBulk:"+response.getTook());
					}

					@Override
					public void afterBulk(long executionId,	BulkRequest request, Throwable failure) {
						// TODO Auto-generated method stub
						System.out.println("afterBulk-failure");
						
					} 
		        })
		        .setBulkActions(10000) 
		        .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB)) 
		        .setFlushInterval(TimeValue.timeValueSeconds(5)) 
		        .setConcurrentRequests(0) 
		        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) 
		        .build();
		return bulkProcessor;
		
	}
}
