package com.ca.field.testelasticsearch;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class UseCase1_Connect
    implements Callable<Object>
{
    private static HashMap<String,String> hosts = new HashMap<String, String>();
    private static TransportClient esclient ;
    
    @Override
    public Object call() throws Exception
    {
        hosts.put("155.35.89.34","9300");
        hosts.put("155.35.89.38","9300");
        hosts.put("155.35.89.39","9300");
        
        System.out.println("["+this.getClass().getSimpleName()+"] "+" started");
        TransportClient MyESClient = getClient();
        if(MyESClient!=null)   {
            System.out.println("["+this.getClass().getSimpleName()+"] "+" got a elasticsearch client");
        } else {
            System.out.println("["+this.getClass().getSimpleName()+"] "+" failed to get a elasticsearch client!");
        }
        
        SearchResponse response = MyESClient.prepareSearch("index1", "index2")
                .setTypes("type1", "type2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        SearchHits hits = response.getHits();
        long TotalHits = hits.getTotalHits();
        System.out.println("["+this.getClass().getSimpleName()+"] "+"Total Hits 1 is: "+TotalHits);
        int i=0;
        for(SearchHit hit : response.getHits().getHits()) {
            System.out.println("["+this.getClass().getSimpleName()+"] "+"Hit "+i +" is: "+hit.toString());
        }
        
        Thread.sleep(500);
        response = MyESClient.prepareSearch().execute().actionGet();
        hits = response.getHits();
        TotalHits = hits.getTotalHits();
        System.out.println("["+this.getClass().getSimpleName()+"] "+"Total Hits 2 is: "+TotalHits);
        
        System.out.println("["+this.getClass().getSimpleName()+"] "+" finished");
        return null;
    }

    public TransportClient getClient() {
        if(esclient != null) {
            System.out.println("["+this.getClass().getSimpleName()+"] "+ "Reusing ES client");
            return esclient;
        } else {
            System.out.println("["+this.getClass().getSimpleName()+"] "+Thread.currentThread().getName());
            System.out.println("["+this.getClass().getSimpleName()+"] "+ "Creating a new ES client");
            try {
                Settings settings = null;
                String cz = Settings.class.getName();
                System.out.println("[TestESClient] cz is"+cz);
                System.out.println("[TestESClient] prepare client settting");
                
                
                org.elasticsearch.common.settings.Settings settings2 = org.elasticsearch.common.settings.Settings
                        .settingsBuilder().put("cluster.name", "map_cluster").build();
                System.out.println("[TestESClient] settting 2 done");
                
                settings = Settings.settingsBuilder().put("cluster.name", "myClusterName").build();
                System.out.println("[TestESClient] add sniff");
                settings = settings.settingsBuilder().put("client.transport.sniff", true).build();
                System.out.println("[TestESClient] apply client setting");
                //TransportClient client = TransportClient.builder().settings(settings).build();
                TransportClient client = null;
                client = TransportClient.builder().build();
                System.out.println("[TestESClient] ok");
                client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("155.35.89.34"), 9300));
                for(Map.Entry<String, String> entry : hosts.entrySet()){
                    String host = entry.getKey();
                    int port = Integer.valueOf(entry.getValue());
                    System.out.println("[TestESClient] Adding "+host+":"+port+" to client");
                    client = client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                }
                
                while (client.connectedNodes().isEmpty()) {
                    try
                    {
                        System.out.println("[TestESClient] All ES Nodes are down, Sleeping!");
                      Thread.sleep(6000L);
                    }
                    catch (InterruptedException intEx)
                    {
                        System.out.println("[TestESClient] Thread sleep interrupted");
                    }
                  }
                
                return client;
            } catch (Exception e) {
                System.out.println("[TestESClient] Caught Exception!");
                e.printStackTrace();
            }
        }
        return null;
    }
}
