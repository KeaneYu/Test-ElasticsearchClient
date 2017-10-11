package com.ca.field.testelasticsearch;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import java.util.concurrent.Future;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class TestESClient
{
    private static HashMap<String,String> hosts = new HashMap<String, String>();
    private static TransportClient esclient ;

    public static void main(String[] args)
    {
        hosts.put("155.35.89.34","9300");
        hosts.put("155.35.89.38","9300");
        hosts.put("155.35.89.39","9300");
        
        UseCase1_Connect uc1 = new UseCase1_Connect();
        
        ExecutorService es = Executors.newFixedThreadPool(3);   
        
        System.out.println("[TestESClient] Start the main loop");
        
        boolean runForever = false;
        
        do {
            es.submit(uc1);  
            
            try
            {
                System.out.println("[TestESClient] Mainloop is sleeping");
                Thread.sleep(7000);
            } catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } while (runForever);
    }

    /*
    public static TransportClient getClient() {
        if(esclient != null) {
            System.out.println("[TestESClient] Reusing ES client");
            return esclient;
        } else {
            System.out.println("[TestESClient] Current thread is: "+Thread.currentThread().getName());
            System.out.println("[TestESClient] Creating a new ES client");
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
    }*/
}
