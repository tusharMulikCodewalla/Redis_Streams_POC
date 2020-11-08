package com.example.Redis_Streams_POC;

import java.util.List;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.Consumer;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;


public class RedisStreamsPOCConsumer {
	
	
	
	  public static final String stream_key = "weather_sensor:wind";
	  
	  public static void main(String[] args) {
	  
	  RedisClient redisClient = RedisClient.create("redis://localhost:6379");
	  StatefulRedisConnection<String, String> connection = redisClient.connect();
	  RedisCommands<String, String> syncCommands = connection.sync();
	  
	  try 
	  { 
		  // create a new group application_3 and consume message from stream_key from beginning of the stream
		  syncCommands.xgroupCreate(XReadArgs.StreamOffset.from(stream_key,"0-0"), "application_3");
	  
	  } catch (RedisBusyException redisBusyException) {
		  System.out.println(String.format("Group '%s' already exists","application_3")); 
	  }
		  // consumer_1 is an consumer associated with application_3 group
	  		// on line no. 38 lastConsumed will read the unread messages from stream
		  @SuppressWarnings("unchecked")
	List<StreamMessage<String, String>> messages = syncCommands.xreadgroup (Consumer.from("application_3",
	  "consumer_1"), XReadArgs.StreamOffset.lastConsumed(stream_key) );
	  
	  if(!messages.isEmpty()) { 
		  for(StreamMessage<String, String> message : messages) {
			  
			  // simply read message and print it
			  System.out.println(message); 
			  
			  // sends an acknowledgment that message has been read and remove it from pending list  
			  syncCommands.xack(stream_key,"application_3", message.getId()); } 
		  } 
		
	  }
	 

}


























