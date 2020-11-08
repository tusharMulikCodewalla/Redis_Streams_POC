package com.example.Redis_Streams_POC;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@SpringBootApplication
public class RedisStreamsPocApplication {
	
	public static final String Streams_Key = "weather_sensor:wind";

	public static void main(String[] args) {
		SpringApplication.run(RedisStreamsPocApplication.class, args);
		
		int nbOfMessageToSend = 1;
		
		if(args != null && args.length != 0) {
			nbOfMessageToSend = Integer.valueOf(nbOfMessageToSend);
		}
		
		//Connecting with Redis
		RedisClient redisClient = RedisClient.create("redis://localhost:6379");
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		
		/*Redis Stream message body is created here
		 * here, map is used as streams message are key/value pairs
		*/
		for(int i=0; i<nbOfMessageToSend; ++i) {
			Map<String, String> messageBody = new HashMap<>();
			messageBody.put("speed", "15");
			messageBody.put("direction", "270");
			messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
			messageBody.put("loop_info", String.valueOf(i));
			
			//xadd is used to add the stream using stream key and messagebody
			String messageId = syncCommands.xadd(Streams_Key, messageBody);
			
			//Id and message content will be printed
			System.out.println(String.format("\t Message %s : %s posted", messageId, messageBody));
		}
		
		System.out.println("\n");
		
		//closing the connection
		connection.close();
		redisClient.shutdown();
		
		
	}

}

























