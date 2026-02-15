package com.dws.tradestore.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
public class TradeProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(TradeProcessorApplication.class, args);
	}
}
