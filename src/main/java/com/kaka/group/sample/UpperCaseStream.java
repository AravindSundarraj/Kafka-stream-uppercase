package com.kaka.group.sample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

@Slf4j
public class UpperCaseStream {

    public static void main(String ar[])throws Exception{

        log.info("Kafka Stream for upper case in progress...");

        // --  Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
       // StreamsConfig streamConfig  = new StreamsConfig(properties);
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> simpleFirstStream = builder.stream("upstream-message",
                Consumed.with(stringSerde, stringSerde));

        simpleFirstStream.foreach( (e ,f)->
                log.info("Print the message : " , e , f));

        simpleFirstStream.foreach( (e ,f)->
                System.out.println("Print the message : " + e + f));

        KStream<String , String> upperCaseStream =
                simpleFirstStream.mapValues( s -> s.toUpperCase());

        upperCaseStream.to("downstream-message" , Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),properties);
        log.info("Hello World Yelling App Started");
        kafkaStreams.start();
        Thread.sleep(35000);
        log.info("Shutting down the Yelling APP now");
        kafkaStreams.close();
    }
}
