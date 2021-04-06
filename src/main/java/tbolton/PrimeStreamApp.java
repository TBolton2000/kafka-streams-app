/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbolton;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.JsonPOJODeserializer;
import org.apache.kafka.streams.JsonPOJOSerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import org.javatuples.KeyValue;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.UUID;

import java.lang.Math;

import tbolton.PrimeData;

/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurence histogram, write the continuous updated histogram
 * into a topic "streams-wordcount-output" where each record is an updated count of a single word.
 */
public class PrimeStreamApp {

    public static void test(Topology topology, Properties props) {
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<PrimeData> primeDataSerializer = new JsonPOJOSerializer<>();
        final Deserializer<PrimeData> primeDataDeserializer = new JsonPOJODeserializer<>();

        serdeProps.put("JsonPOJOClass", PrimeData.class);
        primeDataSerializer.configure(serdeProps, false);
        primeDataDeserializer.configure(serdeProps, false);

        final Serde<PrimeData> primeDataSerde = Serdes.serdeFrom(primeDataSerializer, primeDataDeserializer);

        TestInputTopic<String, PrimeData> inputTopic = testDriver.createInputTopic("numbers-input", Serdes.String().serializer(), primeDataSerde.serializer());
        for (long num = 2; num < 100; num++)
        {
            PrimeData data = new PrimeData(UUID.randomUUID(), System.nanoTime(), num);
            inputTopic.pipeInput(data.getUuid().toString(), data);
        }
        for (long num = 10000000; num < 10000100; num++)
        {
            PrimeData data = new PrimeData(UUID.randomUUID(), System.nanoTime(), num);
            inputTopic.pipeInput(data.getUuid().toString(), data);
        }

        TestOutputTopic<String, PrimeData> primeOutputTopic = testDriver.createOutputTopic("prime-numbers-output", Serdes.String().deserializer(), primeDataSerde.deserializer());
        TestOutputTopic<String, PrimeData> compositeOutputTopic = testDriver.createOutputTopic("composite-numbers-output", Serdes.String().deserializer(), primeDataSerde.deserializer());
        while (!primeOutputTopic.isEmpty())
        {
            PrimeData data = primeOutputTopic.readKeyValue().value;
            System.out.println("Output: " + data.getNumber() + " is " + (data.getIsPrime() ? "prime" : "composite")
             + " computed in " + ((data.getTimestampProcessEnd() - data.getTimestampProcessStart())/1e6) + " ms" );
        }
        while (!compositeOutputTopic.isEmpty())
        {
            PrimeData data = compositeOutputTopic.readKeyValue().value;
            System.out.println("Output: " + data.getNumber() + " is " + (data.getIsPrime() ? "prime" : "composite")
             + " computed in " + ((data.getTimestampProcessEnd() - data.getTimestampProcessStart())/1e6) + " ms" );
        }

        testDriver.close();
    }

    public static void run(Topology topology, Properties props) {
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-prime-check");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<PrimeData> primeDataSerializer = new JsonPOJOSerializer<>();
        final Deserializer<PrimeData> primeDataDeserializer = new JsonPOJODeserializer<>();

        serdeProps.put("JsonPOJOClass", PrimeData.class);
        primeDataSerializer.configure(serdeProps, false);
        primeDataDeserializer.configure(serdeProps, false);

        final Serde<PrimeData> primeDataSerde = Serdes.serdeFrom(primeDataSerializer, primeDataDeserializer);
        
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, PrimeData> mappedNumbers = builder.<String, PrimeData>stream("numbers-input", Consumed.with(Serdes.String(), primeDataSerde))
            .mapValues(value -> 
            {
                PrimeData newValue = new PrimeData(value);
                newValue.setTimestampProcessStart(System.nanoTime());
                newValue.setIsPrime(true);

                for (long divisor = 2; divisor <= Math.sqrt(newValue.getNumber()); divisor++)
                    if (newValue.getNumber() % divisor == 0) { newValue.setIsPrime(false); break; }
                
                newValue.setTimestampProcessEnd(System.nanoTime());
                return newValue;
            });
        
        KStream<String, PrimeData>[] branches = mappedNumbers
            .branch(
                (key, value) -> value.getIsPrime(),
                (key, value) -> !value.getIsPrime()
            );
        
        branches[0]
            .to("prime-numbers-output", Produced.with(Serdes.String(), primeDataSerde));
        branches[1]
            .to("composite-numbers-output", Produced.with(Serdes.String(), primeDataSerde));

        final Topology topology = builder.build();

        run(topology, props);
       
    }
}
