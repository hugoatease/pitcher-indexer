import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PitcherIndexer {
    public static void main(String[] args) throws Exception {
        String propsFile = System.getProperty("pitcher.props", "default.properties");
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> configurationBuilder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties().setFileName(propsFile));

        Configuration config = configurationBuilder.getConfiguration();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("kafka.application_id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap_servers"));
        props.put("schema.registry.url", config.getString("avro.registry_url"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        if (config.getBoolean("kafka.ssl_enable")) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.getString("kafka.ssl_truststore_location"));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.getString("kafka.ssl_truststore_password"));
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getString("kafka.ssl_keystore_location"));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getString("kafka.ssl_keystore_password"));
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, config.getString("kafka.ssl_key_password"));

            if (config.getBoolean("kafka.ssl_disable_host_check")) {
                props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
        }

        SchemaRegistryClient registry = new CachedSchemaRegistryClient(config.getString("avro.registry_url"), 10);
        Schema recordingKeySchemaRef = registry.getByVersion("musicbrainz.musicbrainz.recording-key", 1, false);
        org.apache.avro.Schema recordingKeySchema = ((AvroSchema) registry.getSchemaById(recordingKeySchemaRef.getId())).rawSchema();

        final StreamsBuilder builder = new StreamsBuilder();

        KTable<GenericRecord, GenericRecord> tracks = builder.table("musicbrainz.musicbrainz.track", Materialized.as("track"));
        KTable<GenericRecord, GenericRecord> recordings = builder.table("musicbrainz.musicbrainz.recording", Materialized.as("recording"));

        KTable<GenericRecord, String> result = tracks.join(recordings, (leftValue) -> {
            int recording = (int) ((GenericRecord) leftValue.get("after")).get("recording");
            GenericRecordBuilder keyBuilder = new GenericRecordBuilder(recordingKeySchema);
            return keyBuilder.set("id", recording).build();
        }, (leftValue, rightValue) -> {
            String trackName = ((Utf8) ((GenericRecord) leftValue.get("after")).get("name")).toString();
            String recordingName = ((Utf8) ((GenericRecord) rightValue.get("after")).get("name")).toString();
            return recordingName + " - " + trackName;
        });

        final Serde<String> stringSerde = Serdes.String();

        result.toStream().to("results", Produced.valueSerde(stringSerde));


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        TopologyDescription description = topology.describe();
        System.out.println(description);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.cleanUp();
        streams.start();
        latch.await();
        System.exit(0);
    }
}