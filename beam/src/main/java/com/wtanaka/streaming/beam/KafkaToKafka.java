package com.wtanaka.streaming.beam;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io
   .UnboundedFlinkSink;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io
   .UnboundedFlinkSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.joda.time.Duration;

public class KafkaToKafka
{
   private static final long WINDOW_SIZE = 10;
   // Default window duration in seconds
   private static final long SLIDE_SIZE = 5;
   // Default window slide in seconds
   private static final String KAFKA_TOPIC = "input-topic";
   private static final String KAFKA_OUTPUT_TOPIC = "output-topic";
   private static final String KAFKA_BROKER = "localhost:9092";
   // Default kafka broker to contact
   private static final String GROUP_ID = "myGroup";  // Default groupId
   private static final String ZOOKEEPER = "localhost:2181";
   // Default zookeeper to connect to for Kafka


   /**
    * Function to extract words.
    */
   private static class ExtractWordsFn extends DoFn<String, String>
   {
      private final Aggregator<Long, Long> emptyLines =
         createAggregator("emptyLines", new Sum.SumLongFn());

      @ProcessElement
      public void processElement(ProcessContext c)
      {
         if (c.element().trim().isEmpty())
         {
            emptyLines.addValue(1L);
         }

         // Split the line into words.
         String[] words = c.element().split("[^a-zA-Z']+");

         // Output each word encountered into the output PCollection.
         for (String word : words)
         {
            if (!word.isEmpty())
            {
               c.output(word);
            }
         }
      }
   }

   /**
    * Function to format KV as String.
    */
   public static class FormatAsStringFn extends DoFn<KV<String, Long>, String>
   {
      @ProcessElement
      public void processElement(ProcessContext c)
      {
         String row =
            c.element().getKey() + " - " + c.element().getValue() + " @ "
               + c.timestamp().toString();
         System.out.println(row);
         c.output(row);
      }
   }


   /**
    * Options supported by {@link KafkaToKafka}.
    * <p>
    * <p>Concept #4: Defining your own configuration options. Here, you can
    * add your own arguments to be processed by the command-line parser, and
    * specify default values for them. You can then access the options values
    * in your pipeline code.
    * <p>
    * <p>Inherits standard configuration options.
    */
   public interface KafkaToKafkaOptions extends PipelineOptions,
      FlinkPipelineOptions
   {
      @Description("Sliding window duration, in seconds")
      @Default.Long(WINDOW_SIZE)
      Long getWindowSize();

      void setWindowSize(Long value);

      @Description("Window slide, in seconds")
      @Default.Long(SLIDE_SIZE)
      Long getSlide();

      void setSlide(Long value);

      @Description("The Kafka topic to read from")
      @Default.String(KAFKA_TOPIC)
      String getKafkaTopic();

      void setKafkaTopic(String value);

      @Description("The Kafka topic to write to")
      @Default.String(KAFKA_OUTPUT_TOPIC)
      String getOutputKafkaTopic();

      void setOutputKafkaTopic(String value);

      @Description("The Kafka Broker to read from")
      @Default.String(KAFKA_BROKER)
      String getBroker();

      void setBroker(String value);

      @Description("The Zookeeper server to connect to")
      @Default.String(ZOOKEEPER)
      String getZookeeper();

      void setZookeeper(String value);

      @Description("The groupId")
      @Default.String(GROUP_ID)
      String getGroup();

      void setGroup(String value);
   }

   public static void main(String[] args)
   {
      KafkaToKafkaOptions options = PipelineOptionsFactory.fromArgs(
         args).withValidation().as(KafkaToKafkaOptions.class);

      options.setJobName(
         "KafkaExample - WindowSize: " + options.getWindowSize() +
            " seconds");
      options.setStreaming(true);
      options.setCheckpointingInterval(1000L);
      options.setNumberOfExecutionRetries(5);
      options.setExecutionRetryDelay(3000L);
      options.setRunner(FlinkRunner.class);

      System.out.println(
         options.getKafkaTopic() + " " + options.getZookeeper() + " "
            + options.getBroker() + " " + options.getGroup());
      Pipeline pipeline = Pipeline.create(options);

      Properties p = new Properties();
      p.setProperty("zookeeper.connect", options.getZookeeper());
      p.setProperty("bootstrap.servers", options.getBroker());
      p.setProperty("group.id", options.getGroup());

      String hostname = "UNKNOWN";
      try
      {
         hostname = InetAddress.getLocalHost().getHostName();
      }
      catch (UnknownHostException e)
      {
         hostname = e.toString();
      }

      try
      {
         // this is the Flink consumer that reads the input to
         // the program from a kafka topic.
         FlinkKafkaConsumer08<String> kafkaConsumer =
            new FlinkKafkaConsumer08<>(
               options.getKafkaTopic(),
               new SimpleStringSchema(), p);

         FlinkKafkaProducer08<String> kafkaProducer = new
            FlinkKafkaProducer08<String>(options.getOutputKafkaTopic(), new
            SimpleStringSchema(), p);

         PCollection<String> words = pipeline
            .apply("StreamingWordCount",
               Read.from(UnboundedFlinkSource.of(kafkaConsumer)))
            .apply(ParDo.of(new ExtractWordsFn()))
            .apply(Window.<String>into(FixedWindows.of(
               Duration.standardSeconds(options.getWindowSize())))
               .triggering(AfterWatermark.pastEndOfWindow())
               .withAllowedLateness(Duration.ZERO)
               .discardingFiredPanes());

         PCollection<KV<String, Long>> wordCounts =
            words.apply(Count.<String>perElement());

         final PTransform<PCollection<String>, PDone> kafkaSink =
            KafkaIO.write()
               .withBootstrapServers(options.getBroker())
               .withTopic(options.getOutputKafkaTopic())
               .withValueCoder(StringUtf8Coder.of())
               .values();

         wordCounts.apply(ParDo.of(new FormatAsStringFn())).apply(kafkaSink);

         pipeline.run();
      }
      catch (RuntimeException e)
      {
         throw new RuntimeException("Runtime Exception on " + hostname, e);
      }
   }
}
