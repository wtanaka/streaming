package com.wtanaka.streaming.beam;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.wtanaka.streaming.beam.io.IteratorUnboundedSource;
import com.wtanaka.streaming.beam.util.SerializableIterator;


public class GenerateFakePageViews
{
   private static final long WINDOW_SIZE = 10;
   // Default window duration in seconds
   private static final long SLIDE_SIZE = 5;
   private static final String KAFKA_OUTPUT_TOPIC = "pageviews";
   private static final String KAFKA_BROKER = "localhost:9092";
   // Default kafka broker to contact
   private static final String GROUP_ID = "myGroup";  // Default groupId
   private static final String ZOOKEEPER = "localhost:2181";
   // Default zookeeper to connect to for Kafka

   /**
    * Options supported by {@link GenerateFakePageViews}.
    * <p>
    * <p>Concept #4: Defining your own configuration options. Here, you can
    * add your own arguments to be processed by the command-line parser, and
    * specify default values for them. You can then access the options values
    * in your pipeline code.
    * <p>
    * <p>Inherits standard configuration options.
    */
   public interface MyOptions extends PipelineOptions,
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

   private static class InfiniteIterator implements
      SerializableIterator<String>, Serializable
   {
      private static final long serialVersionUID = -4437364306598422200L;

      @Override
      public boolean hasNext()
      {
         return true;
      }

      @Override
      public String next()
      {
         return String.valueOf(System.currentTimeMillis());
      }

      @Override
      public void remove()
      {
      }
   }

   public static void main(String[] args)
   {
      MyOptions options = PipelineOptionsFactory.fromArgs(
         args).withValidation().as(MyOptions.class);

      options.setJobName(
         "Generate Synthetic Pageviews");
      options.setStreaming(true);
      options.setCheckpointingInterval(1000L);
      options.setNumberOfExecutionRetries(5);
      options.setExecutionRetryDelay(3000L);
      options.setRunner(FlinkRunner.class);

      System.out.println(
         options.getOutputKafkaTopic() + " " + options.getZookeeper() + " "
            + options.getBroker() + " " + options.getGroup());
      Pipeline pipeline = Pipeline.create(options);

      Properties kafkaConfig = new Properties();
      kafkaConfig.setProperty("zookeeper.connect", options.getZookeeper());
      kafkaConfig.setProperty("bootstrap.servers", options.getBroker());
      kafkaConfig.setProperty("group.id", options.getGroup());

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
/*
         FlinkKafkaProducer08<String> kafkaProducer = new
            FlinkKafkaProducer08<String>(options.getOutputKafkaTopic(), new
            SimpleStringSchema(), kafkaConfig);
*/

         // this is the Flink consumer that reads the input to
         // the program from a kafka topic.
         final Read.Unbounded<String> fakeIn = Read.from(
            new IteratorUnboundedSource<String>(new InfiniteIterator(),
               String.class));

         final PTransform<PCollection<String>, PDone> kafkaSink =
            KafkaIO.<String, String>write()
               .withBootstrapServers(options.getBroker())
               .withTopic(options.getOutputKafkaTopic())
               .withValueCoder(StringUtf8Coder.of())
               .values();

         PCollection<String> words = pipeline
            // Read lines from Kafka topic
            .apply("RandomGenerator", fakeIn);
         words.apply(kafkaSink);

         pipeline.run();
      }
      catch (RuntimeException e)
      {
         throw new RuntimeException("Runtime Exception on " + hostname, e);
      }
   }

}
