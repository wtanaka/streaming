package com.wtanaka.beam.transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wtanaka.beam.test.Recorder;

public class ReifyTimestampAndWindowTest
{
   static final SimpleFunction<ValueInSingleWindow<KV<Integer,
         List<Integer>>>, KV<Integer, Long>> GET_PANE
         = new SimpleFunction<ValueInSingleWindow<KV<Integer,
         List<Integer>>>, KV<Integer, Long>>()
   {
      @Override
      public KV<Integer, Long> apply(
            ValueInSingleWindow<KV<Integer, List<Integer>>> input)
      {
         return KV.of(31337, input.getPane().getIndex());
      }
   };

   private static class KvIntLongCapture implements
         SerializableFunction<KV<Integer, Long>, Integer>
   {
      final Recorder<KV<Integer, Long>> m_recorder;

      private KvIntLongCapture(
            Recorder<KV<Integer, Long>> recorder)
      {
         m_recorder = recorder;
      }

      @Override
      public Integer apply(KV<Integer, Long> integerLongKV)
      {
         m_recorder.add(integerLongKV);
         return integerLongKV.getKey();
      }
   }

   static class ValueInSingleWindowCombineFn extends
         CombineFn<Integer,
               ArrayList<Integer>,
               List<Integer>>
   {
      @Override
      public ArrayList<Integer> addInput(ArrayList<Integer> accumulator,
            Integer input)
      {
         accumulator.add(input);
         return accumulator;
      }

      @Override
      public ArrayList<Integer> createAccumulator()
      {
         return new ArrayList<>();
      }

      @Override
      public List<Integer> extractOutput(
            ArrayList<Integer> accumulator)
      {
         return accumulator;
      }

      @Override
      public ArrayList<Integer> mergeAccumulators(
            Iterable<ArrayList<Integer>> accumulators)
      {
         ArrayList<Integer> merged = new ArrayList<>();
         for (ArrayList<Integer> accum : accumulators)
         {
            merged.addAll(accum);
         }
         return merged;
      }
   }

   private static class RecordKvs extends DoFn<KV<Integer, Long>,
         KV<Integer, Long>>
   {
      private final Recorder<KV<Integer, Long>> m_recorder;

      public RecordKvs(Recorder<KV<Integer, Long>> recorderPre)
      {
         m_recorder = recorderPre;
      }

      @ProcessElement
      public void process(ProcessContext c)
      {
         m_recorder.add(c.element());
         c.output(c.element());
      }
   }

   private static Logger LOG = LoggerFactory.getLogger
         (ReifyTimestampAndWindowTest.class);

   @Rule
   public transient TestPipeline m_pipeline = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);

   /**
    * Make a TestStream.Builder of the given values with the given event time.
    *
    * @param values the values to put in the builder
    * @param eventTime The event time to assign to all the elements
    * @return the builder
    */
   static <T> TestStream.Builder<T> makeBuilder(Coder<T> coder, List<T> values,
         long eventTime)
   {
      TestStream.Builder<T> builder = TestStream.create(coder);
      for (final T value : values)
      {
         final Instant instant = new Instant(eventTime);
         builder = builder.addElements(TimestampedValue.of(value, instant));
      }
      return builder;
   }

   /**
    * Make a list of integers
    *
    * @param elementCount the number of strings
    * @return a list with elementCount strings
    */
   static List<Integer> makeInts(int elementCount)
   {
      return IntStream.range(0, elementCount).boxed()
            .collect(Collectors.toList());
   }

   /**
    * Make a list of strings
    *
    * @param elementCount the number of strings
    * @return a list with elementCount strings
    */
   static List<String> makeStrings(int elementCount)
   {
      return IntStream.range(0, elementCount)
            .mapToObj(i -> ("v" + i))
            .collect(Collectors.toList());
   }

   @Test
   public void construct()
   {
      new ReifyTimestampAndWindow();
   }

   @Test
   public void globalWindowNoKeys()
   {
      PCollection<ValueInSingleWindow<String>> result = m_pipeline
            .apply(TestStream
                  .create(StringUtf8Coder.of())
                  .addElements(TimestampedValue.of("dei", new Instant(123L)))
                  .advanceWatermarkToInfinity())
            .apply(ReifyTimestampAndWindow.of());
      PAssert.that(result)
            .containsInAnyOrder(
                  ValueInSingleWindow.of("dei", new Instant(123L),
                        GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
      PCollection<ValueInSingleWindow<ValueInSingleWindow<String>>> doublereify
            = result.apply("Check that implicit values are preserved",
            ReifyTimestampAndWindow.of());
      PAssert.that(doublereify).containsInAnyOrder(ValueInSingleWindow.of
            (ValueInSingleWindow.of("dei", new Instant(123L),
                  GlobalWindow.INSTANCE, PaneInfo.NO_FIRING),
                  new Instant(123L),
                  GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
      m_pipeline.run();
   }

   @Test
   public void globalWindowTrigger()
   {
      final int elementCount = 100;
      final long eventTime = 123L;
      final int numGroups = 7;
      List<Integer> values = makeInts(elementCount);
      TestStream.Builder<Integer> builder = makeBuilder(VarIntCoder.of(),
            values, eventTime);
      Recorder<KV<Integer, Long>> recorderPre = new Recorder<>(
            "globalWindowTrigger.mixed");
      Recorder<KV<Integer, Long>> recorder = new Recorder<>(
            "globalWindowTrigger");
      m_pipeline
            .apply(builder.advanceWatermarkToInfinity())
            .apply(Window
                  .<Integer>configure()
                  .triggering(Repeatedly.forever
                        (AfterPane.elementCountAtLeast(1)))
                  .accumulatingFiredPanes())
            .apply(WithKeys.of(
                  (SerializableFunction<Integer, Integer>) i -> {
                     return (i.intValue() % numGroups);
                  }).withKeyType(TypeDescriptors.integers()))
            .apply(Combine.perKey(new ValueInSingleWindowCombineFn()))
            .apply(ReifyTimestampAndWindow.of())
            .apply(MapElements.via(GET_PANE))
            .apply(ParDo.of(new RecordKvs(recorderPre)))
            .apply(Monotonic.strictlyIncreasing(new Top.Largest(),
                  VarLongCoder.of(), new KvIntLongCapture(recorder),
                  VarIntCoder.of()));
      m_pipeline.run();
      List<Long> preMonotonicResult = recorderPre.get()
            .stream().map(kv -> kv.getValue()).collect(Collectors.toList());
      boolean isAtLeastOneOutOfOrder = false;
      for (int i = 1; i < preMonotonicResult.size(); ++i)
      {
         long diff = preMonotonicResult.get(i) - preMonotonicResult.get(i - 1);
         if (diff < 0)
         {
            isAtLeastOneOutOfOrder = true;
         }
      }

      List<Long> result = recorder.get().stream().map(kv -> kv
            .getValue()).collect(Collectors.toList());
      boolean isAllPositive = true;
      for (int i = 1; i < result.size(); ++i)
      {
         long diff = result.get(i) - result.get(i - 1);
         if (diff <= 0)
         {
            isAllPositive = false;
         }
      }

      if (!isAtLeastOneOutOfOrder)
      {
         LOG.error("Test did not actually check out of order triggers");
      }
      Assert.assertTrue(!isAtLeastOneOutOfOrder || (isAtLeastOneOutOfOrder &&
            isAllPositive));
   }

   @Test
   public void slidingWindowNoTrigger()
   {
      final int elementCount = 20;
      final long eventTime = 123L;
      List<String> values = makeStrings(elementCount);
      TestStream.Builder<String> builder = makeBuilder(StringUtf8Coder
            .of(), values, eventTime);
      PCollection<ValueInSingleWindow<String>> preCombineResult = m_pipeline
            .apply(builder.advanceWatermarkToInfinity())
            .apply(Window.into(SlidingWindows.of(Duration.millis(10L))
                  .every(Duration.millis(5L))))
            .apply(ReifyTimestampAndWindow.of());
      HashSet<ValueInSingleWindow<String>> expected = new HashSet<>();
      for (IntervalWindow window : Arrays.asList(
            new IntervalWindow(new Instant(120L), new Instant(130L)),
            new IntervalWindow(new Instant(115L), new Instant(125L))))
      {
         for (String value : values)
         {
            expected.add(
                  ValueInSingleWindow.of(value, new Instant(eventTime), window,
                        PaneInfo.NO_FIRING));
         }
      }
      PAssert.that(preCombineResult).containsInAnyOrder(expected);
      m_pipeline.run();
   }
}
