package com.wtanaka.beam;

import java.util.logging.Level;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test Sort
 */
public class SortTest
{
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void expand() throws Exception
   {
      final Create.Values<String> threeLetters = Create.of("C", "A", "B");
      final PCollection<String> source = m_pipeline.apply(threeLetters);
      final PCollection<String> sorted = source.apply(new Sort());
      sorted.apply(LoggingIO.write("SortTest", Level.WARNING));
      m_pipeline.run();
   }

}
