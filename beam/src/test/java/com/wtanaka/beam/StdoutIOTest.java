package com.wtanaka.beam;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Test;

import com.wtanaka.beam.transforms.StringToByteArray;

/**
 * Test code for StdoutSink
 */
public class StdoutIOTest
{
   @Test
   public void testConstruct()
   {
      new StdoutIO();
   }

   @Test
   public void testSmoke()
   {
      final TestPipeline pipeline =
         TestPipeline.create().enableAbandonedNodeEnforcement(true);
      pipeline
         .apply(GenerateSequence.from(0).to(1))
         .apply(new StringValueOf<>())
         .apply(StringToByteArray.of("UTF-8"))
         .apply(StdoutIO.write())
      ;
      pipeline.run();
   }

   @Test
   public void testWrite()
   {
      Assert.assertNotNull(StdoutIO.write());
   }
}
