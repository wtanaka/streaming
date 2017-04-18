package com.wtanaka.beam;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.logging.Level;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.wtanaka.beam.transforms.ByteArrayToString;
import com.wtanaka.beam.transforms.StringToByteArray;

/**
 * Test Sort
 */
@RunWith(JUnit4.class)
public class SortTest
{
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void construct()
   {
      new Sort();
   }

   /**
    * Exercise main to make sure it doesn't crash
    */
   @Test
   public void main()
   {
      final InputStream oldIn = System.in;
      final PrintStream oldOut = System.out;
      try
      {
         final ByteArrayInputStream bais = new ByteArrayInputStream(
            new byte[]{});
         System.setIn(bais);
         final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final PrintStream newOut = new PrintStream(baos);
         System.setOut(newOut);
         Sort.main(new String[]{});
         Assert.assertArrayEquals(new byte[]{}, baos.toByteArray());
      }
      finally
      {
         System.setIn(oldIn);
         System.setOut(oldOut);
      }
   }

   @Test
   public void expand() throws Exception
   {
      m_pipeline
         // String
         .apply(Create.of("C", "A", "B"))
         // byte[]
         .apply(StringToByteArray.of("UTF-8"))
         // byte[]
         .apply(new Sort.Transform())
         // String
         .apply(ByteArrayToString.of("UTF-8"))
         // PDone
         .apply(LoggingIO.write("SortTest", Level.WARNING));
      m_pipeline.run();
   }
}
