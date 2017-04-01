/*
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com>
 */
package com.wtanaka.beam;

import java.io.ByteArrayInputStream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author $Author$
 * @version $Name$ $Date$
 **/
@RunWith(JUnit4.class)
public class StreamIOTest
{
   @Test
   public void testSmoke() throws Exception
   {
      Pipeline pipeline = TestPipeline.create();
      final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[]
         {0x68, 0x0a, 0x65, 0x0a, 0x6c, 0x0a, 0x6c, 0x0a, 0x6f, 0x0a});
      PCollection<byte[]> bytes = pipeline.apply(
         new StreamIO.Read.Bound(bais));
      PAssert.that(bytes).containsInAnyOrder(new byte[]{0x68, 0x0a},
         new byte[]
            {0x65, 0x0a}, new byte[]{0x6c, 0x0a}, new byte[]{0x6c, 0x0a},
         new byte[]
            {0x6f, 0x0a});
   }
}
