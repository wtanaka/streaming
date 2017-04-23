/*
 * com.wtanaka.beam
 *
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com/>
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.wtanaka.beam.io;

import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Lists;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import com.wtanaka.beam.SerializableIterator;

/**
 * Test IteratorIO
 */
public class IteratorIOTest
{
   /**
    * Generate random strings
    */
   private static class RandomIterator implements
      SerializableIterator<TimestampedValue<String>>
   {
      private static final long serialVersionUID = -461002278148373L;
      private int m_count;

      RandomIterator(final int count)
      {
         m_count = count;
      }

      @Override
      public boolean hasNext()
      {
         return m_count > 0;
      }

      @Override
      public TimestampedValue<String> next()
      {
         if (m_count <= 0)
         {
            throw new NoSuchElementException();
         }
         m_count = (m_count > 0) ? m_count - 1 : m_count;
         Random random = new Random();
         return TimestampedValue.of(String.valueOf(random.nextInt()),
            Instant.now());
      }
   }

   @Test
   public void testRandomIteratorOne()
   {
      final RandomIterator one = new RandomIterator(1);
      Assert.assertEquals(1, Lists.newArrayList(one).size());
   }

   @Test
   public void testRandomIteratorRandom()
   {
      Random random = new Random(0L);
      for (int i = 0; i < 100; ++i)
      {
         final int count = random.nextInt(100);
         final RandomIterator randomLen = new RandomIterator(count);
         Assert.assertEquals(count, Lists.newArrayList(randomLen).size());
      }
   }

   @Test
   public void testRandomIteratorZero()
   {
      final RandomIterator zero = new RandomIterator(0);
      Assert.assertFalse(zero.hasNext());
   }

   @Test
   public void unboundRead() throws Exception
   {
      final TestPipeline pipeline =
         TestPipeline.create().enableAbandonedNodeEnforcement(true);

      Random random = new Random(0);
      final int count = random.nextInt(1000);
      final PCollection<String> strings = pipeline.apply(
         IteratorIO.unboundRead(new RandomIterator(count), StringUtf8Coder.of
            ()));

      PAssert.that(strings).satisfies(
         (SerializableFunction<Iterable<String>, Void>) strings1 ->
         {
            Assert.assertEquals(count, Lists.newArrayList(strings1).size());
            return null;
         });

      final PipelineResult pipelineState = pipeline.run();
      final PipelineResult.State state = pipelineState.waitUntilFinish();
      Assert.assertEquals(PipelineResult.State.DONE, state);
   }
}
