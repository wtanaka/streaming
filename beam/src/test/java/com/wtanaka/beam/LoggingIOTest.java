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
package com.wtanaka.beam;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.JDK14LoggerAdapter;

/**
 * Test LoggingIO
 */
@RunWith(JUnit4.class)
public class LoggingIOTest
{
   @Rule
   public final transient TestPipeline m_pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(true);

   private Random m_random = new Random();

   class CapturingHandler extends Handler
   {
      private ArrayList<LogRecord> m_records = new ArrayList<>();

      @Override
      public void close() throws SecurityException
      {
      }

      @Override
      public void flush()
      {
      }

      List<LogRecord> getRecords()
      {
         return m_records;
      }

      @Override
      public void publish(LogRecord record)
      {
         m_records.add(record);
      }
   }

   @Test
   public void testConstruct()
   {
      new LoggingIO();
   }

   @Test
   public void testDebug()
   {
      final String logName = LoggingIOTest.class.getName() +
         m_random.nextLong();
      final CapturingHandler handler = new CapturingHandler();
      setupLogger(logName, handler);

      final PCollection<String> result = m_pipeline
         .apply(TestStream.create(VarIntCoder.of())
                          .addElements(
                             TimestampedValue.of(123, new Instant(123L)),
                             TimestampedValue.of(234, new Instant(234L)))
                          .advanceWatermarkToInfinity())
         .apply(new StringValueOf<>())
         .apply("stderr factory", LoggingIO.debug(LoggingIOTest.class))
         .apply("capturing factory", LoggingIO.debug(logName));
      PAssert.that(result).containsInAnyOrder("123", "234");
      m_pipeline.run();

      ArrayList<LogRecord> list = new ArrayList<>(handler.getRecords());
      list.sort(Comparator.comparing(LogRecord::getMessage));

      Assert.assertEquals(2, list.size());
      Assert.assertEquals(Level.FINE, list.get(0).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.123Z> 123",
         list.get(0).getMessage());
      Assert.assertEquals(Level.FINE, list.get(1).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.234Z> 234",
         list.get(1).getMessage());
   }

   private static void setupLogger(final String loggerName,
                                   final CapturingHandler handler)
   {
      handler.setLevel(Level.ALL);
      org.slf4j.Logger logger = LoggerFactory.getLogger(loggerName);
      final Field field;
      try
      {
         field = JDK14LoggerAdapter.class.getDeclaredField("logger");
         field.setAccessible(true);
         final Logger value = (Logger) field.get(logger);
         value.addHandler(handler);
         value.setUseParentHandlers(false);
         value.setLevel(Level.ALL);
      }
      catch (NoSuchFieldException | IllegalAccessException e)
      {
         e.printStackTrace();
      }
   }

   @Test
   public void testError()
   {
      final String logName = LoggingIOTest.class.getName() +
         m_random.nextLong();
      final CapturingHandler handler = new CapturingHandler();
      setupLogger(logName, handler);

      final PCollection<String> result = m_pipeline
         .apply(TestStream.create(VarIntCoder.of())
                          .addElements(
                             TimestampedValue.of(123, new Instant(123L)),
                             TimestampedValue.of(234, new Instant(234L)))
                          .advanceWatermarkToInfinity())
         .apply(new StringValueOf<>())
         .apply("stderr factory", LoggingIO.error(LoggingIOTest.class))
         .apply("capturing factory", LoggingIO.error(logName));
      PAssert.that(result).containsInAnyOrder("123", "234");
      m_pipeline.run();

      ArrayList<LogRecord> list = new ArrayList<>(handler.getRecords());
      list.sort(Comparator.comparing(LogRecord::getMessage));

      Assert.assertEquals(2, list.size());
      Assert.assertEquals(Level.SEVERE, list.get(0).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.123Z> 123",
         list.get(0).getMessage());
      Assert.assertEquals(Level.SEVERE, list.get(1).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.234Z> 234",
         list.get(1).getMessage());
   }

   @Test
   public void testInfo()
   {
      final String logName = LoggingIOTest.class.getName() +
         m_random.nextLong();
      final CapturingHandler handler = new CapturingHandler();
      setupLogger(logName, handler);

      final PCollection<String> result = m_pipeline
         .apply(TestStream.create(VarIntCoder.of())
                          .addElements(
                             TimestampedValue.of(123, new Instant(123L)),
                             TimestampedValue.of(234, new Instant(234L)))
                          .advanceWatermarkToInfinity())
         .apply(new StringValueOf<>())
         .apply("stderr factory", LoggingIO.info(LoggingIOTest.class))
         .apply("capturing factory", LoggingIO.info(logName));
      PAssert.that(result).containsInAnyOrder("123", "234");
      m_pipeline.run();

      ArrayList<LogRecord> list = new ArrayList<>(handler.getRecords());
      list.sort(Comparator.comparing(LogRecord::getMessage));

      Assert.assertEquals(2, list.size());
      Assert.assertEquals(Level.INFO, list.get(0).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.123Z> 123",
         list.get(0).getMessage());
      Assert.assertEquals(Level.INFO, list.get(1).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.234Z> 234",
         list.get(1).getMessage());
   }

   @Test
   public void testPassThrough()
   {
      final PCollection<Long> longs = m_pipeline.apply(
         GenerateSequence.from(0L).to(5L));
      final PCollection<Long> passed = longs.apply(
         LoggingIO.trace(LoggingIOTest.class));
      PAssert.that(passed).containsInAnyOrder(0L, 1L, 2L, 3L, 4L);
      m_pipeline.run();
   }

   @Test
   public void testTrace()
   {
      final String logName = LoggingIOTest.class.getName() +
         m_random.nextLong();
      final CapturingHandler handler = new CapturingHandler();
      setupLogger(logName, handler);

      final PCollection<String> result = m_pipeline
         .apply(TestStream.create(VarIntCoder.of())
                          .addElements(
                             TimestampedValue.of(123, new Instant(123L)),
                             TimestampedValue.of(234, new Instant(234L)))
                          .advanceWatermarkToInfinity())
         .apply(new StringValueOf<>())
         .apply(LoggingIO.trace(logName));
      PAssert.that(result).containsInAnyOrder("123", "234");
      m_pipeline.run();

      ArrayList<LogRecord> list = new ArrayList<>(handler.getRecords());
      list.sort(Comparator.comparing(LogRecord::getMessage));

      Assert.assertEquals(2, list.size());
      Assert.assertEquals(Level.FINEST, list.get(0).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.123Z> 123",
         list.get(0).getMessage());
      Assert.assertEquals(Level.FINEST, list.get(1).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.234Z> 234",
         list.get(1).getMessage());
   }

   @Test
   public void testWarn()
   {
      final String logName = LoggingIOTest.class.getName() +
         m_random.nextLong();
      final CapturingHandler handler = new CapturingHandler();
      setupLogger(logName, handler);

      final PCollection<String> result = m_pipeline
         .apply(TestStream.create(VarIntCoder.of())
                          .addElements(
                             TimestampedValue.of(123, new Instant(123L)),
                             TimestampedValue.of(234, new Instant(234L)))
                          .advanceWatermarkToInfinity())
         .apply(new StringValueOf<>())
         .apply("stderr factory", LoggingIO.warn(LoggingIOTest.class))
         .apply("capturing factory", LoggingIO.warn(logName));
      PAssert.that(result).containsInAnyOrder("123", "234");
      m_pipeline.run();

      ArrayList<LogRecord> list = new ArrayList<>(handler.getRecords());
      list.sort(Comparator.comparing(LogRecord::getMessage));

      Assert.assertEquals(2, list.size());
      Assert.assertEquals(Level.WARNING, list.get(0).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.123Z> 123",
         list.get(0).getMessage());
      Assert.assertEquals(Level.WARNING, list.get(1).getLevel());
      Assert.assertEquals("<1970-01-01T00:00:00.234Z> 234",
         list.get(1).getMessage());
   }
}
