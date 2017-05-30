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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.wtanaka.beam.StreamIO.BoundSource.StdinBoundedReader;
import com.wtanaka.beam.transforms.ByteArrayToString;

/**
 * @author $Author$
 * @version $Name$ $Date$
 **/
@RunWith(JUnit4.class)
public class StreamIOTest
{
   private final PipelineOptions m_options = PipelineOptionsFactory.create();
   private StreamIO.BoundSource m_boundSource;
   private StreamIO.BoundSource m_boundSourceByteSource;
   private StreamIO.UnboundSource m_unbounded;
   private StreamIO.UnboundSource.UnboundReader m_reader;

   public static class IntegerParseInt extends SimpleFunction<String, Integer>
      implements Serializable
   {
      @Override
      public Integer apply(final String input)
      {
         return Integer.parseInt(input);
      }
   }

   private static TestPipeline newPipeline()
   {
      return TestPipeline.create().enableAbandonedNodeEnforcement(
         true);
   }

   @Test
   public void advance() throws Exception
   {
      Assert.assertTrue(m_reader.advance());
      Assert.assertTrue(m_reader.advance());
      Assert.assertFalse(m_reader.advance());
   }

   @Test
   public void close() throws IOException
   {
      m_reader.close();
   }

   private StdinBoundedReader createBytesReader()
   {
      return (StdinBoundedReader) m_boundSourceByteSource.createReader(
         m_options);
   }

   @Test
   public void createReader() throws Exception
   {
      final UnboundedSource.UnboundedReader<byte[]>
         reader = m_unbounded.createReader(null, null);
      Assert.assertNotNull(reader);
   }

   @Test
   public void generateInitialSplits() throws Exception
   {
      final List<? extends UnboundedSource<byte[], UnboundedSource
         .CheckpointMark>>
         splits = m_unbounded.split(1, null);
      Assert.assertTrue(splits.size() > 0);
   }

   @Test
   public void getCheckpointMark() throws Exception
   {
      Assert.assertNotNull(m_reader.getCheckpointMark());
   }

   @Test
   public void getCheckpointMarkCoder() throws Exception
   {
      final Coder<UnboundedSource.CheckpointMark>
         coder = m_unbounded.getCheckpointMarkCoder();
      Assert.assertNotNull(coder);
   }

   @Test
   public void getCurrent() throws Exception
   {
      m_reader.start();
      Assert.assertArrayEquals(new byte[]{0x65, 0x0a}, m_reader.getCurrent
         ());
      m_reader.advance();
      Assert.assertArrayEquals(new byte[]{0x66, 0x0a}, m_reader.getCurrent
         ());
      Assert.assertFalse(m_reader.advance());
   }

   @Test
   public void getCurrentSource() throws Exception
   {
      Assert.assertNotNull(m_reader.getCurrentSource());
   }

   @Test
   public void getCurrentTimestamp() throws Exception
   {
      Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE,
         m_reader.getCurrentTimestamp());
   }

   @Test
   public void getDefaultOutputCoder() throws Exception
   {
      Assert.assertNotNull(m_unbounded.getDefaultOutputCoder());
   }

   @Test
   public void getDefaultOutputCoderBounded()
   {
      Assert.assertNotNull(m_boundSource.getDefaultOutputCoder());
   }

   @Test
   public void getEstimatedSizeBytes() throws IOException
   {
      m_boundSource.getEstimatedSizeBytes(m_options);
   }

   @Test
   public void getWatermark() throws Exception
   {
      Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE,
         m_reader.getWatermark());
   }

   @Before
   public void setUp()
   {
      m_boundSource = new StreamIO.BoundSource();
      m_boundSourceByteSource = new StreamIO.BoundSource(
         new SerializableByteArrayInputStream(new byte[]{65, 10}));

      m_unbounded = new StreamIO.UnboundSource();
      final ByteArrayInputStream bytes = new ByteArrayInputStream(
         new byte[]{0x65, 0x0a, 0x66, 0x0a});
      m_reader = new StreamIO.UnboundSource.UnboundReader(
         new StreamIO.UnboundSource(), bytes);
   }

   @Test
   public void splitIntoBundles()
   {
      final List<? extends BoundedSource<byte[]>>
         result = m_boundSource.split(100,
         m_options);

      Assert.assertEquals(1, result.size());
   }

   @Test
   public void start() throws Exception
   {
      Assert.assertTrue(m_reader.start());
   }

   @Test
   public void testBoundInputCustomStream()
   {
      TestPipeline p = newPipeline();
      final SerializableByteArrayOutputStream baos =
         new SerializableByteArrayOutputStream();
      p
         // bounded test data
         .apply(Create.of(new byte[]{0x68, 0x0a}, new byte[]{0x65, 0x0a}))
         // sink to stdout
         .apply(StreamIO.write(baos));
      p.run();
      byte[] result = baos.toByteArray();
      Assert.assertTrue(result[0] == 0x68 || result[0] == 0x65);
      Assert.assertEquals(0x0a, result[1]);
      Assert.assertTrue(result[2] == 0x68 || result[2] == 0x65);
      Assert.assertEquals(0x0a, result[3]);
      Assert.assertEquals(4, result.length);
   }

   @Test
   public void testBoundInputStderr()
   {
      TestPipeline p = newPipeline();
      p
         // bounded test data
         .apply(Create.of(new byte[]{0x68, 0x0a}, new byte[]{0x65, 0x0a}))
         // sink to stdout
         .apply(StreamIO.stderr());
      p.run();
   }

   @Test
   public void testBoundInputStdout()
   {
      TestPipeline p = newPipeline();
      p
         // bounded test data
         .apply(Create.of(new byte[]{0x68, 0x0a}, new byte[]{0x65, 0x0a}))
         // sink to stdout
         .apply(StreamIO.stdout());
      p.run();
   }

   @Test
   public void testConstruct()
   {
      new StreamIO.UnboundSource.UnboundReader(null);
   }

   @Test
   public void testConstructor()
   {
      new StreamIO();
   }

   @Test
   public void testCreateReader()
   {
      m_boundSource.createReader(m_options);
   }

   @Test
   public void testReadBound()
   {
      final SerializableByteArrayInputStream bais = new
         SerializableByteArrayInputStream(new
         byte[]
         {0x68, 0x0a, 0x65, 0x0a, 0x6c, 0x0a, 0x6c, 0x0a, 0x6f, 0x0a});
      final TestPipeline pipeline = newPipeline();
      PCollection<String> lines = pipeline.apply(
         StreamIO.readBound(bais)).apply(ByteArrayToString.of("UTF-8"));
      pipeline.run();
      PAssert.that(lines).containsInAnyOrder("h", "e", "l", "l", "o");
   }

   @Test
   public void testReadUnbounded()
   {
      StreamIO.stdinUnbounded();
      StreamIO.readUnbounded(
         new SerializableByteArrayInputStream(new byte[]{}));
   }

   @Test
   public void testReaderAdvance() throws IOException
   {
      createBytesReader().advance();
   }

   @Test
   public void testReaderGetSource()
   {
      Assert.assertSame(m_boundSourceByteSource,
         createBytesReader().getCurrentSource());
   }

   @Test
   public void testStdinBound()
   {
      StreamIO.stdinBound();
   }

   @Test
   public void testStreamIoReadUnbounded()
   {
      SerializableByteArrayInputStream inStream =
         new SerializableByteArrayInputStream
            (new byte[]{0x32, 0x0a, 0x33, 0x0a, 0x35, 0x0a, 0x37, 0x0a, 0x31,
               0x31, 0x0a, 0x31, 0x33, 0x0a});
      final Trigger trigger = Repeatedly.forever(
         AfterPane.elementCountAtLeast(1));
      final PCollection<Integer> output = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true)
         .apply(StreamIO.readUnbounded(inStream))
         .apply(Window.<byte[]>configure().triggering(trigger)
            .accumulatingFiredPanes())
         .apply(ByteArrayToString.of("UTF-8"))
         .apply(MapElements.<String, Integer>via(new IntegerParseInt()))
         .apply(Sum.integersGlobally())
         .apply(LoggingIO.readwrite("AfterPaneBehavior", Level.WARNING));
      PAssert.thatSingleton(output).isEqualTo(2 + 3 + 5 + 7 + 11 + 13);
   }

   @Test
   public void testUnboundSourceConstructor()
   {
      final ByteArrayInputStream bais = new ByteArrayInputStream(
         new byte[]{});
      new StreamIO.UnboundSource(bais);
   }

   @Test
   public void testWriteBound()
   {
      TestPipeline pipeline = TestPipeline.create();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final PCollection<String> source = pipeline.apply(
         Create.of("foo", "bar", "baz"));
      // source.apply(new StreamIO.Write.Bound(baos));
      // TODO: This isn't working currently, see comment at top of StreamIO
      // m_pipeline.run();
   }

   @Test
   public void validate() throws Exception
   {
      m_unbounded.validate();
   }

   @Test
   public void validateBounded() throws Exception
   {
      m_boundSource.validate();
   }
}
