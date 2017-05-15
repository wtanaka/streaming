package com.wtanaka.beam;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

import com.wtanaka.beam.StdoutIO.StdoutSink;

/**
 * Test code for StdoutSink
 */
public class StdoutIOTest
{

   private final PipelineOptions m_pipelineOptions =
      PipelineOptionsFactory.create();

   private static class TestOutputStream extends OutputStream implements
      Serializable
   {
      private static final long serialVersionUID = 1L;

      @Override
      public void write(final int i) throws IOException
      {
      }
   }

   @Test
   public void constructWithStream()
   {
      new StdoutSink(new TestOutputStream());
   }

   private StdoutSink.StdoutWriteOperation createWriteOperation()
   {
      final StdoutSink sink = new StdoutSink();
      final StdoutSink.StdoutWriteOperation writeOperation =
         (StdoutSink.StdoutWriteOperation) sink.createWriteOperation(
            m_pipelineOptions);
      Assert.assertSame(sink, writeOperation.getSink());
      return writeOperation;
   }

   private StdoutSink.StdoutWriter createWriter()
   {
      final StdoutSink.StdoutWriteOperation writeOperation
         = createWriteOperation();
      final StdoutSink.StdoutWriter writer =
         (StdoutSink.StdoutWriter) writeOperation.createWriter
            (m_pipelineOptions);
      Assert.assertSame(writeOperation, writer.getWriteOperation());
      return writer;
   }

   @Test
   public void testWrite()
   {
      Assert.assertNotNull(StdoutIO.write());
   }

   @Test
   public void testWriteOpFinalize() throws IOException
   {
      createWriteOperation().finalize(Collections.emptyList(),
         m_pipelineOptions);
   }

   @Test
   public void testWriteOpGetWriterResultCoder()
   {
      final StdoutSink.StdoutWriteOperation writeOperation
         = createWriteOperation();
      final Coder<Void> coder =
         writeOperation.getWriterResultCoder();
      Assert.assertNotNull(coder);
   }

   @Test
   public void testWriteOpInitialize()
   {
      final StdoutSink.StdoutWriteOperation writeOperation
         = createWriteOperation();
      writeOperation.initialize(m_pipelineOptions);
   }

   @Test
   public void testWriterClose() throws IOException
   {
      final StdoutSink.StdoutWriter writer = createWriter();
      writer.close();
   }

   @Test
   public void testWriterOpen() throws IOException
   {
      final StdoutSink.StdoutWriter writer = createWriter();
      writer.open("someUniqueId1234");
   }

   @Test
   public void testWriterWriteOtherStream() throws Exception
   {
      try
      {
         final OutputStream stream = new SerializableByteArrayOutputStream();
         final StdoutSink.StdoutWriteOperation writeOperation
            = (StdoutSink.StdoutWriteOperation) new StdoutSink(stream)
            .createWriteOperation(m_pipelineOptions);
         final Sink.Writer<byte[], Void> writer = writeOperation.createWriter(
            m_pipelineOptions);
         writer.write(new byte[]{10});
      }
      finally
      {
         SerializableByteArrayOutputStream.reset();
      }
   }

   @Test
   public void testWriterWriteStdout() throws IOException
   {
      final StdoutSink.StdoutWriter writer = createWriter();
      writer.write(new byte[]{10});
   }

   @Test
   public void validate()
   {
      final StdoutSink sink = new StdoutSink();
      sink.validate(m_pipelineOptions);
   }
}
