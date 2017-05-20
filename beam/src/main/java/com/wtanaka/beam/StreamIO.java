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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

import com.wtanaka.beam.io.EmptyCheckpointMark;

/**
 * Contains bindings between InputStream and PTransform, for experimenting
 * with DirectRunner
 */
public class StreamIO
{
   private enum HardCodedOutputStream implements Serializable
   {
      STDOUT, STDERR
   }

   private static final Coder<String> DEFAULT_TEXT_CODER =
      StringUtf8Coder.of();

   private static class OutStreamDoFn<OutStreamT extends OutputStream &
      Serializable> extends DoFn<byte[], Void>
   {
      private final OutStreamT m_stream;
      private final HardCodedOutputStream m_streamKey;

      public OutStreamDoFn(final OutStreamT stream)
      {
         m_stream = stream;
         m_streamKey = null;
         assert (m_stream != null) ^ (m_streamKey != null);
      }

      public OutStreamDoFn(final HardCodedOutputStream streamKey)
      {
         m_streamKey = streamKey;
         m_stream = null;
         assert (m_stream != null) ^ (m_streamKey != null);
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException
      {
         byte[] line = c.element();

         OutputStream stream = m_stream;
         // We can't store this in a map ahead of time because someone may
         // have called System.setOut or System.setErr
         if (stream == null)
         {
            switch (m_streamKey)
            {
               case STDOUT:
                  stream = System.out;
                  break;
               case STDERR:
                  stream = System.err;
                  break;
            }
         }

         assert stream != null;
         stream.write(line);
         stream.flush();
      }
   }

   private static class OutTransform<OutT extends OutputStream &
      Serializable> extends PTransform<PCollection<byte[]>, PDone>
   {
      private final OutT m_stream;
      private final HardCodedOutputStream m_streamKey;
      private final PTransform<PCollection<byte[]>, PDone> m_parDo;

      public OutTransform(OutT stream)
      {
         m_stream = stream;
         m_streamKey = null;
         m_parDo = ParDo.of(new OutStreamDoFn(m_stream));
      }

      public OutTransform(HardCodedOutputStream streamKey)
      {
         m_stream = null;
         m_streamKey = streamKey;
         m_parDo = ParDo.of(new OutStreamDoFn(m_streamKey));
      }

      @Override
      public PDone expand(final PCollection<byte[]> input)
      {
         input.apply(m_parDo);
         return PDone.in(input.getPipeline());
      }
   }

   /**
    * BoundSource
    */
   static class BoundSource<InT extends InputStream & Serializable> extends
      BoundedSource<byte[]>
   {
      private static final long serialVersionUID = 1L;
      private final InT m_serializableInStream;

      /**
       * Source.Reader implementation for Stdin
       */
      static class StdinBoundedReader<InT extends InputStream & Serializable>
         extends BoundedReader<byte[]>
      {
         private final InT m_stream;
         private final BoundedSource<byte[]> m_source;
         private final ByteArrayOutputStream m_buffer =
            new ByteArrayOutputStream();

         private StdinBoundedReader(final BoundedSource<byte[]> source,
                                    InT stream)
         {
            m_source = source;
            m_stream = stream;
         }

         @Override
         public boolean advance() throws IOException
         {
            return readNext();
         }

         @Override
         public void close()
         {

         }

         @Override
         public byte[] getCurrent() throws NoSuchElementException
         {
            return m_buffer.toByteArray();
         }

         @Override
         public BoundedSource<byte[]> getCurrentSource()
         {
            return m_source;
         }

         private InputStream getStream()
         {
            return m_stream == null ? System.in : m_stream;
         }

         private boolean readNext() throws IOException
         {
            m_buffer.reset();
            int ch = getStream().read();
            if (ch == -1)
            {
               return false;
            }
            while (ch != -1 && ch != (int) '\n')
            {
               m_buffer.write(ch);
               ch = getStream().read();
            }
            if (ch != -1)
            {
               m_buffer.write(ch);
            }
            return true;
         }

         @Override
         public boolean start() throws IOException
         {
            return readNext();
         }
      }

      BoundSource()
      {
         m_serializableInStream = null;
      }

      BoundSource(InT serializableInStream)
      {
         m_serializableInStream = serializableInStream;
      }

      @Override
      public BoundedReader<byte[]> createReader(
         final PipelineOptions options)
      {
         return new StdinBoundedReader(this, m_serializableInStream);
      }

      @Override
      public Coder<byte[]> getDefaultOutputCoder()
      {
         return ByteArrayCoder.of();
      }

      @Override
      public long getEstimatedSizeBytes(final PipelineOptions options)
         throws IOException
      {

         final int estimate = m_serializableInStream != null ?
            m_serializableInStream.available() : System.in.available();
         return estimate;
      }

      @Override
      public List<? extends BoundedSource<byte[]>> split(
         final long desiredBundleSizeBytes, final PipelineOptions options)
      {
         return Collections.singletonList(this);
      }

      @Override
      public void validate()
      {
      }
   }

   /**
    * UnboundSource
    */
   static class UnboundSource<InStreamT extends InputStream & Serializable>
      extends UnboundedSource<byte[], EmptyCheckpointMark>
   {
      private static final long serialVersionUID = 1L;

      private InStreamT m_stream;

      /**
       * Source.Reader implementation for Stdin
       */
      static class UnboundReader<InStreamT extends InputStream & Serializable>
         extends UnboundedReader<byte[]>
      {
         final private UnboundedSource<byte[], ?> m_source;
         private final InStreamT m_stream;
         private final ByteArrayOutputStream m_buffer =
            new ByteArrayOutputStream();
         private Instant m_timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
         private Instant m_watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

         UnboundReader(final UnboundedSource<byte[], ?> source)
         {
            this(source, null);
         }

         UnboundReader(final UnboundedSource<byte[], ?> source,
                       InStreamT stream)
         {
            m_source = source;
            m_stream = stream;
         }

         @Override
         public boolean advance() throws IOException
         {
            return readNext();
         }

         @Override
         public void close() throws IOException
         {
         }

         @Override
         public CheckpointMark getCheckpointMark()
         {
            return () ->
            {
            };
         }

         @Override
         public byte[] getCurrent() throws NoSuchElementException
         {
            return m_buffer.toByteArray();
         }

         @Override
         public UnboundedSource<byte[], ?> getCurrentSource()
         {
            return m_source;
         }

         @Override
         public Instant getCurrentTimestamp() throws NoSuchElementException
         {
            return m_timestamp;
         }

         private InputStream getStream()
         {
            return m_stream == null ? System.in : m_stream;
         }

         @Override
         public Instant getWatermark()
         {
            return m_watermark;
         }

         private boolean readNext() throws IOException
         {
            m_buffer.reset();
            int ch = getStream().read();
            if (ch == -1)
            {
               m_watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
               return false;
            }
            while (ch != -1 && ch != (int) '\n')
            {
               m_buffer.write(ch);
               ch = getStream().read();
            }
            if (ch != -1)
            {
               m_buffer.write(ch);
            }
            m_watermark = m_timestamp = Instant.now();
            return true;
         }

         @Override
         public boolean start() throws IOException
         {

            return readNext();
         }
      }

      public UnboundSource(final InStreamT in)
      {
         m_stream = in;
      }

      public UnboundSource()
      {
         m_stream = null;
      }

      @Override
      public UnboundedReader<byte[]> createReader(
         final PipelineOptions options,
         final EmptyCheckpointMark ignored)
         throws IOException
      {
         return new UnboundReader(this, m_stream);
      }

      @Override
      public Coder<EmptyCheckpointMark> getCheckpointMarkCoder()
      {
         return AvroCoder.of(EmptyCheckpointMark.class);
      }

      @Override
      public Coder<byte[]> getDefaultOutputCoder()
      {
         return ByteArrayCoder.of();
      }

      @Override
      public List<? extends UnboundedSource<byte[], EmptyCheckpointMark>>
      split(
         final int desiredNumSplits, final PipelineOptions options)
         throws Exception
      {
         return Collections.singletonList(this);
      }

      @Override
      public String toString()
      {
         return "[StdinIO.UnboundSource]";
      }

      @Override
      public void validate()
      {
      }
   }

   public static PTransform<PBegin, PCollection<byte[]>> stdinBound()
   {
      return Read.from(new BoundSource());
   }

   public static PTransform<PBegin, PCollection<byte[]>> stdinUnbounded()
   {
      return Read.from(new UnboundSource());
   }

   public static PTransform<PCollection<byte[]>, PDone> stderr()
   {
      return new OutTransform(HardCodedOutputStream.STDERR);
   }

   public static PTransform<PCollection<byte[]>, PDone> stdout()
   {
      return new OutTransform(HardCodedOutputStream.STDOUT);
   }

   public static <OutStreamT extends OutputStream & Serializable>
   PTransform<PCollection<byte[]>, PDone> write(OutStreamT out)
   {
      return new OutTransform(out);
   }

   public static <InStreamT extends InputStream & Serializable>
   PTransform<PBegin, PCollection<byte[]>> readUnbounded(InStreamT in)
   {
      return Read.from(new UnboundSource(in));
   }

   public static <InStreamT extends InputStream & Serializable>
   PTransform<PBegin, PCollection<byte[]>>
   readBound(final InStreamT in)
   {
      return Read.from(new BoundSource(in));
   }

   ;
}
