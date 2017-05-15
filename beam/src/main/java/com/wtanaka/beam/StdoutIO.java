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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Transform for outputting to System.out
 */
class StdoutIO
{
   /**
    * Sink implementation for Stdout
    */
   static class StdoutSink extends Sink<byte[]>
   {
      private static final long serialVersionUID = 1L;
      private final OutputStream m_serializableOutputStream;

      static class StdoutWriter extends Writer<byte[], Void>
      {
         private final WriteOperation<byte[], Void> m_writeOperation;
         private final OutputStream m_outputStream;

         StdoutWriter(
            final WriteOperation<byte[], Void> writeOperation,
            final OutputStream outputStream)
         {
            m_writeOperation = writeOperation;
            m_outputStream = outputStream;
         }

         @Override
         public Void close()
         {
            return null;
         }

         @Override
         public WriteOperation<byte[], Void> getWriteOperation()
         {
            return m_writeOperation;
         }

         @Override
         public void open(final String uId)
         {
         }

         @Override
         public void write(final byte[] value) throws IOException
         {
            m_outputStream.write(value);
            // System.out.write((int) '\n');
         }
      }

      static class StdoutWriteOperation extends Sink
         .WriteOperation<byte[], Void>
      {
         private static final long serialVersionUID = 1L;
         private final OutputStream m_serializableOutputStream;
         private final Sink<byte[]> m_sink;

         StdoutWriteOperation(final Sink<byte[]> sink,
                              final OutputStream serializableOutputStream)
         {
            m_sink = sink;
            assert serializableOutputStream == null ||
               (serializableOutputStream instanceof Serializable) :
               "Stream " + serializableOutputStream + " must be Serializable";
            m_serializableOutputStream = serializableOutputStream;
         }

         @Override
         public Writer<byte[], Void> createWriter(
            final PipelineOptions options)
         {
            return new StdoutSink.StdoutWriter(this,
               getStream());
         }

         @Override
         public void finalize(final Iterable<Void> writerResults,
                              final PipelineOptions options)
            throws IOException
         {
            getStream().flush();
         }

         @Override
         public Sink<byte[]> getSink()
         {
            return m_sink;
         }

         private OutputStream getStream()
         {
            if (m_serializableOutputStream == null)
            {
               return System.out;
            }
            else
            {
               return m_serializableOutputStream;
            }
         }

         @Override
         public Coder<Void> getWriterResultCoder()
         {
            return VoidCoder.of();
         }

         @Override
         public void initialize(final PipelineOptions options)
         {
         }
      }

      StdoutSink()
      {
         m_serializableOutputStream = null;
      }

      StdoutSink(
         final OutputStream serializableOutput)
      {
         m_serializableOutputStream = serializableOutput;
      }

      @Override
      public WriteOperation<byte[], ?> createWriteOperation(
         final PipelineOptions options)
      {
         return new StdoutSink.StdoutWriteOperation(this,
            m_serializableOutputStream);
      }

      @Override
      public void validate(final PipelineOptions options)
      {

      }
   }

   public static PTransform<PCollection<byte[]>, PDone> write()
   {
      return Write.to(new StdoutSink());
   }
}
