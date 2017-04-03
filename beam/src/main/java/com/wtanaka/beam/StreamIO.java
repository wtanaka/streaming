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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import static java.io.StreamTokenizer.TT_EOF;

/**
 * Contains bindings between InputStream and PTransform, for experimenting
 * with DirectRunner
 *
 * This class is currently not working; I believe it may be because
 * DirectRunner serializes DoFn classes between elements in a stream to
 * help application developers write non-buggy code.
 */
public class StreamIO
{
   private static final Coder<String> DEFAULT_TEXT_CODER =
      StringUtf8Coder.of();

   public static class Read
   {
      private final InputStream m_stream;

      public Read(InputStream stream)
      {
         m_stream = stream;
      }

      public static class StreamLineIterable implements Iterable<String>
      {
         private final InputStream m_inputStream;
         private final StreamTokenizer m_tokenizer;

         public StreamLineIterable(InputStream inputStream)
         {
            m_inputStream = inputStream;
            Reader r = new BufferedReader(
               new InputStreamReader(m_inputStream));
            // TODO: This is not thread safe
            m_tokenizer = new StreamTokenizer(r);
         }

         @Override
         public Iterator<String> iterator()
         {
            return new Iterator<String>()
            {
               @Override
               public synchronized boolean hasNext()
               {
                  try
                  {
                     final int peek = m_tokenizer.nextToken();
                     m_tokenizer.pushBack();
                     return (peek != TT_EOF);
                  }
                  catch (IOException e)
                  {
                     // TODO: Log
                     return false;
                  }
               }

               @Override
               public synchronized String next()
               {
                  try
                  {
                     final int result = m_tokenizer.nextToken();
                     assert result != TT_EOF;
                  }
                  catch (IOException e)
                  {
                     throw new IllegalStateException(e);
                  }
                  return m_tokenizer.sval;
               }

               @Override
               public void remove()
               {
                  throw new UnsupportedOperationException();
               }
            };
         }
      }

      public static class Bound
         extends PTransform<PBegin, PCollection<String>>
      {
         private static final long serialVersionUID = -8522252199996579879L;
         private final InputStream m_inputStream;

         public Bound(InputStream inputStream)
         {
            m_inputStream = inputStream;
         }

         @Override
         public PCollection<String> expand(final PBegin input)
         {
            final StreamLineIterable stream = new StreamLineIterable(
               m_inputStream);
            final PTransform<PBegin, PCollection<String>> transform =
               Create.of(stream).withCoder(DEFAULT_TEXT_CODER);
            return input.apply(transform);
         }
      }
   }

   public static class Write
   {
      public static class StreamWriterDoFn extends DoFn<String, Void>
         implements Serializable
      {
         private static final long serialVersionUID = -4797179060913460533L;
         private final transient PrintStream m_printStream;

         public StreamWriterDoFn(PrintStream printStream)
         {
            m_printStream = printStream;
            assert m_printStream != null;
         }

         @ProcessElement
         public void processElement(ProcessContext c)
         {
            String element = c.element();
            // TODO: This needs to hold for this class to work, but does
            // not currently, even inside of DirectRunner
            assert m_printStream != null;
            m_printStream.print(element);
         }
      }

      public static class Bound
         extends PTransform<PCollection<String>, PDone>
      {
         private static final long serialVersionUID = -8079463736231313274L;
         private final PrintStream m_printStream;

         public Bound(OutputStream outputStream)
         {
            try
            {
               m_printStream = new PrintStream(outputStream, true, "UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
               throw new RuntimeException("Could not find encoding UTF-8");
            }
         }

         @Override
         public PDone expand(final PCollection<String> input)
         {
            final PCollection<Void> almostdone = input.apply(
               ParDo.of(new StreamWriterDoFn(m_printStream)));

            return PDone.in(input.getPipeline());
         }
      }

   }
}
