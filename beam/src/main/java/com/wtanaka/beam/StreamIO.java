/*
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com>
 */
package com.wtanaka.beam;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.Iterator;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import static java.io.StreamTokenizer.TT_EOF;

/**
 * Contains bindings between InputStream and PTransform, for experimenting
 * with DirectRunner
 */
public class StreamIO
{
   public static final Coder<String> DEFAULT_TEXT_CODER =
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
               Create.<String>of((Iterable<String>) stream)
               .withCoder(DEFAULT_TEXT_CODER);
            return input.apply(transform);
         }
      }
   }
}
