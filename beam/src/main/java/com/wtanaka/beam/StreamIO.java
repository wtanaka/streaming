/*
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com>
 */
package com.wtanaka.beam;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Contains bindings between InputStream and PTransform, for experimenting
 * with DirectRunner
 */
public class StreamIO
{
   private static class ReadDoFn extends DoFn<Void, byte[]>
   {
      private static final long serialVersionUID = -8015369258496550935L;

      @ProcessElement
      public void processElement(ProcessContext c)
      {

      }
   }

   public static class StreamLineIterable implements Iterable<byte[]>
   {
      private final InputStream m_inputStream;

      public StreamLineIterable(InputStream inputStream)
      {
         m_inputStream = inputStream;
      }

      @Override
      public Iterator<byte[]> iterator()
      {
         return new Iterator<byte[]>()
         {
            @Override
            public boolean hasNext()
            {
               return false;
            }

            @Override
            public byte[] next()
            {
               return new byte[0];
            }

            @Override
            public void remove()
            {
               throw new UnsupportedOperationException();
            }
         };
      }
   }

   public static class Read
   {
      public static class Bound
         extends PTransform<PBegin, PCollection<byte[]>>
      {
         private final InputStream m_inputStream;

         public Bound(InputStream inputStream)
         {
            m_inputStream = inputStream;
         }

         @Override
         public PCollection<byte[]> expand(final PBegin input)
         {
            return input.apply(Create.<byte[]>of(new StreamLineIterable
               (m_inputStream)).withCoder(ByteArrayCoder.of()));
         }
      }

      private final InputStream m_stream;

      public Read(InputStream stream)
      {
         m_stream = stream;
      }
   }
}
