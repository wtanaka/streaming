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

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Utility code to run beam pipelines on the command line, processing stdin
 * into stdout
 */
@SuppressWarnings("WeakerAccess")
public class MainRunner
{
   static void cmdLine(String[] args, PTransform<PCollection<String>,
      PCollection<String>> transform)
   {
      MainRunner.cmdLine(Read.from(new StdinBoundedSource()),
         Write.to(new StdoutSink()), args, transform);
   }

   /**
    * @param args
    * @param transform TODO: Change to byte[], byte[] or even Byte, Byte
    */
   static void cmdLine(PTransform<PBegin, PCollection<byte[]>> in,
                       PTransform<PCollection<byte[]>, PDone> out,
                       final String[] args,
                       PTransform<PCollection<String>, PCollection<String>>
                          transform)
   {
      final PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
         .withValidation().create();
      options.setRunner(DirectRunner.class);
      Pipeline p = Pipeline.create(options);
      final PCollection<byte[]> stdin = p.apply(in);
      final PCollection<String> strStdin = stdin.apply(
         MapElements.via(new SimpleFunction<byte[],
            String>()
         {
            private static final long serialVersionUID = 7602634974725087165L;

            @Override
            public String apply(final byte[] input)
            {
               return new String(input);
            }
         }));
      final PCollection<String> output = strStdin.apply(transform);
      final PCollection<byte[]> byteOut = output.apply(
         MapElements.via(new SimpleFunction<String, byte[]>()
         {
            private static final long serialVersionUID = 5126587914661976309L;

            @Override
            public byte[] apply(final String input)
            {
               return input.getBytes();
            }
         })
      );
      byteOut.apply(out);
      p.run();
   }
}

