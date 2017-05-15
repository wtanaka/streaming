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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
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
   MainRunner()
   {
   }

   public static void cmdLine(String[] args, PTransform<PCollection<byte[]>,
      PCollection<byte[]>> transform)
   {
      MainRunner.cmdLine(StreamIO.stdinUnbounded(),
         StreamIO.stdout(), args, transform);
   }

   /**
    * @param args command line arguments
    * @param transform PTransform to convert input to output
    * @param in PTransform providing input data
    * @param out PTransform to send output data
    */
   public static void cmdLine(
      PTransform<PBegin, PCollection<byte[]>> in,
      PTransform<PCollection<byte[]>, PDone> out,
      final String[] args,
      PTransform<PCollection<byte[]>, PCollection<byte[]>> transform)
   {
      assert transform != null;
      Pipeline p = createPipeline(args);
      final PCollection<byte[]> stdin = p.apply(in);
      final PCollection<byte[]> output = stdin.apply(transform);
      output.apply(out);
      p.run();
   }

   private static Pipeline createPipeline(final String[] args)
   {
      final PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
         .withValidation().create();
      options.setRunner(DirectRunner.class);
      return Pipeline.create(options);
   }
}

