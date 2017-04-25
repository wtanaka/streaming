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

import java.io.Serializable;
import java.util.logging.Level;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.transforms.ByteArrayToString;
import com.wtanaka.beam.transforms.Sequential;

/**
 * <p>Implementation of a streaming version of StreamingWc that outputs
 * statistics periodically
 * <p>
 * yes hello world | java -cp beam/build/libs/beam-all.jar
 * com.wtanaka.beam.StreamingWc
 */
public class StreamingWc
{
   static class WcStats implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private final int m_numLines;
      private final int m_numWords;
      private final int m_numBytes;

      WcStats(int numLines, int numWords, int numBytes)
      {
         m_numLines = numLines;
         m_numWords = numWords;
         m_numBytes = numBytes;
      }

      int getNumBytes()
      {
         return m_numBytes;
      }

      int getNumLines()
      {
         return m_numLines;
      }

      int getNumWords()
      {
         return m_numWords;
      }

      @Override
      public String toString()
      {
         return m_numLines + " " + m_numWords + " " + m_numBytes;
      }
   }

   static class StatsCombineFn extends Combine.CombineFn<byte[],
      WcStats, WcStats>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public WcStats addInput(final WcStats accumulator,
                              final byte[] byteInput)
      {
         final String input = new String(byteInput);
         return new WcStats(accumulator.getNumLines() + 1,
            accumulator.getNumWords() + input.split("\\s+").length,
            accumulator.getNumBytes() + byteInput.length);
      }

      @Override
      public WcStats createAccumulator()
      {
         return new WcStats(0, 0, 0);
      }

      @Override
      public WcStats extractOutput(final WcStats accumulator)
      {
         return accumulator;
      }

      @Override
      public WcStats mergeAccumulators(
         final Iterable<WcStats> accumulators)
      {
         int numLines = 0;
         int numWords = 0;
         int numBytes = 0;
         for (WcStats stats : accumulators)
         {
            numLines += stats.getNumLines();
            numWords += stats.getNumWords();
            numBytes += stats.getNumBytes();
         }
         return new WcStats(numLines, numWords, numBytes);
      }
   }

   public static class Transform extends PTransform<PCollection<byte[]>,
      PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         input.apply(ByteArrayToString.of("UTF-8")).apply
            (LoggingIO.write("StreamingWc.Transform", Level.SEVERE));
         final PCollection<byte[]> triggered = input.apply(
            Window.<byte[]>triggering(
               Repeatedly.forever
                  (AfterPane.elementCountAtLeast(11)))
               .accumulatingFiredPanes());
         PCollection<WcStats> stats = triggered.apply(
            Combine.globally(new StatsCombineFn()));
         return stats.apply(MapElements.via(
            new SimpleFunction<WcStats, byte[]>()
            {
               private static final long serialVersionUID = 1L;

               public byte[] apply(WcStats s)
               {
                  return s.toString().getBytes();
               }
            }));
      }
   }

   public static void main(String[] args)
   {
      MainRunner.cmdLine(
         Read.from(new StdinIO.UnboundSource()),
         Sequential.of(
            ByteArrayToString.of("UTF-8"),
            LoggingIO.write("StreamingWc", Level.WARNING)),
         args,
         new Transform());
   }
}
