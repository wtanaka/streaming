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
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.transforms.Combine;

public class Wc
{
   static class Stats implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private final int m_numBytes;
      private final int m_numLines;
      private final int m_numWords;

      Stats(int numLines, int numWords, int numBytes)
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
      Stats, Stats>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public Stats addInput(final Stats accumulator,
                            final byte[] byteInput)
      {
         final String input = new String(byteInput);
         return new Stats(accumulator.getNumLines() + 1,
            accumulator.getNumWords() + input.split("\\s+").length,
            accumulator.getNumBytes() + byteInput.length);
      }

      @Override
      public Stats createAccumulator()
      {
         return new Stats(0, 0, 0);
      }

      @Override
      public Stats extractOutput(final Stats accumulator)
      {
         return accumulator;
      }

      @Override
      public Stats mergeAccumulators(
         final Iterable<Stats> accumulators)
      {
         int numLines = 0;
         int numWords = 0;
         int numBytes = 0;
         for (Stats stats : accumulators)
         {
            numLines += stats.getNumLines();
            numWords += stats.getNumWords();
            numBytes += stats.getNumBytes();
         }
         return new Stats(numLines, numWords, numBytes);
      }
   }

   Wc()
   {
   }

   static byte[] bytesFor(Stats stats)
   {
      return (stats.toString() + "\n").getBytes(StandardCharsets.UTF_8);
   }
}
