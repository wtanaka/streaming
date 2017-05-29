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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * PTransform that logs its input
 */
public class LoggingIO
{
   public static class ReadWrite<T> extends
      PTransform<PCollection<T>, PCollection<T>>
   {
      private static final long serialVersionUID = 7349020373029956433L;
      private final DoFn<T, T> m_doFn;

      static class LogDoFn<T> extends DoFn<T, T>
      {
         private static final long serialVersionUID = -7710028799519540960L;
         private final String m_loggerString;
         private final Level m_level;

         LogDoFn(String loggerString, Level level)
         {
            m_loggerString = loggerString;
            m_level = level;
         }

         @ProcessElement
         public void processElement(ProcessContext c)
         {
            final T element = c.element();
            Logger.getLogger(m_loggerString).log(m_level,
               "<" + c.timestamp() + "> " + element
                  + (c.pane() == PaneInfo.NO_FIRING ? "" :
                  " [pane: " + c.pane() + "]"));
            c.outputWithTimestamp(element, c.timestamp());
         }
      }

      ReadWrite(String logger, Level level)
      {
         m_doFn = new LogDoFn<T>(logger, level);
      }

      @Override
      public PCollection<T> expand(final PCollection<T> input)
      {
         return input.apply(ParDo.of(m_doFn));
      }
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>>
      readwrite(String loggerString, Level level)
   {
      return new ReadWrite<T>(loggerString, level);
   }
}
