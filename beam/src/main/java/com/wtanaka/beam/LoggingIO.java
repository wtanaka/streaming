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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PTransform that logs its input
 */
public class LoggingIO
{
   private enum Level
   {
      DEBUG, ERROR, INFO, TRACE, WARN
   }

   static class DefaultConvertToLog<T> implements ConvertToLog<T>
   {
      @Override
      public Object[] getArguments(T element,
                                   final Instant timestamp,
                                   final PaneInfo pane)
      {
         return new Object[]{timestamp, element,
            pane == PaneInfo.NO_FIRING ? "" : " [pane: " + pane + "]"};
      }

      @Override
      public String getFormat()
      {
         return "<{}> {}{}";
      }
   }

   public static class ReadWrite<T> extends
      PTransform<PCollection<T>, PCollection<T>>
   {
      private static final long serialVersionUID = 7349020373029956433L;
      private final DoFn<T, T> m_doFn;

      static class LogDoFn<T> extends DoFn<T, T>
      {
         private static final long serialVersionUID = -7710028799519540960L;
         private final ConvertToLog<T> m_converter;
         private final boolean m_isClass;
         private final Level m_level;
         private final Class m_loggerClass;
         private final String m_loggerString;

         LogDoFn(Class loggerClass, Level level)
         {
            m_converter = new DefaultConvertToLog<>();
            m_level = level;
            m_loggerClass = loggerClass;
            m_loggerString = null;
            m_isClass = true;
         }

         LogDoFn(String loggerString, Level level)
         {
            m_converter = new DefaultConvertToLog<>();
            m_level = level;
            m_loggerClass = null;
            m_loggerString = loggerString;
            m_isClass = false;
         }

         @ProcessElement
         public void processElement(ProcessContext c)
         {
            final Logger logger = m_isClass
               ? LoggerFactory.getLogger(m_loggerClass)
               : LoggerFactory.getLogger(m_loggerString);

            final T element = c.element();
            final Instant timestamp = c.timestamp();
            final PaneInfo pane = c.pane();

            final String format = m_converter.getFormat();
            final Object[] arguments = m_converter.getArguments(element,
               timestamp, pane);

            switch (m_level)
            {
               case TRACE:
                  logger.trace(format, arguments);
                  break;
               case DEBUG:
                  logger.debug(format, arguments);
                  break;
               case INFO:
                  logger.info(format, arguments);
                  break;
               case WARN:
                  logger.warn(format, arguments);
                  break;
               case ERROR:
                  logger.error(format, arguments);
                  break;
//               default:
//                  assert false;
            }
            c.outputWithTimestamp(element, timestamp);
         }
      }

      ReadWrite(Class logger, Level level)
      {
         m_doFn = new LogDoFn<>(logger, level);
      }

      ReadWrite(String logger, Level level)
      {
         m_doFn = new LogDoFn<>(logger, level);
      }

      @Override
      public PCollection<T> expand(final PCollection<T> input)
      {
         return input.apply(ParDo.of(m_doFn));
      }
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> debug(
      String string)
   {
      return new ReadWrite<>(string, Level.DEBUG);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> debug(
      Class clazz)
   {
      return new ReadWrite<>(clazz, Level.DEBUG);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> error(
      String string)
   {
      return new ReadWrite<>(string, Level.ERROR);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> error(
      Class clazz)
   {
      return new ReadWrite<>(clazz, Level.ERROR);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> info(
      String string)
   {
      return new ReadWrite<>(string, Level.INFO);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> info(
      Class clazz)
   {
      return new ReadWrite<>(clazz, Level.INFO);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> trace(
      String string)
   {
      return new ReadWrite<>(string, Level.TRACE);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> trace(
      Class clazz)
   {
      return new ReadWrite<>(clazz, Level.TRACE);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> warn(
      String string)
   {
      return new ReadWrite<>(string, Level.WARN);
   }

   public static <T> PTransform<PCollection<T>, PCollection<T>> warn(
      Class clazz)
   {
      return new ReadWrite<>(clazz, Level.WARN);
   }

   interface ConvertToLog<T> extends Serializable
   {
      Object[] getArguments(T element, final Instant timestamp,
                            final PaneInfo pane);

      String getFormat();
   }
}
