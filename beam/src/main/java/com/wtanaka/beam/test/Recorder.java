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
package com.wtanaka.beam.test;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

/**
 * Test result recorder that saves items in a static variable for use in
 * recording results in unit tests.  Saving items in an instance variable
 * from a PTransform won't work, since the value will get lost by the unit
 * test environment.
 */
public class Recorder<T> implements Serializable
{
   private final String m_testKey;
   private static final Hashtable<String, Vector> s_results = new
         Hashtable<>();

   public Recorder(String testKey)
   {
      m_testKey = testKey;
   }

   public void add(T item)
   {
      synchronized (s_results)
      {
         if (s_results.get(m_testKey) == null)
         {
            s_results.put(m_testKey, new Vector());
         }
      }
      s_results.get(m_testKey).add(item);
   }

   @Override
   protected void finalize() throws Throwable
   {
      s_results.remove(m_testKey);
      super.finalize();
   }

   public Vector<T> get()
   {
      return s_results.get(m_testKey);
   }
}
