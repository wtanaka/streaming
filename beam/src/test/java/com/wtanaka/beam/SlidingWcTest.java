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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class SlidingWcTest
{
   @Test
   public void testContruct()
   {
      new SlidingWc();
   }

   @Test
   public void testMain()
   {
      final InputStream oldIn = System.in;
      final PrintStream oldOut = System.out;
      try
      {
         final byte[] bytes = {65, 10, 66, 10};
         InputStream in = new SerializableByteArrayInputStream(bytes);
         System.setIn(in);
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         System.setOut(new PrintStream(out));
         SlidingWc.main(new String[]{});
         Assert.assertEquals(
            "2 2 4\n2 2 4\n2 2 4\n2 2 4\n2 2 4\n"
               + "2 2 4\n2 2 4\n2 2 4\n2 2 4\n2 2 4\n",
            new String(out.toByteArray(), StandardCharsets.UTF_8));
      }
      finally
      {
         System.setIn(oldIn);
         System.setOut(oldOut);
      }
   }

}