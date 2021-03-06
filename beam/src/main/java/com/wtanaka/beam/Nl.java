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

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * <p>Implementation of an "approximate" version of nl
 * <p>
 * yes | head -50 | java -cp beam/build/libs/beam-all.jar
 * com.wtanaka.beam.Nl
 */
public class Nl
{
   public static class Transform extends PTransform<PCollection<byte[]>,
      PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      public static class CountingDoFn
         extends DoFn<KV<Integer, byte[]>, byte[]>
      {
         private static final long serialVersionUID = 1L;
         private static final String STATE_ID = "countState";
         private static final int FIRST_LINE_NUM = 1;

         @StateId(STATE_ID)
         private final StateSpec<ValueState<Integer>>
            stateCell = StateSpecs.value(VarIntCoder.of());

         @ProcessElement
         public void process(ProcessContext context, @StateId(STATE_ID)
            ValueState<Integer> state)
         {
            final Integer stateVal = state.read();
            final int approxLineNum =
               (stateVal == null ? FIRST_LINE_NUM : stateVal);
            state.write(approxLineNum + 1);
            final byte[] nonKeyedInput = context.element().getValue();
            String inputValueStr = new String(nonKeyedInput);
            final byte[] output = (String.valueOf(approxLineNum) + "\t" +
               inputValueStr).getBytes();
            context.output(output);
         }
      }

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         // Attach an arbitrary hard-coded key to each string so they share
         // the same DoFn state
         return input
            .apply(
               WithKeys.of((SerializableFunction<byte[], Integer>) s -> 3))
            .setCoder(KvCoder.of(VarIntCoder.of(), ByteArrayCoder.of()))
            .apply(ParDo.of(new CountingDoFn()));
      }
   }

   Nl()
   {
   }

   public static void main(String[] args)
   {
      MainRunner.cmdLine(args, new Nl.Transform());
   }
}
