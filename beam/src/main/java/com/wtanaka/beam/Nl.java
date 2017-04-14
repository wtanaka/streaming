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

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * <p>Implementation of an "approximate" version of nl
 *
 * yes | head -50 | java -cp beam/build/libs/beam-all.jar
 * com.wtanaka.beam.Nl
 */
public class Nl
{
   public static class Transform extends PTransform<PCollection<String>,
      PCollection<String>>
   {
      private static final long serialVersionUID = 1L;

      public static class CountingDoFn
         extends DoFn<KV<Integer, String>, String>
      {
         private static final long serialVersionUID = 1L;
         private static final String STATE_ID = "countState";
         private static final int FIRST_LINE_NUM = 1;

         @StateId(STATE_ID)
         private final StateSpec<Object, ValueState<Integer>>
            stateCell = StateSpecs.value(VarIntCoder.of());

         @ProcessElement
         public void process(ProcessContext context, @StateId(STATE_ID)
            ValueState<Integer> state)
         {
            final Integer stateVal = state.read();
            final int approxLineNum =
               (stateVal == null ? FIRST_LINE_NUM : stateVal);
            state.write(approxLineNum + 1);
            final String nonKeyedInput = context.element().getValue();
            final String output = String.valueOf(approxLineNum) + "\t" +
               nonKeyedInput;
            context.output(output);
         }
      }

      @Override
      public PCollection<String> expand(final PCollection<String> input)
      {
         // Attach an arbitrary hard-coded key to each string so they share
         // the same DoFn state
         final PCollection<KV<Integer, String>> keyedInput = input.apply(
            WithKeys.of((SerializableFunction<String, Integer>) s -> 3))
            .setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()));
         return keyedInput.apply(ParDo.of(new CountingDoFn()));
      }
   }

   public static void main(String[] args)
   {
      MainRunner.cmdLine(args, new Nl.Transform());
   }
}
