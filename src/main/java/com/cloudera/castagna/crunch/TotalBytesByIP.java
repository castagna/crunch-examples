/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.castagna.crunch;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// From org.apache.crunch.examples.TotalBytesByIP

@SuppressWarnings("serial")
public class TotalBytesByIP extends Configured implements Tool, Serializable {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println();
			System.err.println("Two and only two arguments are accepted.");
			System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}

		Pipeline pipeline = new MRPipeline(TotalBytesByIP.class, getConf());
		PCollection<String> lines = pipeline.readTextFile(args[0]);
		CombineFn<String, Long> longSumCombiner = CombineFn.SUM_LONGS();

		PTable<String, Long> ipAddrResponseSize = lines.parallelDo(
				extractIPResponseSize,
				Writables.tableOf(Writables.strings(), Writables.longs())
		).groupByKey().combineValues(longSumCombiner).top(10);
		
		pipeline.writeTextFile(ipAddrResponseSize, args[1]);
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

	DoFn<String, Pair<String, Long>> extractIPResponseSize = new DoFn<String, Pair<String, Long>>() {
		transient Pattern pattern;

		public void initialize() {
			pattern = Pattern.compile(Constants.logRegex);
		}

		public void process(String line, Emitter<Pair<String, Long>> emitter) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.matches()) {
				try {
					Long requestSize = Long.parseLong(matcher.group(7));
					String remoteAddr = matcher.group(1);
					emitter.emit(Pair.of(remoteAddr, requestSize));
				} catch (NumberFormatException e) {
					this.getCounter(Constants.COUNTERS.CORRUPT_SIZE).increment(1);
				}
			}
		}
	};

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new TotalBytesByIP(), args);
		System.exit(exitCode);
	}

}