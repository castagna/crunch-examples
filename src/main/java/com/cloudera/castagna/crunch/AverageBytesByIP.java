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

import static org.apache.crunch.fn.Aggregators.SUM_LONGS;
import static org.apache.crunch.fn.Aggregators.pairAggregator;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
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

//From org.apache.crunch.examples.AverageBytesByIP

@SuppressWarnings("serial")
public class AverageBytesByIP extends Configured implements Tool, Serializable {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println();
			System.err.println("Two and only two arguments are accepted.");
			System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}

		Pipeline pipeline = new MRPipeline(AverageBytesByIP.class, getConf());
		PCollection<String> lines = pipeline.readTextFile(args[0]);
		Aggregator<Pair<Long, Long>> agg = pairAggregator(SUM_LONGS(), SUM_LONGS());

		PTable<String, Pair<Long, Long>> remoteAddrResponseSize = lines.parallelDo (
				extractResponseSize,
				Writables.tableOf (
						Writables.strings(),
						Writables.pairs(Writables.longs(), Writables.longs())
				)
		).groupByKey().combineValues(agg);

		PTable<String, Double> avgs = remoteAddrResponseSize.parallelDo(calulateAverage, Writables.tableOf(Writables.strings(), Writables.doubles()));
		pipeline.writeTextFile(avgs.top(100), args[1]);
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}

	MapFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>> calulateAverage = new MapFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>>() {
		@Override
		public Pair<String, Double> map(Pair<String, Pair<Long, Long>> arg) {
			Pair<Long, Long> sumCount = arg.second();
			double avg = 0;
			if (sumCount.second() > 0) {
				avg = (double) sumCount.first() / (double) sumCount.second();
			}
			return Pair.of(arg.first(), avg);
		}
	};

	DoFn<String, Pair<String, Pair<Long, Long>>> extractResponseSize = new DoFn<String, Pair<String, Pair<Long, Long>>>() {
		transient Pattern pattern;

		public void initialize() {
			pattern = Pattern.compile(Constants.logRegex);
		}

		public void process(String line,
				Emitter<Pair<String, Pair<Long, Long>>> emitter) {
			Matcher matcher = pattern.matcher(line);
			if (matcher.matches()) {
				try {
					Long responseSize = Long.parseLong(matcher.group(7));
					Pair<Long, Long> sumCount = Pair.of(responseSize, 1L);
					String remoteAddr = matcher.group(1);
					emitter.emit(Pair.of(remoteAddr, sumCount));
				} catch (NumberFormatException e) {
					this.getCounter(Constants.COUNTERS.CORRUPT_SIZE).increment(1);
				}
			} else {
				this.getCounter(Constants.COUNTERS.NO_MATCH).increment(1);
			}
		}
	};

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new AverageBytesByIP(), args);
		System.exit(result);
	}

}
