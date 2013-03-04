package com.cloudera.castagna.crunch;

public class Constants {

	static final String logRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) (\\d+) \"([^\"]+)\" <(.+?)> <(.+?)> \"(.+?)\" (\\d+)";

	static enum COUNTERS {
		NO_MATCH, 
		CORRUPT_SIZE
	}

}
