package com.ctriposs.sdb.benchmark;

import java.io.IOException;

import com.ctriposs.sdb.Config;
import com.ctriposs.sdb.SDB;

public class SdbBenchmark extends DbBenchmark {
	private SDB sdb_;
	private String post_message_;
	private final String databaseDir_;

	public SdbBenchmark(String[] commands) throws Exception {
		super(commands);
		databaseDir_ = this.getBaseDir() + "sdb/unit/benchmark";
		dbName = "Sessdb";
		version = "test";
	}

	@Override
	public void open() {
	    	Config config = new Config();
	    	config.setCompressionEnabled(this.getCompressMode());
		sdb_ = new SDB(databaseDir_, config);
	}

	public void stop(String benchmark) {
		super.stop(benchmark);

		// if (FLAGS_histogram) {
		// System.out.printf("Microseconds per op:\n%s\n",
		// hist_.ToString().c_str());
		// }

		if (post_message_ != null) {
			System.out.printf("\n%s\n", post_message_);
			post_message_ = null;
		}
	}

	@Override
	public void put(byte[] key, byte[] value) {
		sdb_.put(key, value);
	}

	@Override
	public byte[] get(byte[] key) {
		return sdb_.get(key);
	}

	@Override
	public void destroyDb() {
		if (sdb_ != null) {
			try {
				sdb_.close();
				sdb_.destory();
				sdb_ = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		try {
			sdb_.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new SdbBenchmark(args).run();
	}
}
