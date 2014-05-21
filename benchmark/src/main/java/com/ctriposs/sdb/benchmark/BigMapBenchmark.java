package com.ctriposs.sdb.benchmark;

import java.io.File;
import java.io.IOException;

import com.ctriposs.bigmap.BigConcurrentHashMapImpl;
import com.ctriposs.bigmap.utils.FileUtil;

public class BigMapBenchmark extends DbBenchmark {

	private final String databaseDir_;
	private static String DATABASE_NAME = "sample";
	private BigConcurrentHashMapImpl map_;

	public BigMapBenchmark(String[] commands) {
		super(commands);
		databaseDir_ = this.getBaseDir() + "bitmap/unit/benchmark";
		dbName = "BigMap";
		version = "1.0.0";
	}

	@Override
	public void open() {
		try {
			map_ = new BigConcurrentHashMapImpl(databaseDir_, DATABASE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void put(byte[] key, byte[] value) {
		map_.put(key, value);

	}

	@Override
	public byte[] get(byte[] key) {
		return map_.get(key);
	}

	@Override
	public void close() {
		try {
			map_.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroyDb() {
		if (map_ != null) {
			try {
				map_.close();
				FileUtil.deleteDirectory(new File(databaseDir_));
				map_ = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new BigMapBenchmark(args).run();
	}

}
