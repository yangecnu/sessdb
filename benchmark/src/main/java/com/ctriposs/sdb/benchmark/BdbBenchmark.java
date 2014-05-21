package com.ctriposs.sdb.benchmark;

import java.io.File;

import com.ctriposs.sdb.utils.FileUtil;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

public class BdbBenchmark extends DbBenchmark {

	private final String databaseDir_;
	private static String DATABASE_NAME = "sample";
	private Database bdb_;
	private Environment env_;

	public BdbBenchmark(String[] commands) throws Exception {
		super(commands);
		databaseDir_ = this.getBaseDir() + "bdb/unit/benchmark";
		dbName = "Berkeleydb";
		version = "5.0.73";
	}

	@Override
	public void put(byte[] key, byte[] value) {
		try {
			bdb_.put(null, new DatabaseEntry(key), new DatabaseEntry(value));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

	}

	@Override
	public byte[] get(byte[] key) {
		DatabaseEntry value = new DatabaseEntry();
		try {
			bdb_.get(null, new DatabaseEntry(key), value, LockMode.DEFAULT);
			return value.getData();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void destroyDb() {
		if (bdb_ != null) {
			try {
				bdb_.close();
				env_.removeDatabase(null, BdbBenchmark.DATABASE_NAME);
				env_.close();
				FileUtil.deleteDirectory(new File(databaseDir_));
				bdb_ = null;
				env_ = null;

			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void open() {
		try {
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setCachePercent(90);

			File file = new File(databaseDir_);
			if (!file.exists()) {
				file.mkdirs();
			}
			env_ = new Environment(file, envConfig);
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			bdb_ = env_
					.openDatabase(null, BdbBenchmark.DATABASE_NAME, dbConfig);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			bdb_.close();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new BdbBenchmark(args).run();
	}

}
