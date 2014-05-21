package com.ctriposs.sdb.benchmark;

import java.io.File;
import java.io.IOException;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Hasher;

import com.ctriposs.sdb.utils.FileUtil;

public class MdbBenchmark extends DbBenchmark {

	private final String databaseDir_;
	private static String DATABASE_NAME = "mapdb";
	private static String MAP_NAME = "testMap";
	private DB mdb_;
	private HTreeMap<byte[], byte[]> map_;

	public MdbBenchmark(String[] commands) {
		super(commands);
		databaseDir_ = this.getBaseDir() + "mdb/unit/benchmark";
		dbName = "Mapdb";
		version = "1.0.1";
	}

	@Override
	public void open() {
		File file = new File(databaseDir_);
		if (!file.exists()) {
			file.mkdirs();
		}
		try {
			File dbFile = File.createTempFile(DATABASE_NAME, "db", file);
			DBMaker maker = DBMaker
				    .newFileDB(dbFile)
				    .transactionDisable()
				    .asyncWriteQueueSize(5000)
				    .mmapFileEnableIfSupported();
			if(this.getCompressMode()){
			    maker.compressionEnable();
			}
			mdb_ = maker.make();
			map_ = mdb_.createHashMap(MAP_NAME)
				.hasher(Hasher.BYTE_ARRAY)
				.makeOrGet();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		mdb_.commit();
		mdb_.close();
	}

	@Override
	public void destroyDb() {
		if (mdb_ != null) {
			mdb_.delete(MAP_NAME);
			mdb_.close();
			FileUtil.deleteDirectory(new File(databaseDir_));
			map_ = null;
		}
	}

	public static void main(String[] args) throws Exception {
		new MdbBenchmark(args).run();
	}

}
