package com.ctriposs.sdb;

public class Config {
	
	public static final Config SMALL = new Config().setShardNumber((short)1);
	public static final Config DEFAULT = new Config().setShardNumber((short)4);
	public static final Config BIG = new Config().setShardNumber((short)8);
	public static final Config LARGE = new Config().setShardNumber((short)16);
	public static final Config HUGE = new Config().setShardNumber((short)32);
	
	private short shardNumber = 4;
	
	private boolean compressionEnabled = true;
	
	public boolean isCompressionEnabled() {
		return compressionEnabled;
	}
	
	/**
	 * Important: shard number can't be changed for an existing DB.
	 * 
	 * @return shard number
	 */
	public short getShardNumber() {
		return shardNumber;
	}

	/**
	 * Enable snappy compression for value
	 * 
	 * @param compressionEnabled
	 * @return Level Cache configuration
	 */
	public Config setCompressionEnabled(boolean compressionEnabled) {
		this.compressionEnabled = compressionEnabled;
		return this;
	}
	
	private Config setShardNumber(short shardNumber) {
		this.shardNumber = shardNumber;
		return this;
	}

}
