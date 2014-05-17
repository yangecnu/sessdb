package com.ctriposs.sdb.datafile;

import java.nio.channels.FileChannel;

public class DataFileEntry implements Comparable<DataFileEntry> {
	
	private short index;
	private FileChannel dataFileChannel;
	String fileFullName;
	//private boolean memoryMapped;
	
	public short getIndex() {
		return index;
	}
	public void setIndex(short index) {
		this.index = index;
	}
	public FileChannel getDataFileChannel() {
		return dataFileChannel;
	}
	public void setDataFileChannel(FileChannel dataFileChannel) {
		this.dataFileChannel = dataFileChannel;
	}
	@Override
	public int compareTo(DataFileEntry o) {
		if (this.index < o.index) return -1;
		else if (this.index > o.index) return 1;
		return 0;
	}
}
