package com.ctriposs.sdb.datafile;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctriposs.sdb.table.AbstractMapTable;
import com.ctriposs.sdb.utils.FileUtil;
import com.google.common.base.Preconditions;

public class DataFileManager implements Closeable {
	
	static final Logger log = LoggerFactory.getLogger(DataFileManager.class);
	
	public final static String DATA_FILE_SUFFIX = ".data";
	public final static int DATA_FILE_SIZE = 128 * 1024 * 1024; // 128M
	public final static int DATA_FILE_WITH_METADATA_SIZE = DATA_FILE_SIZE + 1;
	
	private List<Short> freeList = new ArrayList<Short>();
	
	private ArrayList<DataFileEntry> dataFileEntries = new ArrayList<DataFileEntry>();
	private short size = 0;
	
	private short shard;
	private String dir;
	private boolean closed = false;
	
	public DataFileManager(String dir, short shard) throws IOException {
		this.dir = dir;
		if (!this.dir.endsWith(File.separator)) {
			this.dir += File.separator;
		}
		this.shard = shard;
		
		this.loadExistingDataFiles();
	}
	
	private void loadExistingDataFiles() throws IOException {
		File dirFile = new File(dir);
		if (!dirFile.exists())  {
			dirFile.mkdirs();
		}
		String fileNames[] = dirFile.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String filename) {
				if (!filename.endsWith(AbstractMapTable.DATA_FILE_SUFFIX)) return false;
				if (!filename.startsWith(String.valueOf(shard))) return false;
				return true;
			}
			
		});
		
		// new DB
		if (fileNames == null || fileNames.length == 0) {
			return;
		}
		
		PriorityQueue<DataFileEntry> pq = new PriorityQueue<DataFileEntry>();
		for(String orginalFileName : fileNames) {
			int dotIndex = orginalFileName.lastIndexOf(".");
			String fileName = orginalFileName;
			if (dotIndex > 0) {
				fileName = orginalFileName.substring(0, dotIndex);
			}
			String[] parts = fileName.split("-");
			Preconditions.checkArgument(parts != null && parts.length == 2, "on-disk data file names corrupted!");
			short index = Short.parseShort(parts[1]);
			RandomAccessFile dataRaf = new RandomAccessFile(fileName, "rw");
			FileChannel dataFileChannel = dataRaf.getChannel();
			dataRaf.close();
			ByteBuffer buf = ByteBuffer.allocate(1);
			dataFileChannel.read(buf, DATA_FILE_SIZE);
			if (buf.get(0) == (byte)0) { // add to free list
				freeList.add(index);
			}
			DataFileEntry dataFileEntry = new DataFileEntry();
			dataFileEntry.fileFullName = this.dir + orginalFileName;
			dataFileEntry.setDataFileChannel(dataFileChannel);
			dataFileEntry.setIndex(index);
			pq.add(dataFileEntry);
		}
		
		while(pq.size() > 0) {
			DataFileEntry dataFileEntry = pq.poll();
			Preconditions.checkArgument(dataFileEntry.getIndex() == size, "on-disk data file index corrupted!");
			dataFileEntries.add(dataFileEntry);
			size++;
		}
	}
	
	public DataFileEntry getDataFileByIndex(short index) {
		return dataFileEntries.get(index);	
	}
	
	public void releaseDataFileByIndex(short index) throws IOException {
		DataFileEntry dataFileEntry = dataFileEntries.get(index);
		freeList.add(index);
		FileChannel dataChannel = dataFileEntry.getDataFileChannel();
		// mark free
		dataChannel.write(ByteBuffer.wrap(new byte[] {0}), DATA_FILE_SIZE);
	}
	
	public synchronized DataFileEntry acquireDataFile() throws IOException {
		if (freeList.size() == 0) {
			String fileFullName = this.dir + shard + "-" + size + AbstractMapTable.DATA_FILE_SUFFIX;
			RandomAccessFile dataRaf = new RandomAccessFile(fileFullName, "rw");
			dataRaf.setLength(DATA_FILE_WITH_METADATA_SIZE);
			FileChannel dataChannel = dataRaf.getChannel();
			dataRaf.close();
			// mark in use
			dataChannel.write(ByteBuffer.wrap(new byte[] {1}), DATA_FILE_SIZE);
			DataFileEntry dataFileEntry = new DataFileEntry();
			dataFileEntry.fileFullName = fileFullName;
			dataFileEntry.setIndex(size);
			dataFileEntry.setDataFileChannel(dataChannel);
			// enlarge
			if (dataFileEntries.size() <= size) {
				for(int i = 0; i < 10; i++) {
					dataFileEntries.add(null);
				}
			}
			dataFileEntries.add(size, dataFileEntry);
			size++;
			return dataFileEntry;
		} else {
			short index = freeList.remove(0);
			DataFileEntry dataFileEntry = dataFileEntries.get(index);
			FileChannel dataChannel = dataFileEntry.getDataFileChannel();
			// mark in use
			dataChannel.write(ByteBuffer.wrap(new byte[] {1}), DATA_FILE_SIZE);
			return dataFileEntry;
		}
	}

	@Override
	public void close() throws IOException {
		for(DataFileEntry entry : dataFileEntries) {
			entry.getDataFileChannel().close();
		}
		closed = true;
	}
	
	public void delete() {
		Preconditions.checkArgument(closed, "Can't delete not closed data file manager!");
		
		for(DataFileEntry entry : dataFileEntries) {
			if (!FileUtil.deleteFile(entry.fileFullName)) {
	    		log.warn("fail to delete data file " + entry.fileFullName + ", please delete it manully");
			}
		}
	}
}
