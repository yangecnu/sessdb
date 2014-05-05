package com.ctriposs.sdb.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.xerial.snappy.Snappy;

import com.ctriposs.sdb.utils.MMFUtil;
import com.google.common.base.Preconditions;

/**
 * In memory hashmap backed by memory mapped WAL(Write Ahead Log)
 * 
 * @author bulldog
 *
 */
public class HashMapTable extends AbstractMapTable {
	
	private AtomicBoolean immutable = new AtomicBoolean(true);

	private ConcurrentHashMap<ByteArrayWrapper, InMemIndex> hashMap;
	protected MappedByteBuffer dataMappedByteBuffer;
	protected MappedByteBuffer indexMappedByteBuffer;
	
	private boolean compressionEnabled = true;
	
	// Create new
	public HashMapTable(String dir, int level, long createdTime)
			throws IOException {
		super(dir, level, createdTime);
		mapIndexAndDataFiles();
		initToAppendIndexAndOffset();
	}
	
	public HashMapTable(String dir, short shard, int level, long createdTime)
			throws IOException {
		super(dir, shard, level, createdTime);
		mapIndexAndDataFiles();
		initToAppendIndexAndOffset();
	}
	
	// Load existing
	public HashMapTable(String dir, String fileName)
			throws IOException {
		super(dir, fileName);
		mapIndexAndDataFiles();
		initToAppendIndexAndOffset();
	}
	
	public void setCompressionEnabled(boolean enabled) {
		this.compressionEnabled = enabled;
	}
	
	private void mapIndexAndDataFiles() throws IOException {
		indexMappedByteBuffer = this.indexChannel.map(MapMode.READ_WRITE, 0, this.indexChannel.size());
		dataMappedByteBuffer = this.dataChannel.map(MapMode.READ_WRITE, 0, this.dataChannel.size());
	}
	
	public Set<Map.Entry<ByteArrayWrapper, InMemIndex>> getEntrySet() {
		return this.hashMap.entrySet();
	}
	
	private void initToAppendIndexAndOffset() throws IOException {
		this.hashMap = new ConcurrentHashMap<ByteArrayWrapper, InMemIndex>(INIT_INDEX_ITEMS_PER_TABLE);
		toAppendIndex = new AtomicInteger(0);
		toAppendDataFileOffset = new AtomicLong(0);
		int index = 0;
		IMapEntry mapEntry = new MMFMapEntryImpl(index, this.indexMappedByteBuffer, this.dataMappedByteBuffer);
		while(mapEntry.isInUse()) {
			toAppendIndex.incrementAndGet();
			toAppendDataFileOffset.set(((MMFMapEntryImpl)mapEntry).getItemOffsetInDataFile());
			InMemIndex inMemIndex = new InMemIndex(index);
			// populate in memory skip list
			hashMap.put(new ByteArrayWrapper(mapEntry.getKey()), inMemIndex);
			index++;
			mapEntry = new MMFMapEntryImpl(index, this.indexMappedByteBuffer, this.dataMappedByteBuffer);
		}
	}
	
	// for testing
	public IMapEntry appendNew(byte[] key, byte[] value, long timeToLive, long createdTime) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		return this.appendNew(key, Arrays.hashCode(key), value, timeToLive, createdTime, false, false);
	}
	
	private IMapEntry appendTombstone(byte[] key) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		return this.appendNew(key, Arrays.hashCode(key), new byte[] {0}, NO_TIMEOUT, System.currentTimeMillis(), true, false);
	}
	
	private IMapEntry appendNewCompressed(byte[] key, byte[] value, long timeToLive, long createdTime) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		return this.appendNew(key, Arrays.hashCode(key), value, timeToLive, createdTime, false, true);
	}
	
	private IMapEntry appendNew(byte[] key, int keyHash, byte[] value, long timeToLive, long createdTime, boolean markDelete, boolean compressed) throws IOException {
		appendLock.lock();
		try {
			
			if (toAppendIndex.get() == INIT_INDEX_ITEMS_PER_TABLE) { // index overflow
				return null;
			}
			int dataLength = key.length + value.length;
			if (toAppendDataFileOffset.get() + dataLength > INIT_DATA_FILE_SIZE) { // data overflow
				return null;
			}
			
			// write index metadata
			indexBuf.clear();
			indexBuf.putLong(IMapEntry.INDEX_ITEM_IN_DATA_FILE_OFFSET_OFFSET, toAppendDataFileOffset.get());
			indexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET, key.length);
			indexBuf.putInt(IMapEntry.INDEX_ITEM_VALUE_LENGTH_OFFSET, value.length);
			indexBuf.putLong(IMapEntry.INDEX_ITEM_TIME_TO_LIVE_OFFSET, timeToLive);
			indexBuf.putLong(IMapEntry.INDEX_ITEM_CREATED_TIME_OFFSET, createdTime);
			indexBuf.putInt(IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET, keyHash);
			byte status = 1; // mark in use
			if (markDelete) {
				status = (byte) (status + 2); // binary 11
			}
			if (compressed && !markDelete) {
				status = (byte) (status + 4);
			}
			indexBuf.put(IMapEntry.INDEX_ITEM_STATUS, status); // mark in use
			
			int offsetInIndexFile = INDEX_ITEM_LENGTH * toAppendIndex.get();
			this.indexMappedByteBuffer.position(offsetInIndexFile);
			//indexBuf.rewind();
			this.indexMappedByteBuffer.put(indexBuf);
			
			// write key/value
			this.dataMappedByteBuffer.position((int)toAppendDataFileOffset.get());
			this.dataMappedByteBuffer.put(ByteBuffer.wrap(key));
			this.dataMappedByteBuffer.position((int)toAppendDataFileOffset.get() + key.length);
			this.dataMappedByteBuffer.put(ByteBuffer.wrap(value));
			
			int appendedIndex = toAppendIndex.get();
			this.hashMap.put(new ByteArrayWrapper(key), new InMemIndex(appendedIndex));
			
			// commit/update offset & index
			toAppendDataFileOffset.addAndGet(dataLength);
			toAppendIndex.incrementAndGet();
			
			return new MMFMapEntryImpl(appendedIndex, this.indexMappedByteBuffer, this.dataMappedByteBuffer);
		}
		finally {
			appendLock.unlock();
		}
	}

	@Override
	public IMapEntry getMapEntry(int index) {
		Preconditions.checkArgument(index >= 0, "index (%s) must be equal to or greater than 0", index);
		Preconditions.checkArgument(!isEmpty(), "Can't get map entry since the map is empty");
		return new MMFMapEntryImpl(index, this.indexMappedByteBuffer, this.dataMappedByteBuffer);
	}
	
	@Override
	public GetResult get(byte[] key) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		GetResult result = new GetResult();
		InMemIndex inMemIndex = this.hashMap.get(new ByteArrayWrapper(key));
		if (inMemIndex == null) return result;
		synchronized(inMemIndex) {
			IMapEntry mapEntry = this.getMapEntry(inMemIndex.getIndex());
			if (mapEntry.isCompressed()) {
				result.setValue(Snappy.uncompress(mapEntry.getValue()));
			} else {
				result.setValue(mapEntry.getValue());
			}
			if (mapEntry.isDeleted()) {
				result.setDeleted(true);
				return result;
			}
			if (mapEntry.isExpired()) {
				result.setExpired(true);
				return result;
			}
			result.setLevel(this.getLevel());
			result.setTimeToLive(mapEntry.getTimeToLive());
			result.setCreatedTime(mapEntry.getCreatedTime());
			
			return result;
		}
	}
	
	public void markImmutable(boolean immutable) {
		this.immutable.set(immutable);	
	}
	
	public boolean isImmutable() {
		return this.immutable.get();
	}

	public boolean put(byte[] key, byte[] value, long timeToLive, long createdTime, boolean isDelete) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		
		IMapEntry mapEntry = null;
		if (isDelete) {
			// make a tombstone
			mapEntry = this.appendTombstone(key);
		} else {
			mapEntry = this.compressionEnabled ? 
					this.appendNewCompressed(key, Snappy.compress(value), timeToLive, createdTime) : this.appendNew(key, value, timeToLive, createdTime);
		}

		if (mapEntry == null) { // no space
			return false;
		}
		
		return true;
	}
	
	public void put(byte[] key, byte[] value, long timeToLive, long createdTime) throws IOException {
		this.put(key, value, timeToLive, createdTime, false);
	}
	
	public void delete(byte[] key) throws IOException {
		this.appendTombstone(key);
	}
	
	public int getRealSize() {
		return this.hashMap.size();
	}
	
	@Override
	public void close() throws IOException {
		MMFUtil.unmap(indexMappedByteBuffer);
		MMFUtil.unmap(dataMappedByteBuffer);
		super.close();
	}
}
