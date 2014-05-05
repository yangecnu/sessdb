package com.ctriposs.sdb.table;

public class GetResult {
	
	private byte[] value;
	
	private boolean deleted;
	
	private boolean expired;
	
	private boolean immutable;
	
	private long timeToLive;

	public boolean isFound() {
		return value != null;
	}
	
	public boolean isDeleted() {
		return deleted;
	}
	
	public boolean isExpired() {
		return expired;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	void setValue(byte[] value) {
		this.value = value;
	}
	
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
	
	void setExpired(boolean expired) {
		this.expired = expired;
	}

	public boolean isImmutable() {
		return immutable;
	}

	void setImmutable(boolean immutable) {
		this.immutable = immutable;
	}

	public long getTimeToLive() {
		return timeToLive;
	}

	void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}
}
