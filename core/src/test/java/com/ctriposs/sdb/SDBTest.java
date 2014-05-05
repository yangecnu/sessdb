package com.ctriposs.sdb;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.sdb.SDB;
import com.ctriposs.sdb.utils.TestUtil;

public class SDBTest {
	
    // You can set the STRESS_FACTOR system property to make the tests run more iterations.
    public static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1"));
	
	private static String testDir = TestUtil.TEST_BASE_DIR + "sdb/unit/ydb_test";
	
	private SDB db;
	
	@Test
	public void testDB() {
		db = new SDB(testDir);
		
		Set<String> rndStringSet = new HashSet<String>();
        for(int i=0; i < 2000000*STRESS_FACTOR; i++)
        {
        	String rndString = TestUtil.randomString(64);
        	rndStringSet.add(rndString);
            db.put(rndString.getBytes(), rndString.getBytes());
            if ((i%50000)==0 && i!=0 ) {
                System.out.println(i+" rows written");
            }
        }
        
        for(String rndString : rndStringSet) {
        	byte[] value = db.get(rndString.getBytes());
        	assertNotNull(value);
        	assertEquals(rndString, new String(value));
        }
        
        // delete
        for(String rndString : rndStringSet) {
        	db.delete(rndString.getBytes());
        }
        
        for(String rndString : rndStringSet) {
        	byte[] value = db.get(rndString.getBytes());
        	assertNull(value);
        }
	}
	
	@After
	public void clear() throws IOException {
		if (db != null) {
			db.close();
			db.destory();
		}
	}
}
