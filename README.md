leveldb-java
============

java version of leveldb.

This is a pure java version of LevelDb (http://code.google.com/p/leveldb-java/) which is almost implemented according to the original version with some modifications for Java. Currently this project has past most test cases in the package "com.leveldb.tests" and you can use this leveldb according to the test cases. For example, to Put/Delete/Get data to leveldb

public void testPutDeleteGet() {

     ASSERT_OK(db.Put(new WriteOptions(), new Slice("foo"), new Slice("v1")));
     
     assertTrue("v1".compareTo(Get("foo").toString()) == 0);
     
     ASSERT_OK(db .Put(new WriteOptions(), new Slice("foo"), new Slice("v2")));
     
     assertTrue("v2".compareTo(Get("foo").toString()) == 0);
     
     ASSERT_OK(db.Delete(new WriteOptions(), new Slice("foo")));
     
     String g = Get(new Slice("foo")).toString();
     
     assertTrue("NOT_FOUND".compareTo(g) == 0);
     
     assertTrue(Close());
     
}

About the auther: Lu, Wei, Software enginner @Hangzhou, China. Email: luwei114 at 163 dot com

