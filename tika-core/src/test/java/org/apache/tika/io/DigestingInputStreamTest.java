package org.apache.tika.io;

import org.apache.tika.mime.MimeType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DigestingInputStreamTest {

    public static byte[] BYTES;
    public static byte[] TRUE_DIGEST;
    public static MessageDigest MESSAGE_DIGEST;
    final static int INPUT_STREAM_LENGTH = DigestingInputStream.SKIP_BUFFER_LENGTH*3;
    static int SEED = new Random().nextInt();
    static Random RANDOM = new Random(SEED);
    private DigestingInputStream newDigestingInputStream;

    @BeforeClass
    public static void setUp() throws NoSuchAlgorithmException {
        BYTES = new byte[INPUT_STREAM_LENGTH];
        for (int i = 0; i < INPUT_STREAM_LENGTH; i++) {
            BYTES[i] = (byte)RANDOM.nextInt(256);
        }
        MESSAGE_DIGEST = MessageDigest.getInstance("md5");
        TRUE_DIGEST = MESSAGE_DIGEST.digest(BYTES);
    }

    public static MessageDigest getDigest() {
        MESSAGE_DIGEST.reset();
        return MESSAGE_DIGEST;
    }

    @Test
    public void testNotEquals() throws IOException {
        byte[] arr1 = new byte[10];
        byte[] arr2 = new byte[10];
        for (int i = 0; i < 10; i++) {
            arr1[i] = (byte)i;
            arr2[i] = (byte)i;
        }
        boolean ex = false;
        try {
            assertByteArraysNotEqual(arr1, arr2);
        } catch (AssertionError e) {
            ex = true;
        }
        assertTrue(ex);
    }

    @Test
    public void javaFailsTest() throws IOException {
        //as soon as Java stops failing this test,
        //move back to Java!!!

        //step 1, prove that the basic read works
        MessageDigest digest = getDigest();
        InputStream is = new BufferedInputStream(new ByteArrayInputStream(BYTES));
        DigestInputStream dis = new DigestInputStream(is, digest);
        while (dis.read() != -1) { }
        assertArrayEquals(TRUE_DIGEST, digest.digest());

        //step 2, prove that skip does not work
        digest = getDigest();
        is = new BufferedInputStream(new ByteArrayInputStream(BYTES));
        dis = new DigestInputStream(is, digest);
        dis.skip(10);
        while (dis.read() != -1) { }
        assertByteArraysNotEqual(TRUE_DIGEST, digest.digest());

        //step 3, prove that reset does not work
        digest = getDigest();
        is = new BufferedInputStream(new ByteArrayInputStream(BYTES));
        dis = new DigestInputStream(is, digest);
        dis.mark(1);
        dis.read();
        dis.reset();
        while (dis.read() != -1) { }
        assertByteArraysNotEqual(TRUE_DIGEST, digest.digest());

    }

    @Test
    public void basicSkipTest() throws IOException {
        //step 1, basic read works
        DigestingInputStream dis = getNewDigestingInputStream();
        while (dis.read() != -1) { }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //skip < length works
        dis = getNewDigestingInputStream();
        assertEquals(10L, dis.skip(10));
        while (dis.read() != -1) { }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //skip > length works
        dis = getNewDigestingInputStream();
        assertEquals(INPUT_STREAM_LENGTH, dis.skip(INPUT_STREAM_LENGTH + 100));
        while (dis.read() != -1) { }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //test that proxied stream really is read and bytes are "skipped"
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        dis = getNewDigestingInputStream();
        //read some bytes
        for (int i = 0; i < 10; i++) {
            os.write(dis.read());
        }
        long skipped = dis.skip(100);
        assertEquals(100L, skipped);
        int b = dis.read();
        while (b != -1) {
            os.write(b);
            b = dis.read();
        }
        assertEquals(os.toByteArray().length, INPUT_STREAM_LENGTH-100);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void basicResetTest() throws IOException {
        //test basic mark/reset
        DigestingInputStream dis = getNewDigestingInputStream();
        dis.mark(10);
        for (int i = 0; i < 5; i++ ) {
            dis.read();
        }
        dis.reset();
        while (dis.read() != -1) {}
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //read some first
        dis = getNewDigestingInputStream();
        for (int i = 0; i < 5; i++ ) {
            dis.read();
        }
        dis.mark(10);
        for (int i = 0; i < 5; i++ ) {
            dis.read();
        }
        dis.reset();
        while (dis.read() != -1) {}
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //read beyond mark
        dis = getNewDigestingInputStream();
        dis.mark(10);
        for (int i = 0; i < 15; i++ ) {
            dis.read();
        }
        dis.reset();
        while (dis.read() != -1) {}
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void testSkipArrayRead() throws IOException {
        //step 1, basic read works
        DigestingInputStream dis = getNewDigestingInputStream();
        byte[] buffer = new byte[INPUT_STREAM_LENGTH];
        int moreRead = 0;
        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertArrayEquals(BYTES, buffer);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
        assertEquals(0, moreRead);

        //step 1b, basic read buffer beyond stream length
        dis = getNewDigestingInputStream();
        buffer = new byte[INPUT_STREAM_LENGTH+100];
        moreRead = 0;
        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertByteArrayEquals(BYTES, buffer, 0, INPUT_STREAM_LENGTH);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
        assertEquals(0, moreRead);

        //skip < length works
        dis = getNewDigestingInputStream();
        assertEquals(10L, dis.skip(10));
        buffer = new byte[INPUT_STREAM_LENGTH-10];
        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
        assertEquals(0, moreRead);

        //skip > length works
        dis = getNewDigestingInputStream();
        assertEquals(INPUT_STREAM_LENGTH, dis.skip(INPUT_STREAM_LENGTH + 100));
        buffer = new byte[10];
        byte[] uninit = new byte[10];
        System.arraycopy(buffer, 0, uninit, 0, 10);

        dis.read(buffer);
        moreRead = 0;
        while (dis.read() != -1) {
            moreRead++;
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
        assertEquals(0, moreRead);
        assertArrayEquals(uninit, buffer);//nothing has changed
    }

    @Test
    public void testArrayRead() throws IOException {
        //buffer exact length
        DigestingInputStream dis = getNewDigestingInputStream();
        byte[] buffer = new byte[INPUT_STREAM_LENGTH];
        int moreRead = 0;

        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertEquals(0, moreRead);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //buffer greater than necessary
        dis = getNewDigestingInputStream();
        buffer = new byte[INPUT_STREAM_LENGTH + 10];
        moreRead = 0;
        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertEquals(0, moreRead);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //now try reading some before doing array read
        dis = getNewDigestingInputStream();
        buffer = new byte[INPUT_STREAM_LENGTH - 10];
        moreRead = 0;
        //read some stuff
        for (int i = 0; i < 10; i++) {
            dis.read();
        }

        dis.read(buffer);
        while (dis.read() != -1) {
            moreRead++;
        }
        assertEquals(0, moreRead);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        //now try reading random lengths
        dis = getNewDigestingInputStream();

        buffer = new byte[RANDOM.nextInt(50)];
        int r = dis.read(buffer);
        while (r > -1) {
            buffer = new byte[RANDOM.nextInt(2000)];
            r = dis.read(buffer);
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void testMarkResetArrayRead() throws IOException {
        DigestingInputStream dis = getNewDigestingInputStream();
        //read something
        for (int i = 0; i < 10; i++) {
            dis.read();
        }
        dis.mark(100);
        byte[] buffer = new byte[50];
        dis.read(buffer);
        dis.reset();
        buffer = new byte[INPUT_STREAM_LENGTH];//longer than necessary
        dis.read(buffer);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());

        dis = getNewDigestingInputStream();
        //read something
        for (int i = 0; i < 10; i++) {
            dis.read();
        }
        dis.mark(100);
        buffer = new byte[10];
        for (int i = 0; i < 4; i++) {
            dis.read(buffer);
        }
        dis.reset();
        buffer = new byte[15];
        for (int i = 0; i < 6; i++) {
            dis.read(buffer);
        }
        buffer = new byte[INPUT_STREAM_LENGTH];//longer than necessary
        dis.read(buffer);
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void testArrayOffset() throws Exception {
        DigestingInputStream dis = getNewDigestingInputStream();
        byte[] buffer = new byte[1000];
        int start = RANDOM.nextInt(1000);
        int len = RANDOM.nextInt(100);
        len = (start+len > 1000) ? 1000-start : len;//keep len from going beyond array

        int r = dis.read(buffer, start, len);
        while (r > -1) {
            start = RANDOM.nextInt(1000);
            len = RANDOM.nextInt(100);
            len = (start+len > 1000) ? 1000-start : len;//keep len from going beyond array
            r = dis.read(buffer, start, len);
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void testResetArrayOffset() throws IOException {
        DigestingInputStream dis = getNewDigestingInputStream();
        byte[] buffer = new byte[100];
        dis.mark(10000);
        for (int i = 0; i < 5; i++) {
            dis.read(buffer, 12, 10);
        }
        dis.reset();
        int r = dis.read(buffer, 15, 15);
        while (r > -1) {
            r = dis.read(buffer, 15, 15);
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
    }

    @Test
    public void testRandResetArrayOffset() throws IOException {
  /*      //pick up here
        DigestingInputStream dis = getNewDigestingInputStream();
        dis.mark(10000);
        for (int i = 0; i < 5; i++) {
            dis.read(buffer, 12, 10);
        }
        dis.reset();
        int r = dis.read(buffer, 15, 15);
        while (r > -1) {
            r = dis.read(buffer, 15, 15);
        }
        assertArrayEquals(TRUE_DIGEST, dis.getMessageDigest().digest());
*/
    }

    @Test
    public void underlyingFileInputStreamTest() throws IOException {

        File testFile = new File(MimeType.class.getResource("datamatrix.png").getFile());
        InputStream is = TikaInputStream.get(testFile);
        MessageDigest digest = getDigest();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        IOUtils.copy(is, os);
        is.close();
        byte[] truth = digest.digest(os.toByteArray());
        System.out.println(os.toByteArray().length);
        DigestingInputStream dis = getNewDigestingInputStreamFromFile();
        byte[] buffer = new byte[64];
        while (dis.read() != -1) {}
        dis.close();
        assertArrayEquals(truth, dis.getMessageDigest().digest());
    }


    public DigestingInputStream getNewDigestingInputStream() {
        InputStream is = new BufferedInputStream(new ByteArrayInputStream(BYTES));
        return new DigestingInputStream(is, getDigest());
    }

    public DigestingInputStream getNewDigestingInputStreamFromFile() throws IOException {
        File testFile = new File(MimeType.class.getResource("datamatrix.png").getFile());
        InputStream is = TikaInputStream.get(testFile);
        return new DigestingInputStream(is, getDigest());
    }

    public static void assertByteArraysNotEqual(byte[] arr1, byte[] arr2) {
        if (arr1.length != arr2.length) {
            return;
        }

        for (int i = 0; i < arr1.length; i++) {
            if (arr1[i] != arr2[i]) {
                return;
            };
        }
        throw new AssertionError("Byte arrays are equal");
    }

    public static void assertByteArrayEquals(byte[] a, byte[] b, int off, int len) {
        assertTrue(off+len <= a.length);
        assertTrue(off+len <= b.length);
        for (int i = off; i < off+len; i++) {
            assertEquals(a[i], b[i]);
        }
    }

}
