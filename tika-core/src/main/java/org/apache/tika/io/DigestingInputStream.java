package org.apache.tika.io;

import org.apache.tika.parser.DigestingParser;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

public class DigestingInputStream extends ProxyInputStream {
    protected static final int SKIP_BUFFER_LENGTH = 8192;
    private final MessageDigest digest;

    /**
     * Last position that was digested.
     * Necessary to handle mark/reset properly.
     */
    private long lastDigestedPosition = -1;

    /**
     * Current read position within this stream.
     */
    private long position = 0;

    /**
     * Marked position, or -1 if there is no current mark.
     */
    private long mark = -1;
    /**
     * Constructs a new ProxyInputStream.
     *
     * @param proxy the InputStream to delegate to
     */
    public DigestingInputStream(InputStream proxy, MessageDigest digest) {
        super(proxy);
        this.digest = digest;

    }

    @Override
    public int read() throws IOException {
        int i = super.read();
        if (i == -1) {
            return i;
        }
        position++;
        if (lastDigestedPosition < position) {
            digest.update((byte) i);
            lastDigestedPosition =  position;
        }

        return i;
    }

    @Override
    public int read(byte[] arr) throws IOException {
        int r = super.read(arr);
        if (r <= 0) {
            return r;
        }
        //if the end of what was read was entirely before lastDigestedPosition
        if (position+r < lastDigestedPosition) {
            position += r;
            return r;
        }

        long start = (lastDigestedPosition == -1) ? 0 : lastDigestedPosition-position;
        position += r;
        if (start > -1 && start < r) {
            System.out.println(start + " : " + r + " : " + (r-start));
            digest.update(arr, (int)start, r-(int)start);
            lastDigestedPosition = position;
        }
        return r;
    }

    @Override
    public int read(byte[] arr, int off, int len) throws IOException {
        System.out.println(off + " : " + len);
        int r = super.read(arr, off, len);
        if (r == -1) {
            return r;
        }
        long start = (lastDigestedPosition == -1 ||
                lastDigestedPosition == position) ? off :
                off+lastDigestedPosition-position;
        long newLen = r-start+off;
        newLen = (newLen+start > arr.length) ? arr.length-start : newLen;
        System.out.println(off + " : " + start + " ::: " + r + " : " + newLen +" :::: "  + lastDigestedPosition + " : "+ position);
        position += r;
        if (position > lastDigestedPosition) {
            digest.update(arr, (int)start, (int)newLen);
            lastDigestedPosition = position;
        }
        return r;
    }

    @Override
    public void mark(int readlimit) {
        super.mark(readlimit);
        mark = position;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        position = mark;
        mark = -1;
    }


    /**
     * Acts like it skips, but actually
     * reads to correctly calculate digest during skip
     *
     * @param ln the number of bytes to "skip"
     * @return the actual number of bytes "skipped"
     * @throws IOException if an I/O error occurs
     */
    @Override
    public long skip(long ln) throws IOException {
        try {
            if (ln < SKIP_BUFFER_LENGTH) {
                byte[] buffer = new byte[(int)ln];
                int r = read(buffer);
                return r;
            }
            byte[] buffer = new byte[SKIP_BUFFER_LENGTH];
            long total = 0;
            int r = read(buffer);
            while (r > -1) {
                total += r;
                r = read(buffer);
            }
            return total;

        } catch (IOException e) {
            handleIOException(e);
            return 0;
        }
    }

    public MessageDigest getMessageDigest() {
        return digest;
    }
}
