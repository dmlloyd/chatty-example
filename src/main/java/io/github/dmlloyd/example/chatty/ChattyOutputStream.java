package io.github.dmlloyd.example.chatty;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.LockSupport;

final class ChattyOutputStream extends OutputStream {
    private final WritableByteChannel channel;
    private final SelectionKey key;
    private final ByteBuffer buffer =  ByteBuffer.allocateDirect(8192);

    ChattyOutputStream(final WritableByteChannel channel, final SelectionKey key) {
        this.channel = channel;
        this.key = key;
    }

    public void write(final int b) throws IOException {
        if (! buffer.hasRemaining()) {
            flush();
        }
        buffer.put((byte) b);
        return;
    }

    public void write(final byte[] b, int off, int len) throws IOException {
        int rem;
        for (;;) {
            if (len == 0) {
                return;
            }
            rem = buffer.remaining();
            if (rem == 0) {
                flush();
            } else {
                int cnt = Math.min(len, rem);
                buffer.put(b, off, cnt);
                len -= cnt;
                off += cnt;
            }
        }
    }

    public void flush() throws IOException {
        flushBytes();
    }

    private int flushBytes() throws IOException {
        if (buffer.position() == 0) {
            return 0;
        }
        buffer.flip();
        try {
            int res = channel.write(buffer);
            while (res == 0) {
                awaitWritable();
                res = channel.write(buffer);
            }
            return res;
        } finally {
            buffer.compact();
        }
    }

    private void awaitWritable() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException();
        }
        Thread[] threads = (Thread[]) key.attachment();
        threads[Chatty.IDX_WRITE] = Thread.currentThread();
        key.interestOpsOr(SelectionKey.OP_WRITE);
        // todo: ping the selector thread nicely...?
        key.selector().wakeup();
        LockSupport.park(this);
        key.interestOpsAnd(~SelectionKey.OP_WRITE);
        threads[Chatty.IDX_WRITE] = null;
    }

    public void close() throws IOException {
        // no half-closed state here!!
        channel.close();
        key.selector().wakeup();
        buffer.clear().limit(0);
    }
}
