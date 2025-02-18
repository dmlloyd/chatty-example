package io.github.dmlloyd.example.chatty;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.locks.LockSupport;

final class ChattyInputStream extends InputStream {
    private final ReadableByteChannel channel;
    private final SelectionKey key;
    private final ByteBuffer buffer =  ByteBuffer.allocateDirect(8192);

    ChattyInputStream(final ReadableByteChannel channel, final SelectionKey key) {
        this.channel = channel;
        this.key = key;
        buffer.position(0);
    }

    public int read() throws IOException {
        if (! buffer.hasRemaining()) {
            int res = fill();
            if (res == -1) {
                return -1;
            }
        }
        return Byte.toUnsignedInt(buffer.get());
    }

    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (! buffer.hasRemaining()) {
            int res = fill();
            if (res == -1) {
                return -1;
            }
        }
        int size = Math.min(buffer.remaining(), len);
        buffer.get(b, off, size);
        return size;
    }

    int fill() throws IOException {
        buffer.compact();
        try {
            int res = channel.read(buffer);
            while (res == 0) {
                awaitReadable();
                res = channel.read(buffer);
            }
            return res;
        } finally {
            buffer.flip();
        }
    }

    private void awaitReadable() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException();
        }
        Thread[] threads = (Thread[]) key.attachment();
        threads[Chatty.IDX_READ] = Thread.currentThread();
        key.interestOpsOr(SelectionKey.OP_READ);
        // todo: ping the selector thread nicely...?
        key.selector().wakeup();
        LockSupport.park(this);
        key.interestOpsAnd(~SelectionKey.OP_READ);
        threads[Chatty.IDX_READ] = null;
    }

    public int available() {
        return buffer.remaining();
    }

    public void close() throws IOException {
        // no half-closed state here!!
        channel.close();
        key.selector().wakeup();
        buffer.clear().limit(0);
    }
}
