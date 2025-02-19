package io.github.dmlloyd.example.chatty;

import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import org.jboss.threads.virtual.EventLoopThread;

final class ChattyServerSocket {
    private final Executor executor;
    private final ServerSocketChannel channel;
    private final SelectionKey key;

    ChattyServerSocket(final Executor executor, final ServerSocketChannel channel, final SelectionKey key) {
        this.executor = executor;
        this.channel = channel;
        this.key = key;
    }

    public void accept(BiConsumer<InputStream, OutputStream> handler) throws IOException {
        SocketChannel accepted = acceptBlocking();
        executor.execute(() -> {
            try {
                accepted.configureBlocking(false);
                SelectionKey key = accepted.register(this.key.selector(), 0, new Thread[2]);
                handler.accept(new ChattyInputStream(accepted, key), new ChattyOutputStream(accepted, key));
            } catch (IOException e) {
                throw new IOError(e);
            }
        });
    }

    private SocketChannel acceptBlocking() throws IOException {
        SocketChannel accepted = channel.accept();
        while (accepted == null) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }
            Thread[] threads = (Thread[]) key.attachment();
            threads[Chatty.IDX_READ] = Thread.currentThread();
            key.interestOpsOr(SelectionKey.OP_ACCEPT);
            if (EventLoopThread.current() == null) {
                // not on an event loop; ping the selector
                key.selector().wakeup();
            }
            LockSupport.park(this);
            key.interestOpsAnd(~SelectionKey.OP_ACCEPT);
            threads[Chatty.IDX_READ] = null;
            accepted = channel.accept();
        }
        return accepted;
    }
}
