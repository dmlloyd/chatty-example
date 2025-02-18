package io.github.dmlloyd.example.chatty;

import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import org.jboss.threads.virtual.Scheduler;

final class ChattyServerSocket {
    private final Scheduler scheduler;
    private final ServerSocketChannel channel;
    private final SelectionKey key;

    ChattyServerSocket(final Scheduler scheduler, final ServerSocketChannel channel, final SelectionKey key) {
        this.scheduler = scheduler;
        this.channel = channel;
        this.key = key;
    }

    public void accept(BiConsumer<InputStream, OutputStream> handler) throws IOException {
        SocketChannel accepted = acceptBlocking();
        scheduler.execute(() -> {
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
            // todo: ping the selector thread nicely...?
            key.selector().wakeup();
            LockSupport.park(this);
            key.interestOpsAnd(~SelectionKey.OP_ACCEPT);
            threads[Chatty.IDX_READ] = null;
            accepted = channel.accept();
        }
        return accepted;
    }
}
