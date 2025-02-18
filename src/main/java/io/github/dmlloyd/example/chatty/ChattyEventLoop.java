package io.github.dmlloyd.example.chatty;

import java.io.IOError;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jboss.threads.virtual.EventLoop;

final class ChattyEventLoop extends EventLoop {
    private final Selector selector;
    private final Consumer<SelectionKey> handleEvent = this::handleEvent;
    private final Executor ioThreadExecutor;

    ChattyEventLoop(Executor ioThreadExecutor) {
        this.ioThreadExecutor = ioThreadExecutor;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    Selector selector() {
        return selector;
    }

    Executor ioThreadExecutor() {
        return ioThreadExecutor;
    }

    protected void unparkAny(final long waitTime) {
        try {
            if (waitTime == 0) {
                selector.selectNow(handleEvent);
            } else if (waitTime == -1) {
                selector.select(handleEvent);
            } else {
                selector.select(handleEvent, (waitTime + 999_999L) / 1_000_000L);
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
        // do not wait more than 10 ms (100 Hz) to poll for ready I/O when busy
        LockSupport.parkNanos(10_000_000L);
    }

    protected void wakeup() {
        selector.wakeup();
    }

    private void handleEvent(SelectionKey selectionKey) {
        Thread[] threads = (Thread[]) selectionKey.attachment();
        if ((selectionKey.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0) {
            selectionKey.interestOpsAnd(~(SelectionKey.OP_READ | SelectionKey.OP_ACCEPT));
            LockSupport.unpark(threads[Chatty.IDX_READ]);
        }
        if ((selectionKey.readyOps() & (SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT)) != 0) {
            selectionKey.interestOpsAnd(~(SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT));
            LockSupport.unpark(threads[Chatty.IDX_WRITE]);
        }
    }
}
