package io.github.dmlloyd.example.chatty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import io.smallrye.common.net.Inet;
import org.jboss.threads.EnhancedQueueExecutor;
import org.jboss.threads.virtual.EventLoopThread;
import org.jboss.threads.virtual.Scheduler;

/**
 * The chatty server.
 */
public final class Chatty {
    static final int IDX_READ = 0;
    static final int IDX_WRITE = 1;

    public static void main(String[] args) {
        EnhancedQueueExecutor eqe = new EnhancedQueueExecutor.Builder().build();
        Scheduler scheduler = Scheduler.create(eqe);
        EventLoopThread elt = scheduler.newEventLoopThread(new ChattyEventLoop());
        ChattyEventLoop el = (ChattyEventLoop) elt.eventLoop();
        elt.newPool().execute(new Server(el, elt.newPool()));
        for (;;) {
            try {
                Thread.sleep(3_000L);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    static class Server implements Runnable {

        private static final String TOMBSTONE = "Tombstone!";
        private final Set<LinkedBlockingQueue<String>> subscribers = ConcurrentHashMap.newKeySet();
        private final ChattyEventLoop eventLoop;
        private final Executor executor;

        Server(final ChattyEventLoop eventLoop, final Executor executor) {
            this.eventLoop = eventLoop;
            this.executor = executor;
        }

        public void run() {
            try {
                ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.INET6);
                server.configureBlocking(false);
                server.bind(new InetSocketAddress(Inet.INET4_ANY, 42424));
                Selector sel = eventLoop.selector();
                SelectionKey key = server.register(sel, SelectionKey.OP_ACCEPT, new Thread[1]);
                ChattyServerSocket css = new ChattyServerSocket(executor, server, key);
                for (;;) {
                    css.accept(this::runThread);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void runThread(InputStream inputStream, OutputStream outputStream) {
            try (InputStreamReader isr = new InputStreamReader(inputStream)) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    try (OutputStreamWriter osw = new OutputStreamWriter(outputStream)) {
                        try (PrintWriter pw = new PrintWriter(osw)) {
                            pw.print("Welcome to Chatty! Please enter your name.\r\n");
                            pw.flush();
                            String name;
                            do {
                                name = br.readLine();
                                if (name == null) {
                                    return;
                                }
                            } while (name.isEmpty());
                            pw.printf("Hello, %s! Start chatting!\r\n", name);
                            pw.flush();
                            final LinkedBlockingQueue<String> q = new LinkedBlockingQueue<>() {
                                public boolean equals(final Object obj) {
                                    return this == obj;
                                }

                                public int hashCode() {
                                    return System.identityHashCode(this);
                                }
                            };
                            String qm = name + " has joined the chat!";
                            subscribers.forEach(qq -> qq.add(qm));
                            subscribers.add(q);
                            try {
                                final String finalName = name;
                                executor.execute(() -> {
                                    // take user input and put it into the chat queue
                                    try {
                                        for (; ; ) {
                                            String msg = br.readLine();
                                            if (msg == null) {
                                                break;
                                            }
                                            if (msg.isEmpty()) {
                                                continue;
                                            }
                                            String qm1 = finalName + "> " + msg;
                                            subscribers.forEach(qq -> qq.add(qm1));
                                        }
                                        br.close();
                                        pw.close();
                                    } catch (IOException ignored) {
                                    } finally {
                                        q.add(TOMBSTONE);
                                    }
                                });
                                for (; ; ) {
                                    try {
                                        String msg = q.take();
                                        //noinspection StringEquality
                                        if (msg == TOMBSTONE) {
                                            break;
                                        }
                                        pw.print(msg);
                                        pw.print("\r\n");
                                        pw.flush();
                                    } catch (InterruptedException e) {
                                        // close it all down
                                        return;
                                    }
                                }
                            } finally {
                                subscribers.remove(q);
                                String qm1 = name + " has left the chat!";
                                subscribers.forEach(qq -> qq.add(qm1));
                            }
                        }
                    }
                }
            } catch (IOException ignored) {
            }
        }
    }
}

