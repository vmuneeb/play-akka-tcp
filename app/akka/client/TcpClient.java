package akka.client;

/**
 * Created by muneeb on 16/10/16.
 */

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;
import akka.japi.Procedure;
import akka.util.ByteString;
import controllers.MessageProtocol;

public class TcpClient extends UntypedActor {

    private LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    public static final long MAX_STORED = 1000000000;
    public static final long HIGH_WATERMARK = MAX_STORED * 5 / 10;
    public static final long LOW_WATERMARK = MAX_STORED * 2 / 10;

    private long transferred;
    private int storageOffset = 0;
    private long stored = 0;
    private Queue<ByteString> storage = new LinkedList<>();
    private boolean suspended = false;

    private static class Ack implements Tcp.Event {
        public final int ack;

        public Ack(int ack) {
            this.ack = ack;
        }
    }

    private ActorRef responseHandler;
    private ActorRef tcpActor;
    ActorRef connection = null;
    final InetSocketAddress remote;
    HashMap<Long, CompletableFuture> futureMap = new HashMap<>();

    public static Props props(InetSocketAddress remote, ActorRef responseHandler) {
        return Props.create(TcpClient.class, remote, responseHandler);
    }

    public TcpClient(InetSocketAddress remote, ActorRef responseHandler) {
        this.remote = remote;
        this.responseHandler = responseHandler;
        if (tcpActor == null) {
            tcpActor = Tcp.get(getContext().system()).manager();
        }
        tcpActor.tell(TcpMessage.connect(remote), getSelf());
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof CommandFailed) {
            LOG.info("In ClientActor - received message: failed");
            getContext().stop(getSelf());
        } else if (msg instanceof Connected) {
            LOG.info("In ClientActor - received message: connected");
            getSender().tell(TcpMessage.register(getSelf()), getSelf());
            this.connection = getSender();
            getContext().become(writing);

        }
    }


    private final Procedure<Object> writing = new Procedure<Object>() {
        @Override
        public void apply(Object msg) throws Exception {

            if (msg instanceof Tcp.Received) {
                LOG.info("Got response from TCP server");
                responseHandler.tell(msg, ActorRef.noSender());

            }
            if (msg instanceof MessageProtocol) {
                LOG.info("sending request to TCP server");
                MessageProtocol message = (MessageProtocol) msg;
                connection.tell(TcpMessage.write(ByteString.fromString(Long.toString(message.counter)), new Ack(currentOffset())), getSelf());
                buffer(ByteString.fromString(Long.toString(message.counter)));

            } else if (msg instanceof Ack) {
                Ack ack = (Ack) msg;
                acknowledge(ack.ack);

            } else if (msg instanceof Tcp.CommandFailed) {
                LOG.error("command failed");
                final Tcp.Write w = (Tcp.Write) ((Tcp.CommandFailed) msg).cmd();
                connection.tell(TcpMessage.resumeWriting(), getSelf());
                getContext().become(buffering((Ack) w.ack()));

            } else if (msg instanceof Tcp.ConnectionClosed) {
                LOG.debug("Restarting Tcp Connection");
                getContext().stop(getSelf());
                if (!storage.isEmpty()) {
                    storage.clear();
                }
            }
        }
    };


    //#buffering
    protected Procedure<Object> buffering(final Ack nack) {
        return new Procedure<Object>() {

            private int toAck = 10;
            private boolean peerClosed = false;

            @Override
            public void apply(Object msg) throws Exception {
                if (msg instanceof Tcp.Received) {
                    LOG.info("Got response from switch in TCP Server");
                    responseHandler.tell(msg, getSelf());

                } else if (msg instanceof MessageProtocol) {
                    LOG.info("sending request to TCP Server in buffering");
                    MessageProtocol message = (MessageProtocol) msg;
                    buffer(ByteString.fromString(Long.toString(message.counter)));
                } else if (msg instanceof Tcp.WritingResumed) {
                    writeFirst();

                } else if (msg instanceof Tcp.ConnectionClosed) {
                    LOG.error("Connection failed to TCP Server");
                    LOG.debug("Restarting Tcp Connection");
                    getContext().stop(getSelf());
                    if (!storage.isEmpty()) {
                        storage.clear();
                    }
                } else if (msg instanceof Ack) {
                    Ack ackTemp = (Ack) msg;
                    final int ack = ackTemp.ack;
                    acknowledge(ack);

                    if (ack >= nack.ack) {
                        // otherwise it was the ack of the last successful write
                        if (storage.isEmpty()) {
                            if (peerClosed)
                                getContext().stop(getSelf());
                            else
                                getContext().become(writing);

                        } else {
                            if (toAck > 0) {
                                // stay in ACK-based mode for a short while
                                writeFirst();
                                --toAck;
                            } else {
                                // then return to NACK-based again
                                writeAll();
                                getContext().become(writing);
                            }
                        }
                    }
                }
            }
        };
    }


    //#helpers
    protected void buffer(ByteString data) {
        storage.add(data);
        stored += data.size();
        if (stored > MAX_STORED) {
            LOG.warning("drop connection to [{}] (buffer overrun)", remote);
            getContext().stop(getSelf());

        } else if (stored > HIGH_WATERMARK) {
            LOG.error("suspending reading at {}", currentOffset());
            connection.tell(TcpMessage.suspendReading(), getSelf());
            suspended = true;
        }
    }

    protected void acknowledge(int ack) {
        assert ack == storageOffset;
        assert !storage.isEmpty();
        final ByteString acked = storage.remove();
        stored -= acked.size();
        transferred += acked.size();
        storageOffset += 1;
        if (suspended && stored < LOW_WATERMARK) {
            LOG.debug("resuming reading");
            connection.tell(TcpMessage.resumeReading(), getSelf());
            suspended = false;
        }
    }

    protected int currentOffset() {
        return storageOffset + storage.size();
    }

    protected void writeAll() {
        int i = 0;
        for (ByteString data : storage) {
            connection.tell(TcpMessage.write(data, new Ack(storageOffset + i++)), getSelf());
        }
    }

    protected void writeFirst() {
        connection.tell(TcpMessage.write(storage.peek(), new Ack(storageOffset)), getSelf());
    }


}
