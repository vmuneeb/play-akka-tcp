package akka.server;

/**
 * Created by muneeb on 16/10/16.
 */

import java.net.InetSocketAddress;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.Tcp.Bound;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;

public class Server extends UntypedActor {

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorRef tcpActor;

    public static Props props(ActorRef tcpActor) {
        return Props.create(Server.class, tcpActor);
    }

    public Server(ActorRef tcpActor) {
        this.tcpActor = tcpActor;
    }

    @Override
    public void preStart() throws Exception {
        if (tcpActor == null) {
            tcpActor = Tcp.get(getContext().system()).manager();
        }

        tcpActor.tell(TcpMessage.bind(getSelf(),
                new InetSocketAddress("localhost", 9090), 100), getSelf());
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Bound) {
            log.info("In ServerActor - received message: bound");

        } else if (msg instanceof CommandFailed) {
            getContext().stop(getSelf());

        } else if (msg instanceof Connected) {
            final Connected conn = (Connected) msg;
            log.info("In ServerActor - received message: connected");

            final ActorRef handler = getContext().actorOf(
                    Props.create(ConnectionHandler.class));

            getSender().tell(TcpMessage.register(handler), getSelf());
        }
    }


}