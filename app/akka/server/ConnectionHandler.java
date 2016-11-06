package akka.server;

/**
 * Created by muneeb on 16/10/16.
 */
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp.ConnectionClosed;
import akka.io.Tcp.Received;
import akka.io.TcpMessage;
import akka.util.ByteString;

/**
 * Created by saeed on 15/February/15 AD.
 */
public class ConnectionHandler extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Received) {
            final String data = ((Received) msg).data().utf8String();
            log.info("In SimplisticHandlerActor - Received message: " + data);
            getSender().tell(TcpMessage.write(ByteString.fromString(data)), getSelf());
        } else if (msg instanceof ConnectionClosed) {
            getContext().stop(getSelf());
        }
    }
}