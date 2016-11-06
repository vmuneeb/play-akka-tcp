package akka.client;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.server.Server;
import controllers.MessageProtocol;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Created by muneeb on 05/11/16.
 */
public class RequestProcessor extends UntypedActor {

    private ActorRef tcpActor;
    HashMap<Long,CompletableFuture> map ;

    public static Props props(ActorRef tcpActor,HashMap<Long,CompletableFuture> map) {
        return Props.create(RequestProcessor.class,tcpActor,map);
    }

    public RequestProcessor(ActorRef tcpActor,HashMap<Long,CompletableFuture> map) {
        this.tcpActor = tcpActor;
        this.map = map;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if(message instanceof MessageProtocol) {
            //Do some processing..
            map.put(((MessageProtocol) message).counter,((MessageProtocol) message).future);
            tcpActor.tell(message,ActorRef.noSender());
        }

    }
}
