package akka.client;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Created by muneeb on 05/11/16.
 */
public class ResponseProcessor extends UntypedActor {

    HashMap<Long,CompletableFuture> map ;

    public static Props props(HashMap<Long,CompletableFuture> map) {
        return Props.create(ResponseProcessor.class,map);
    }

    public ResponseProcessor(HashMap<Long,CompletableFuture> map) {
        this.map = map;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if(message instanceof Tcp.Received) {
            long counter = Long.valueOf(((Tcp.Received) message).data().utf8String());
            if (map.containsKey(counter)) {
                map.get(counter).complete(Long.toString(counter));
                map.remove(counter);
            }
        }

    }
}
