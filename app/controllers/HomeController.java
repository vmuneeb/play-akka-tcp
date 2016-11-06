package controllers;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.client.RequestProcessor;
import akka.client.ResponseProcessor;
import akka.client.TcpClient;
import akka.routing.FromConfig;
import akka.server.Server;
import com.google.inject.Inject;
import play.mvc.*;

import scala.compat.java8.FutureConverters;
import static akka.pattern.Patterns.ask;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This controller contains an action to handle HTTP requests
 */
public class HomeController extends Controller {

    final ActorSystem system;
     ActorRef serverActor;
     ActorRef clientActor;
     ActorRef requestHandlerActor;
     ActorRef responseHandlerActor;
     HashMap<Long,CompletableFuture> map = new HashMap<Long,CompletableFuture>();;
    static long counter = 0;

    @Inject
    HomeController(ActorSystem system) {
        this.system = system;
        serverActor = system.actorOf(Server.props(null), "serverActor");

        responseHandlerActor = system.actorOf(ResponseProcessor.props(map).withRouter(new FromConfig()), "ResponseHandler");
        clientActor = system.actorOf(TcpClient.props(
                new InetSocketAddress("localhost", 9090), responseHandlerActor), "clientActor");
        requestHandlerActor = system.actorOf(RequestProcessor.props(clientActor,map).withRouter(new FromConfig()), "RequestHandler");
    }

    public Result index() {
        return ok("Hello World");
    }

    public CompletionStage<Result> tcpWithFuture() {
        counter++;
        CompletableFuture<String> future =new CompletableFuture<>();
        MessageProtocol message = new MessageProtocol(future,counter);
        requestHandlerActor.tell(message,ActorRef.noSender());
        return future.thenApply(res-> {
            return ok(res.toString());
        });
    }

}
