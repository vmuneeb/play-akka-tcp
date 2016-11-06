package controllers;

import java.util.concurrent.CompletableFuture;

/**
 * Created by muneeb on 16/10/16.
 */
public class MessageProtocol {
    public CompletableFuture<Integer> future;
    public long  counter;

    public MessageProtocol(CompletableFuture future, long counter) {
        this.future = future;
        this.counter = counter;
    }
}
