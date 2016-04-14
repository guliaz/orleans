package com.barley.orleans.akka;

import akka.actor.UntypedActor;

public class Greeter extends UntypedActor {

    public enum Msg {
        GREET, DONE, START
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message == Msg.GREET) {
            System.out.println("Hello World!!");
            getSender().tell(Msg.GREET, getSelf());
        } else if (message == Msg.DONE) {
            getSender().tell(Msg.DONE, getSelf());
            System.out.println("GOT DONE, sending DONE BACK");
        }
    }
}