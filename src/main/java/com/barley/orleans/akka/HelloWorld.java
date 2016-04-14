package com.barley.orleans.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.barley.orleans.metrics.meters.AppMeters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

import javax.inject.Named;
import java.util.Date;

@Named("HelloWorld")
@Scope("prototype")
public class HelloWorld extends UntypedActor {

    @Autowired
    AppMeters appMetrics;

    @Override
    public void onReceive(Object msg) {
        if (msg == Greeter.Msg.DONE) {
            // when the greeter is done, stop this actor and with it the application
            appMetrics.increment("DONE");
            try {
                Thread.sleep(60000);
                System.out.println(String.format("%s | Waited a minute, Starting Greet again!!!!", new Date()));
                getSender().tell(Greeter.Msg.GREET, getSelf());
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        } else if (msg == Greeter.Msg.GREET) {
            System.out.println("Received GREET BACK - payload is verified");
            appMetrics.increment("GREET");
            getSender().tell(Greeter.Msg.DONE, getSelf());
        } else if (msg == Greeter.Msg.START) {
            final ActorRef greeter = getContext().actorOf(Props.create(Greeter.class), "greeter");
            // tell it to perform the greeting
            greeter.tell(Greeter.Msg.GREET, getSelf());
            appMetrics.increment("START");
        } else
            unhandled(msg);
    }
}