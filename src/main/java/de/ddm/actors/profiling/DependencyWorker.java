package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        Receptionist.Listing listing;
    }

    @Getter
    @AllArgsConstructor
    public static class NewTaskMessage implements Message, LargeMessageProxy.LargeMessage {
        int taskPerColCounter;
        int fileIdA;
        int colIdA;
        int rowFromA;
        int rowToA;
        int fileIdB;
        int colIdB;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int taskId;
        NewTaskMessage newTaskMessage;
    }

    @Getter
    @AllArgsConstructor
    public static class DataMessage implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        int taskId;
        int from;
        int to;
        boolean isA;
        List<String> data;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorker";

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyWorker::new);
    }

    private DependencyWorker(ActorContext<Message> context) {
        super(context);

        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
        return this;
    }

    int sizeOfDataToSend = 10000;

    private Behavior<Message> handle(TaskMessage message) {

        LargeMessageProxy.LargeMessage requestDataMessageA = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), true, 0, sizeOfDataToSend);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessageA, message.getDependencyMinerLargeMessageProxy()));

        LargeMessageProxy.LargeMessage requestDataMessageB = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), false, 0, sizeOfDataToSend);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessageB, message.getDependencyMinerLargeMessageProxy()));

        taskMessage = message;

        return this;
    }

    TaskMessage taskMessage;
    List<String> colA = new ArrayList<>();
    List<String> colB = new ArrayList<>();
    boolean isAComplete;
    boolean isBComplete;

    private Behavior<Message> handle(DataMessage message) {
        if (!message.data.isEmpty()) {
            LargeMessageProxy.LargeMessage requestDataMessage;
            if (message.isA) {
                colA.addAll(message.getData());
                requestDataMessage = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), message.isA, colA.size(), colA.size() + sizeOfDataToSend);
            } else {
                colB.addAll(message.getData());
                requestDataMessage = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), message.isA, colB.size(), colB.size() + sizeOfDataToSend);
            }
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessage, taskMessage.getDependencyMinerLargeMessageProxy()));
        } else {
            if (message.isA) {
                isAComplete = true;
            } else {
                isBComplete = true;
            }

            if (isAComplete && isBComplete) {
                work();
            }
        }


        return this;
    }

    private void work() {
        this.getContext().getLog().info("Working! {} {}", taskMessage.getTaskId(), taskMessage.getNewTaskMessage().getTaskPerColCounter());

        NewTaskMessage newTaskMessage = taskMessage.getNewTaskMessage();

        int result = new HashSet<>(colB).containsAll(colA) ? 1 : 0;
        colA = new ArrayList<>();
        colB = new ArrayList<>();
        isBComplete = false;
        isAComplete = false;

        LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result, newTaskMessage.getFileIdA(), newTaskMessage.getColIdA(), newTaskMessage.rowFromA, newTaskMessage.rowToA, newTaskMessage.getFileIdB(), newTaskMessage.getColIdB(), newTaskMessage.taskPerColCounter, taskMessage.getTaskId());
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, taskMessage.getDependencyMinerLargeMessageProxy()));
    }
}
