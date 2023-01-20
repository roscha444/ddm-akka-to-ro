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
import lombok.Setter;

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
    @Setter
    public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        int taskId;
        int taskPerColCounter;
        int fileIdA;
        int colIdA;
        int rowFromA;
        int rowToA;
        int fileIdB;
        int colIdB;
        int amountColumnA;
        int amountColumnB;
    }

    @Getter
    @AllArgsConstructor
    public static class DataMessage implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4667745204456518160L;
        int taskId;
        int fileId;
        int colId;
        int from;
        int to;
        HashSet<String> data;
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

        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());
        this.batchData.add(new ArrayList<>());

        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());
        this.batchDataReady.add(new ArrayList<>());

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
        taskMessage = message;

        this.getContext().getLog().info("New Task! {}. Requesting data...", message.getTaskId());

        if (this.batchData.get(message.getFileIdA()).isEmpty()) {
            for (int i = 0; i < message.getAmountColumnA(); i++) {
                this.batchData.get(message.getFileIdA()).add(new HashSet<>());
                this.batchDataReady.get(message.getFileIdA()).add(false);
            }
        }

        if (this.batchData.get(message.getFileIdB()).isEmpty()) {
            for (int j = 0; j < message.getAmountColumnB(); j++) {
                this.batchData.get(message.getFileIdB()).add(new HashSet<>());
                this.batchDataReady.get(message.getFileIdB()).add(false);
            }
        }

        if (!batchReady(taskMessage.getFileIdA(), taskMessage.getColIdA())) {
            LargeMessageProxy.LargeMessage requestDataMessageA = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), message.getFileIdA(), message.getColIdA(), 0, sizeOfDataToSend);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessageA, message.getDependencyMinerLargeMessageProxy()));
        }

        if (!batchReady(taskMessage.getFileIdB(), taskMessage.getColIdB())) {
            LargeMessageProxy.LargeMessage requestDataMessageB = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), message.getFileIdB(), message.getColIdB(), 0, sizeOfDataToSend);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessageB, message.getDependencyMinerLargeMessageProxy()));
        }

        if (batchReady(taskMessage.getFileIdB(), taskMessage.getColIdB()) && batchReady(taskMessage.getFileIdA(), taskMessage.getColIdA())) {
            work();
        }

        return this;
    }

    private Boolean batchReady(int taskMessage, int taskMessage1) {
        return batchDataReady.get(taskMessage).get(taskMessage1);
    }

    TaskMessage taskMessage;
    List<List<HashSet<String>>> batchData = new ArrayList<>();
    List<List<Boolean>> batchDataReady = new ArrayList<>();

    private Behavior<Message> handle(DataMessage message) {
        if (!message.data.isEmpty()) {

            //TODO if data is already loaded

            batchData.get(message.getFileId()).get(message.getColId()).addAll(message.getData());

            int storedSize = batchData.get(message.getFileId()).get(message.getColId()).size();
            LargeMessageProxy.LargeMessage requestDataMessage = new DependencyMiner.RequestDataForTaskMessage(this.getContext().getSelf(), message.getTaskId(), message.getFileId(), message.getColId(), storedSize, storedSize + sizeOfDataToSend);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestDataMessage, taskMessage.getDependencyMinerLargeMessageProxy()));
        } else {

            batchDataReady.get(message.getFileId()).set(message.getColId(), true);

            if (batchReady(taskMessage.getFileIdB(), taskMessage.getColIdB()) && batchReady(taskMessage.getFileIdA(), taskMessage.getColIdA())) {
                work();
            }
        }


        return this;
    }

    private void work() {
        this.getContext().getLog().info("Working! {} {}", taskMessage.getTaskId(), taskMessage.getTaskPerColCounter());

        int result = batchData.get(taskMessage.getFileIdB()).get(taskMessage.getColIdB()).containsAll(batchData.get(taskMessage.getFileIdA()).get(taskMessage.getColIdA())) ? 1 : 0;

        LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(
                this.getContext().getSelf(),
                result,
                taskMessage.getFileIdA(),
                taskMessage.getColIdA(),
                taskMessage.rowFromA,
                taskMessage.rowToA,
                taskMessage.getFileIdB(),
                taskMessage.getColIdB(),
                taskMessage.taskPerColCounter,
                taskMessage.getTaskId());

        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, taskMessage.getDependencyMinerLargeMessageProxy()));
    }
}
