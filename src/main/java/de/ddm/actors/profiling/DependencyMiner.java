package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.WorkDTO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int id;
        String[] header;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int id;
        List<String[]> batch;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        boolean result;
        String[] dependentAttributes;
        String[] referencedAttributes;
        File firstFile;
        File secondFile;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";

    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyMiner::new);
    }

    private DependencyMiner(ActorContext<Message> context) {
        super(context);
        discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        inputFiles = InputConfigurationSingleton.get().getInputFiles();
        headerLines = new String[inputFiles.length][];

        inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < inputFiles.length; id++)
            inputReaders.add(context.spawn(InputReader.create(id, inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        largeMessageProxy = getContext().spawn(LargeMessageProxy.create(getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

        idleDependencyWorkers = new ArrayList<>();
        workingDependencyWorkers = new ArrayList<>();

        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private long startTime;

    private final boolean discoverNaryDependencies;
    private final File[] inputFiles;
    private final String[][] headerLines;

    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> idleDependencyWorkers;

    private final List<ActorRef<DependencyWorker.Message>> workingDependencyWorkers;


    private List<String[]> batchWhichHaveToProcess;

    List<WorkDTO> combinations = new ArrayList();

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(CompletionMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        for (ActorRef<InputReader.Message> inputReader : inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(getContext().getSelf()));
        for (ActorRef<InputReader.Message> inputReader : inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(getContext().getSelf()));

        data = new BatchMessage[inputReaders.size()];
        startTime = System.currentTimeMillis();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        headerLines[message.getId()] = message.getHeader();
        return this;
    }

    BatchMessage[] data;

    /**
     * New file content received, assigned idle workers to that new data
     *
     * @param message with file content as String[]
     * @return new Behavior
     */
    private Behavior<Message> handle(BatchMessage message) {

        //Stupides berechnen von Kombinationen

        data[message.getId()] = message;

        String[] headerLine = headerLines[message.getId()];

        for (int i = 0; i < headerLine.length; i++) {

            List<String> colOne = new ArrayList<>();
            for (int ia = 0; ia < message.getBatch().size(); ia++) {
                colOne.add(message.getBatch().get(ia)[i]);
            }

            for (int j = 0; j < headerLine.length; j++) {

                List<String> colTwo = new ArrayList<>();
                for (int ib = 0; ib < message.getBatch().size(); ib++) {
                    colTwo.add(message.getBatch().get(ib)[j]);
                }

                if (i != j) {
                    combinations.add(new WorkDTO(inputFiles[message.getId()], headerLine[i], colOne, inputFiles[message.getId()], headerLine[j], colTwo));
                }

            }
        }
        return this;
    }

    /**
     * RegistrationMessage from a worker that
     *
     * @param message with the ActorRef of the worker
     * @return new Behavior
     */
    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> newDependencyWorker = message.getDependencyWorker();

        newDependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 1, combinations.remove(0)));

        return this;
    }

    private void registerDependencyWorkerAsIdleWorker(ActorRef<DependencyWorker.Message> newDependencyWorker) {
        if (!idleDependencyWorkers.contains(newDependencyWorker) || !workingDependencyWorkers.contains(newDependencyWorker)) {
            idleDependencyWorkers.add(newDependencyWorker);
            getContext().watch(newDependencyWorker);
        }
    }

    /**
     * A completion message from a worker was send. Validate the result and write it to the resultCollector
     *
     * @param message with the completion result of a worker
     * @return new Behavior
     */
    private Behavior<Message> handle(CompletionMessage message) {

        //Einfach durchlaufen bis alle Kombinationen getestet wurden

        if (message.result) {
            InclusionDependency ind = new InclusionDependency(message.getFirstFile(), message.getDependentAttributes(), message.getSecondFile(), message.getReferencedAttributes());
            List<InclusionDependency> inds = new ArrayList<>(1);
            inds.add(ind);
            resultCollector.tell(new ResultCollector.ResultMessage(inds));
        }

        if (combinations.size() == 0) {
            end();
        }

        message.getDependencyWorker().tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 1, combinations.remove(0)));

        return this;
    }

    private void end() {
        resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - startTime;
        getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        idleDependencyWorkers.remove(dependencyWorker);
        return this;
    }
}