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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int result;
        int fileIdA;
        int colIdA;
        int rowFromA;
        int rowToA;
        int fileIdB;
        int colIdB;
        int taskPerColCounter;
        int taskId;
    }

    @Getter
    @AllArgsConstructor
    public static class RequestDataForTaskMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        int taskId;
        int fileId;
        int ColId;
        int from;
        int to;
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
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];

        this.inputReaders = new ArrayList<>(inputFiles.length);
        this.batchData = new ArrayList<List<HashSet<String>>>();

        for (int id = 0; id < this.inputFiles.length; id++) {
            this.batchData.add(new ArrayList<>());
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        }

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();
        this.dependencyWorkersLargeMessageProxy = new ArrayList<>();

        this.readyForFile = new boolean[this.inputReaders.size()];
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

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkersLargeMessageProxy;
    List<List<HashSet<String>>> batchData;
    boolean[] readyForFile;
    boolean allTasksComputed = false;
    int currentTask = 0;
    private final List<DependencyWorker.TaskMessage> tasks = new ArrayList();

    private final List<Boolean> taskComplete = new ArrayList();
    boolean[][][] resultsSplitBySubTask;
    int sizeOfA = 50000;

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
                .onMessage(RequestDataForTaskMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(RequestDataForTaskMessage message) {
        HashSet<String> data = this.batchData.get(message.getFileId()).get(message.getColId());
        HashSet<String> dataToSend;

        if (message.from > data.size()) {
            dataToSend = new HashSet<>();
        } else {
            dataToSend = data.stream().skip(message.from).limit(message.to - message.from).collect(Collectors.toCollection(HashSet::new));
        }

        LargeMessageProxy.LargeMessage dataMessage = new DependencyWorker.DataMessage(message.taskId, message.getFileId(), message.getColId(), message.from, message.from + dataToSend.size(), dataToSend);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(dataMessage, this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(message.dependencyWorker))));

        return this;
    }

    private Behavior<Message> handle(StartMessage message) {
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        this.startTime = System.currentTimeMillis();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        this.headerLines[message.getId()] = message.getHeader();
        return this;
    }


    private Behavior<Message> handle(BatchMessage message) {
        this.getContext().getLog().info("Read BatchMessage from {}", this.inputFiles[message.id].getName());

        List<String[]> batchRows = message.getBatch();

        if (batchRows.size() != 0) {

            int amountOfColumns = message.getBatch().get(0).length;

            for (int column = 0; column < amountOfColumns; column++) {
                if (batchData.get(message.getId()).isEmpty() || batchData.get(message.getId()).size() <= column) {
                    batchData.get(message.getId()).add(column, new HashSet<String>());
                }

                for (String[] batchRow : batchRows) {
                    String valueForColumn = batchRow[column];
                    batchData.get(message.getId()).get(column).add(valueForColumn);
                }
            }

            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        } else {
            // file is empty, mark it as ready
            this.readyForFile[message.getId()] = true;

            boolean allFilesReady = true;
            for (boolean b : this.readyForFile) {
                if (!b) {
                    allFilesReady = false;
                    break;
                }
            }
            if (allFilesReady) {
                computeTasks();
            }
        }
        return this;
    }

    /**
     * Compute the tasks on the data
     */
    private void computeTasks() {

        this.getContext().getLog().info("Compute tasks");


        //compute results list
        resultsSplitBySubTask = new boolean[this.inputFiles.length][][];


        // Compute tasks
        for (int fileIdA = 0; fileIdA < this.batchData.size(); fileIdA++) {
            List<HashSet<String>> tableA = this.batchData.get(fileIdA);
            resultsSplitBySubTask[fileIdA] = new boolean[tableA.size()][];

            for (int fileIdB = 0; fileIdB < this.batchData.size(); fileIdB++) {
                List<HashSet<String>> tableB = this.batchData.get(fileIdB);

                for (int colA = 0; colA < tableA.size(); colA++) {
                    for (int colB = 0; colB < tableB.size(); colB++) {
                        if (fileIdA != fileIdB || colA != colB) {
                            splitTasks(fileIdA, colA, fileIdB, colB);
                        }
                    }
                }
            }

        }

        this.getContext().getLog().info("Computed {} tasks", this.tasks.size());

        this.allTasksComputed = true;
        for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
            sendNextTask(dependencyWorker, -1);
        }
    }

    ActorRef<DependencyWorker.Message> lastWorker;

    /**
     * Send the next task to the DependencyWorker
     */
    private void sendNextTask(ActorRef<DependencyWorker.Message> dependencyWorker, int taskId) {
        if (currentTask < this.tasks.size()) {
            DependencyWorker.TaskMessage task = this.tasks.get(currentTask);
            task.setTaskId(currentTask);
            task.setDependencyMinerLargeMessageProxy(largeMessageProxy);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(dependencyWorker))));
            currentTask++;
            this.lastWorker = dependencyWorker;
        } else {
            this.getContext().getLog().info("All tasks are assigned, waiting for last job to complete");

            boolean allTrue = true;
            for (boolean complete : taskComplete) {
                if (!complete) {
                    allTrue = false;
                    break;
                }
            }

            if (allTrue) {
                this.getContext().getLog().info("Last task completed!");
                this.end();
            }
        }
    }


    /**
     * Split the task in subtask for parallelization. We can split A into smaller packages (Ai c B)
     */
    private void splitTasks(int fileIdA, int colIdA, int fileIdB, int colIdB) {
        Set<String> colA = this.batchData.get(fileIdA).get(colIdA);

        int taskPerColCounter = 0;
        for (int rowFromA = 0; rowFromA < colA.size(); rowFromA += sizeOfA) {
            int rowToA = rowFromA + sizeOfA;
            if (rowToA > colA.size()) {
                rowToA = colA.size();
            }
            taskComplete.add(false);
            tasks.add(
                    new DependencyWorker.TaskMessage(null, -1, taskPerColCounter,
                            fileIdA,
                            colIdA,
                            rowFromA,
                            rowToA,
                            fileIdB,
                            colIdB,
                            this.batchData.get(fileIdA).size(),
                            this.batchData.get(fileIdB).size()));
            taskPerColCounter++;
        }
        resultsSplitBySubTask[fileIdA][colIdA] = new boolean[taskPerColCounter];
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.dependencyWorkersLargeMessageProxy.add(message.getDependencyMinerLargeMessageProxy());
            this.getContext().watch(dependencyWorker);

            if (allTasksComputed) {
                sendNextTask(dependencyWorker, -1);
            }
        }
        return this;
    }

    /**
     * Task is competed from DependencyWorker. Check if all subtasks are completed and calculate the result.
     */
    private Behavior<Message> handle(CompletionMessage message) {

        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();

        boolean result = message.getResult() == 1;
        boolean[] resultForThisCol = resultsSplitBySubTask[message.getFileIdA()][message.getColIdA()];
        resultForThisCol[message.taskPerColCounter] = result;

        boolean allTrue = true;
        for (boolean rowResult : resultForThisCol) {
            if (!rowResult) {
                allTrue = false;
                break;
            }
        }

        if (allTrue) {
            this.getContext().getLog().info("Received all results for {} c {} : {}", this.headerLines[message.getFileIdA()][message.getColIdA()], this.headerLines[message.getFileIdB()][message.getColIdB()], result);

            File dependentFile = this.inputFiles[message.getFileIdA()];
            File referencedFile = this.inputFiles[message.getFileIdB()];
            String[] dependentAttributes = {this.headerLines[message.getFileIdA()][message.getColIdA()]};
            String[] referencedAttributes = {this.headerLines[message.getFileIdB()][message.getColIdB()]};

            InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);

            List<InclusionDependency> inds = new ArrayList<>();
            inds.add(ind);

            this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
        }

        taskComplete.remove(0);
        sendNextTask(dependencyWorker, message.getTaskId());

        return this;
    }

    private void end() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dependencyWorker);
        return this;
    }
}