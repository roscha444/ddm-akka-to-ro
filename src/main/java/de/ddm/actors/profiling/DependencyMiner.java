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
        int task;
        boolean isA;
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

        for (int id = 0; id < this.inputFiles.length; id++)
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();
        this.dependencyWorkersLargeMessageProxy = new ArrayList<>();
        this.batchData = new ArrayList[this.inputReaders.size()][];
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
    ArrayList<String>[][] batchData;
    boolean[] readyForFile;
    boolean allTasksComputed = false;
    int currentTask = 0;
    private final List<DependencyWorker.NewTaskMessage> tasks = new ArrayList();
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

        DependencyWorker.NewTaskMessage taskMessage = this.tasks.get(message.task);
        List<String> data;
        if (message.isA) {
            data = this.batchData[taskMessage.getFileIdA()][taskMessage.getColIdA()];
            if (message.from > data.size()) {
                data = new ArrayList<>();
            } else {
                data = data.subList(message.from, Math.min(message.to, data.size()));
            }
        } else {
            data = this.batchData[taskMessage.getFileIdB()][taskMessage.getColIdB()];
            if (message.from > data.size()) {
                data = new ArrayList<>();
            } else {
                data = data.subList(message.from, Math.min(message.to, data.size()));
            }
        }

        LargeMessageProxy.LargeMessage dataMessage = new DependencyWorker.DataMessage(message.getTask(), message.from, Math.min(message.to, data.size()), message.isA, data);
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
            // file is not empty yet

            int colSize = message.getBatch().get(0).length;

            // Create Set with all columns which store the value
            ArrayList<String>[] cols = new ArrayList[colSize];

            for (int col = 0; col < colSize; col++) {
                Set<String> colValues = new HashSet<>();
                for (int row = 0; row < batchRows.size(); row++) {
                    colValues.add(batchRows.get(row)[col]);
                }
                cols[col] = new ArrayList<>(colValues);
            }

            if (batchData[message.getId()] != null) {
                //Append the data to the existing data
                for (int col = 0; col < cols.length; col++) {
                    batchData[message.getId()][col].addAll(cols[col]);
                }
            } else {
                batchData[message.getId()] = cols;
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
        for (int fileIdA = 0; fileIdA < this.batchData.length; fileIdA++) {

            // compute tasks within this table
            List<String>[] tableA = this.batchData[fileIdA];

            resultsSplitBySubTask[fileIdA] = new boolean[tableA.length][];

            for (int colA = 0; colA < tableA.length; colA++) {
                for (int colB = 0; colB < tableA.length; colB++) {
                    if (colA != colB) {
                        splitTasks(fileIdA, colA, fileIdA, colB);
                    }
                }
            }

            for (int fileIdB = 0; fileIdB < this.batchData.length; fileIdB++) {
                if (fileIdB != fileIdA) {
                    // compute tasks in combination to other table
                    List<String>[] tableB = this.batchData[fileIdB];

                    for (int colA = 0; colA < tableA.length; colA++) {
                        for (int colB = 0; colB < tableB.length; colB++) {
                            splitTasks(fileIdA, colA, fileIdB, colB);
                        }
                    }
                }

            }

        }
        this.getContext().getLog().info("Computed {} tasks", this.tasks.size());
        //TODO
        // All tasks computed
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
            DependencyWorker.NewTaskMessage task = this.tasks.get(currentTask);
            LargeMessageProxy.LargeMessage taskMessage = new DependencyWorker.TaskMessage(this.largeMessageProxy, currentTask, task);
            this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskMessage, this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(dependencyWorker))));
            currentTask++;
            this.lastWorker = dependencyWorker;
        } else {
            this.getContext().getLog().info("All tasks are done, waiting for last job to complete");
            if (taskId == currentTask - 1) {
                this.getContext().getLog().info("Last task completed!");
                this.end();
            }
        }
    }


    /**
     * Split the task in subtask for parallelization. We can split A into smaller packages (Ai c B)
     */
    private void splitTasks(int fileIdA, int colIdA, int fileIdB, int colIdB) {
        List<String> colA = this.batchData[fileIdA][colIdA];

        int taskPerColCounter = 0;
        for (int rowFromA = 0; rowFromA < colA.size(); rowFromA += sizeOfA) {
            int rowToA = rowFromA + sizeOfA;
            if (rowToA > colA.size()) {
                rowToA = colA.size();
            }
            tasks.add(new DependencyWorker.NewTaskMessage(taskPerColCounter, fileIdA, colIdA, rowFromA, rowToA, fileIdB, colIdB));
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