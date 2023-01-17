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
        this.data = new ArrayList[this.inputReaders.size()][];
        this.isReady = new boolean[this.inputReaders.size()];
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

    ArrayList<String>[][] data;
    boolean[] isReady;

    private Behavior<Message> handle(BatchMessage message) {
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

            if (data[message.getId()] != null) {
                //Append the data to the existing data
                for (int col = 0; col < cols.length; col++) {
                    data[message.getId()][col].addAll(cols[col]);
                }
            } else {
                data[message.getId()] = cols;
            }

            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        } else {
            // file is empty, mark it as ready
            this.isReady[message.getId()] = true;

            boolean allReady = true;
            for (boolean b : this.isReady) {
                if (!b) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) {
                computeTasks();
            }
        }
        return this;
    }

    int sizeOfA = 10000;

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Task {
        int taskPerColCounter;
        int fileIdA;
        int colIdA;
        int rowFromA;
        int rowToA;
        int fileIdB;
        int colIdB;
        List<String> colA;
        List<String> colB;
    }

    private List<Task> tasksToDo = new ArrayList();

    private void computeTasks() {

        //compute results list
        results = new boolean[this.inputFiles.length][][];


        // Compute tasks
        for (int fileIdA = 0; fileIdA < this.data.length; fileIdA++) {

            // compute tasks within this table
            List<String>[] tableA = this.data[fileIdA];

            results[fileIdA] = new boolean[tableA.length][];

            for (int colA = 0; colA < tableA.length; colA++) {
                for (int colB = 0; colB < tableA.length; colB++) {
                    if (colA != colB) {
                        splitTasks(fileIdA, colA, fileIdA, colB);
                    }
                }
            }

            for (int fileIdB = 0; fileIdB < this.data.length; fileIdB++) {
                if (fileIdB != fileIdA) {
                    // compute tasks in combination to other table
                    List<String>[] tableB = this.data[fileIdB];

                    for (int colA = 0; colA < tableA.length; colA++) {
                        for (int colB = 0; colB < tableB.length; colB++) {
                            splitTasks(fileIdA, colA, fileIdB, colB);
                        }
                    }
                }

            }

        }
        // All tasks computed
        for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
            getNextTask(dependencyWorker);
        }
    }

    private void getNextTask(ActorRef<DependencyWorker.Message> dependencyWorker) {
        if (currentTask < this.tasksToDo.size()) {
            Task todo = this.tasksToDo.get(currentTask);
            dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, currentTask, todo));
            currentTask++;
        } else {
            this.end();
        }
    }

    int currentTask = 0;

    private void splitTasks(int fileIdA, int colIdA, int fileIdB, int colIdB) {
        // for parallelization, we can split A into smaller packages (A c B)
        List<String> colA = this.data[fileIdA][colIdA];

        List<String> colB = this.data[fileIdB][colIdB];

        int taskPerColCounter = 0;
        for (int rowFromA = 0; rowFromA < colA.size(); rowFromA += sizeOfA) {
            int rowToA = rowFromA + sizeOfA;
            if (rowToA > colA.size()) {
                rowToA = colA.size();
            }
            tasksToDo.add(new Task(taskPerColCounter, fileIdA, colIdA, rowFromA, rowToA, fileIdB, colIdB, colA.subList(rowFromA, rowToA), colB));
            taskPerColCounter++;
        }
        results[fileIdA][colIdA] = new boolean[taskPerColCounter];
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);
            // The worker should get some work ... let me send her something before I figure out what I actually want from her.
            // I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

            getNextTask(dependencyWorker);
        }
        return this;
    }

    boolean[][][] results;

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();

        getNextTask(dependencyWorker);

        boolean result = message.getResult() == 1;
        boolean[] resultForThisCol = results[message.getFileIdA()][message.getColIdA()];
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