package com.purbon.kafka.perf;

import picocli.CommandLine;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@CommandLine.Command(name = "kafka-perf-tool", subcommands = { CommandLine.HelpCommand.class },
        description = "kafka performance test tool" )
public class Cmd implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    public static Properties config(String fileParam) {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream(fileParam));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of given threads!!")
    private int threads = 1;

    private KafkaPerfTool kafkaPerfTool;

    @CommandLine.Command(name = "write", description = "kafka performance write test")
    void write(@CommandLine.Option(names = {"-c", "--config"}) String configFile) {
        System.out.println("Kafka Perf Test on Writing");
        kafkaPerfTool = new KafkaPerfTool(threads);
        addShutdownHook();
        Properties props = config(configFile);
        kafkaPerfTool.run(props);

    }

    @CommandLine.Command(name = "read", description = "kafka performance read test")
    void read(@CommandLine.Option(names = {"-c", "--config"}) String configFile) {
        System.out.println("Kafka Perf Test on Reading");
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaPerfTool.stop();
        }));
    }

    public static void main(String[] args) {
/*
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();

        String fileParam = args[0];
        KafkaPerfTool kpf = new KafkaPerfTool(5);

        executor.submit(() -> {
            Properties props = config(fileParam);
            kpf.run(props);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           kpf.stop();
        }));
 */
        var commandLine = new CommandLine(new Cmd());
        if (args.length == 0) {
            commandLine.usage(System.out);
        } else {
            commandLine.execute(args);
        }
    }
}
