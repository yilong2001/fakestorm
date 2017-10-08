package org.apache.fake.storm.supervisor.manager;

import org.apache.commons.io.FileUtils;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

import static org.apache.fake.storm.utils.ClusterConfigUtil.FILE_PATH_SEPARATOR;
import static org.apache.fake.storm.utils.ClusterConfigUtil.STORM_ROOT_DIR;
import static org.apache.fake.storm.utils.CmdUtils.writeScript;
import static org.apache.fake.storm.utils.SupervisorConfigUtil.*;

/**
 * Created by yilong on 2017/9/14.
 */
public class WorkerNodeLoader {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerNodeLoader.class);

    private static final FilenameFilter jarFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".jar");
        }
    };

    //private final String topoId;
    private final String supervisorId;
    private final String workerId;
    private final int port;
    private final String supervisorHost;

    private final String javaLibPath;
    private final String stormJarPath;
    private final String stormRootDir;
    private final String workerPidDir;
    private final String workerTmpDir;
    private final String user;
    private final Map<String, Object> conf;

    public WorkerNodeLoader(Map<String, Object> conf,
                            String supervisorId,
                            String workerId, String supervisorHost, int port) {
        this.conf = conf;
        //this.topoId = topoId;
        this.supervisorId = supervisorId;
        this.supervisorHost = supervisorHost;

        this.workerId = workerId;
        this.port = port;

        this.stormJarPath = getWorkerJarPath(conf, workerId);

        this.user = System.getProperty("user")==null?"guest":System.getProperty("user");
        this.stormRootDir = System.getProperty(STORM_ROOT_DIR);
        if (this.stormRootDir == null) {
            throw new RuntimeException("storm root dir is null, should set with -Dstorm.root.dir");
        }

        this.workerTmpDir = getWorkerTmpPath(conf, workerId);
        this.workerPidDir = getWorkerPidPath(conf, workerId);
        this.javaLibPath = getJavaLibraryPath(conf, this.stormRootDir);

        LOG.info(" ========================================= ");
        LOG.info("workerTmpDir : " + workerTmpDir);
        LOG.info("workerPidDir : " + workerPidDir);
        LOG.info("javaLibPath : " + javaLibPath);
        LOG.info(" ========================================= ");
    }

    public void init() throws IOException {
        FileUtils.forceMkdir(new File(workerTmpDir));
        FileUtils.forceMkdir(new File(workerPidDir));
        FileUtils.forceMkdir(new File(stormJarPath));
    }

    public void loadWorkProcess(Map<String, String> env) throws IOException {
        List<String> loadCmds = mkLoadCommand();

        String workerDir = workerTmpDir;
        String scriptFile = writeScript(workerDir, loadCmds, env);
        System.out.println("**************** worker to run script file name  ****************");
        System.out.println(scriptFile);

        String cmdstring = "chmod a+x "+"/"+scriptFile; //+workerDir
        Process proc = Runtime.getRuntime().exec(cmdstring);
        try {
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        List<String> commands = new ArrayList<>();
        commands.add("sh");
        commands.add(scriptFile);
        //commands.add(getWorkerLaunchBinPath());
        //commands.add(user);
        //commands.addAll(args);

        File targetDir = new File(workerDir);

        String prefix = "********** onWorker: ";
        startProcess(commands, env, prefix, new ExitCodeCallback() {
            @Override
            public void call(int exitCode) {
                LOG.error("************* running worker exitcode : " + exitCode);
            }
        }, targetDir);

        //LOG.info("********************* complete worker script ***************** ");
    }

    public static Process startProcess(List<String> command,
                                        Map<String,String> environment,
                                        final String logPrefix,
                                        final ExitCodeCallback exitCodeCallback,
                                        File dir)
            throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        Map<String,String> procEnv = builder.environment();
        if (dir != null) {
            builder.directory(dir);
        }
        builder.redirectErrorStream(true);
        if (environment != null) {
            procEnv.putAll(environment);
        }
        final Process process = builder.start();
        if (logPrefix != null || exitCodeCallback != null) {
            Utils.asyncLoop(new Callable<Object>() {
                public Object call() {
                    if (logPrefix != null ) {
                        Utils.readAndLogStream(logPrefix,
                                process.getInputStream());
                    }
                    if (exitCodeCallback != null) {
                        try {
                            process.waitFor();
                            exitCodeCallback.call(process.exitValue());
                        } catch (InterruptedException ie) {
                            LOG.info("{} interrupted", logPrefix);
                            exitCodeCallback.call(-1);
                        }
                    }
                    return null; // Run only once.
                }
            });
        }
        return process;
    }

    public String getWorkerLaunchBinPath() {
        return stormRootDir + "/bin/worker-launcher";
    }

    private List<String> mkLoadCommand() throws IOException {
        final String javaCmd = javaCmd("java");
        List<String> classPathParams = getClassPaths();

        List<String> commandList = new ArrayList<>();
        //Worker Command...
        commandList.add(javaCmd);
        commandList.add("-server");
        commandList.add("-Djava.library.path=" + javaLibPath);
        commandList.add("-Djava.io.tmpdir=" + workerTmpDir);
        commandList.addAll(classPathParams);
        commandList.add("org.apache.fake.storm.supervisor.worker.WorkerMain");
        //commandList.add(topoId);
        commandList.add(supervisorId);
        commandList.add(supervisorHost);
        commandList.add(String.valueOf(port));
        commandList.add(workerId);

        return commandList;
    }

    private List<String> getClassPaths() {
        List<String> classPathParams = new ArrayList<>();
        classPathParams.add("-cp");
        classPathParams.add(stormClasspath());
        return classPathParams;
    }

    protected String stormClasspath() {
        String cps = ClusterConfigUtil.getFakeStormJarPath(conf);

        String stormjar = getWorkerJarFilename(conf, workerId);

        cps = cps + ":" + stormjar;

        //File stormLibDir = new File(stormRootDir, "lib");
        //List<String> t1 = getFullJars(stormLibDir);
        //for (String t1s : t1) {
        //    cps += ":" + t1s;
        //}

        List<String> pathElements = new LinkedList<>();
        File stormExtlibDir = new File(stormRootDir, "extlib");
        List<String> t1 = getFullJars(stormExtlibDir);
        for (String t1s : t1) {
            cps += ":" + t1s;
        }

        String stormConfDir = new File(stormRootDir, "conf").getAbsolutePath();
        cps += ":" + stormConfDir;

        return cps;
    }

    protected List<String> getFullJars(File dir) {
        File[] files = dir.listFiles(jarFilter);

        if (files == null) {
            return Collections.emptyList();
        }
        ArrayList<String> ret = new ArrayList<>(files.length);
        for (File f: files) {
            ret.add(f.getAbsolutePath());
        }
        return ret;
    }

    protected String javaCmd(String cmd) {
        String ret = null;
        String javaHome = System.getenv().get("JAVA_HOME");
        if ((javaHome != null) && (javaHome.length() > 0)) {
            ret = javaHome + FILE_PATH_SEPARATOR + "bin" + FILE_PATH_SEPARATOR + cmd;
        } else {
            ret = cmd;
        }
        return ret;
    }
}
