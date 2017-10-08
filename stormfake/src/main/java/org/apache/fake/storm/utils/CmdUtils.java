package org.apache.fake.storm.utils;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fake.storm.utils.ClusterConfigUtil.FILE_PATH_SEPARATOR;

/**
 * Created by yilong on 2017/9/14.
 */

public class CmdUtils {
    public static String writeScript(String dir, List<String> command,
                                     Map<String,String> environment) throws IOException {
        String path = scriptFilePath(dir);
        File scfile = new File(path);
        if (scfile.exists()) {
            scfile.delete();
        }

        try(BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            out.write("#!/bin/bash");
            out.newLine();
            if (environment != null) {
                for (String k : environment.keySet()) {
                    String v = environment.get(k);
                    if (v == null) {
                        v = "";
                    }
                    out.write(shellCmd(
                            Arrays.asList(
                                    "export",k+"="+v)));
                    out.write(";");
                    out.newLine();
                }
            }
            out.newLine();
            out.write("exec " + shellCmd(command)+";");
        }
        return path;
    }

    public static String shellCmd (List<String> command) {
        List<String> changedCommands = new ArrayList<>(command.size());
        for (String str: command) {
            if (str == null) {
                continue;
            }
            changedCommands.add("'" + str.replaceAll("'", "'\"'\"'") + "'");
        }
        return StringUtilsEx.join(changedCommands, " ");
    }

    public static String scriptFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "storm-worker-script.sh";
    }
}
