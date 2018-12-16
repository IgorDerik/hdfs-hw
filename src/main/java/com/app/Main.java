package com.app;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop" );

//        Utils.convertCsvToParquet(args[0],args[1]);
//        Utils.convertCsvToParquet("hdfs://localhost:8020/user/admin/matches.csv","hdfs://localhost:8020/user/admin/par111");

        try {
            Utils.showRootHDFS();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
