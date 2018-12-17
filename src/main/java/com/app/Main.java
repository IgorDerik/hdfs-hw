package com.app;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","C:\\hadoop" );

        Utils.convertCsvToParquet(args[0],args[1]);

    /*
        try {
            Utils.showFiles("hdfs://localhost:8020","/user/admin/EHR");
        } catch (IOException e) {
            e.printStackTrace();
        }
    */
    }

}
