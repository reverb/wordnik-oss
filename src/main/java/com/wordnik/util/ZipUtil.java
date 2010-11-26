package com.wordnik.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtil {
	public static void createArchive(String outputFilename, String subDir, String ... inputFiles) throws IOException {
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outputFilename));
        if(subDir != null){
        	subDir = subDir + "/";
        }
        else{
        	subDir = "";
        }
        byte[] buf = new byte[1024];

        List<File> files = new ArrayList<File>();
        for(String inputFile : inputFiles){
        	files.add(new File(inputFile));
        }
        for (int i=0; i<files.size(); i++) {
        	File file = files.get(i);
        	
            FileInputStream in = new FileInputStream(file);
            
            out.putNextEntry(new ZipEntry(subDir + file.getName()));
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            out.closeEntry();
            in.close();
        }
        out.close();
	}

	public static void createArchive(String outputFilename, String subDir, List<String> inputFiles) throws IOException {
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outputFilename));
        if(subDir != null){
        	subDir = subDir + "/";
        }
        else{
        	subDir = "";
        }
        byte[] buf = new byte[1024];

        List<File> files = new ArrayList<File>();
        for(String inputFile : inputFiles){
        	files.add(new File(inputFile));
        }
        for (int i=0; i<files.size(); i++) {
        	File file = files.get(i);
        	
            FileInputStream in = new FileInputStream(file);
            
            out.putNextEntry(new ZipEntry(subDir + file.getName()));
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            out.closeEntry();
            in.close();
        }
        out.close();
	}
}