// Copyright (C) 2010  Wordnik, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your 
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty 
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser 
// General Public License for more details.  You should have received a copy 
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

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