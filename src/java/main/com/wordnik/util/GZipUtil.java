package com.wordnik.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GZipUtil {
	public static void createArchive(String outputFilename, String inputFile) throws IOException {
		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(outputFilename));
        byte[] buf = new byte[1024];

    	File file = new File(inputFile);

        FileInputStream in = new FileInputStream(file);

        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
	}
}
