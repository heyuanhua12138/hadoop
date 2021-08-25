package com.hyh.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class CompressDeCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //compress("d:/mrinput/compress/compress.txt","org.apache.hadoop.io.compress.DefaultCodec");
        //compress("d:/mrinput/compress/compress.txt","org.apache.hadoop.io.compress.GzipCodec");
        //compress("d:/mrinput/compress/compress.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        deCompress("d:/mrinput/compress/compress.txt.bz2");
    }

    public static void compress(String file, String compress) throws IOException, ClassNotFoundException {
        String fileName = file;
        String compressClass = compress;
        FileInputStream fileInputStream = null;
        FileOutputStream fileOutputStream = null;
        CompressionOutputStream compressionOutputStream = null;
        try {
            //获取输入流
            fileInputStream = new FileInputStream(new File(fileName));
            Class codec = Class.forName(compressClass);
            //获取输出流
            CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codec, new Configuration());
            fileOutputStream = new FileOutputStream(new File(file + compressionCodec.getDefaultExtension()));
            //compressionCodec.createOutputStream(new OutputStream(new File("1")));
            compressionOutputStream = compressionCodec.createOutputStream(fileOutputStream);
            //流的对拷
            IOUtils.copyBytes(fileInputStream, compressionOutputStream, 1024 * 1024 * 5, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //关流
            fileInputStream.close();
            compressionOutputStream.close();
            fileOutputStream.close();
        }
    }

    //解压缩
    public static void deCompress(String file) throws IOException {
        String fileName = file;
        //校验是否能够解压缩
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec compressionCodec = compressionCodecFactory.getCodec(new Path(fileName));
        if (compressionCodec == null) {
            return;
        }
        //FileInputStream fileInputStream = null;
        CompressionInputStream compressionInputStream = null;
        FileOutputStream fileOutputStream = null;
        try {
            //获取输入流
            //fileInputStream = new FileInputStream(new File(fileName));
            compressionInputStream = compressionCodec.createInputStream(new FileInputStream(new File(fileName)));
            //获取输出流
            fileOutputStream = new FileOutputStream(new File(fileName + ".decompress"));
            IOUtils.copyBytes(compressionInputStream, fileOutputStream, 1024 * 1024 * 5, false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            compressionInputStream.close();
            fileOutputStream.close();
        }
    }

}
