package org.example;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author tangwu
 * @data 2023/12/25
 * @apiNote
 */
public class Main {
    // 文件路径
    private static final String TMP_DIR = ".";

    public static void main(String[] args) throws IOException {
        // 2gb内存，5000w条数据，如果每条数据《40字节，不到2gb的话，可以直接读取到内存中就地排序
        long start = System.currentTimeMillis();
        generateCSV(TMP_DIR + "\\source.csv", 50_000_000);
        long end = System.currentTimeMillis();
        System.out.println("生成csv文件耗时：" + (end - start) + "ms");

        Path input = Paths.get(TMP_DIR + "\\source.csv");
        Path output = Paths.get(TMP_DIR + "\\id.csv");
        ExternalSort.sortLargeFile(input, output, (o1, o2) -> {
            int before = Integer.parseInt(o1.split(",")[0]);
            int after = Integer.parseInt(o2.split(",")[0]);
            return Integer.compare(before, after);
        });
        long end2 = System.currentTimeMillis();
        System.out.println("总共耗时：" + (end2 - start) + "ms");

        output = Paths.get(TMP_DIR + "\\name.csv");
        ExternalSort.sortLargeFile(input, output, (o1, o2) -> {
            String before = o1.split(",")[1];
            String after = o2.split(",")[1];
            if (before.equals(after)) {
                return 0;
            }
            if (before.compareTo(after) > 0) {
                return 1;
            }
            return -1;
        });
        long end3 = System.currentTimeMillis();
        System.out.println("总共耗时：" + (end3 - end2) + "ms");
        System.exit(0);
    }


    //#region 生成csv文件
    public static void generateCSV(String filename, int rows) {
        try {
            RandomAccessFile file = new RandomAccessFile(filename, "rw");
            FileChannel channel = file.getChannel();
            int blockSize = 100000; // number of rows per block
            long position = 0;
            for (int b = 0; b < rows; b += blockSize) {
                int rowsInThisBlock = Math.min(blockSize, rows - b);
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, rowsInThisBlock * 50L);
                for (int i = 0; i < rowsInThisBlock; i++) {
                    int id = getRandomInt(1, 1000000000);
                    String name = getRandomName();
                    String address = getRandomAddress();
                    String record = id + "," + name + "," + address + "\n";
                    position += record.getBytes().length;
                    buffer.put(record.getBytes());
                }
            }
            channel.close();
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int getRandomInt(int min, int max) {
        // 模拟雪花算法，实际上id可能重复。如果要满足不重复的条件且不使用雪花的话
        // 1.hashset去重,5000w个int的hashset要1g多内存不可以
        // 2.布隆过滤器
        Random random = new Random();
        return random.nextInt(max - min + 1) + min;
    }

    private static String getRandomName() {
        String[] names = {"aaa", "bbb", "ccc", "ddd"};
        Random random = new Random();
        return names[random.nextInt(names.length)];
    }

    private static String getRandomAddress() {
        String[] addresses = {"town", "city", "province"};
        Random random = new Random();
        return addresses[random.nextInt(addresses.length)];
    }
    //#endregion


}