package org.example;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

public class ExternalSort {
    public static final Charset CHARSET = Charset.defaultCharset();
    // 文件路径
    private static final String TMP_DIR = ".";

    public static void sortLargeFile(Path input, Path output, Comparator<String> comparator) throws IOException {
        List<Path> tempFileList = sortInBatch(input.toFile(), comparator);
        mergeSortedFiles(tempFileList, output, comparator);
    }

    public static List<Path> sortInBatch(File file, Comparator<String> comparator) {
        ArrayList<Path> res = new ArrayList<>();
        try {
            RandomAccessFile myFile = new RandomAccessFile(file, "r");
            FileChannel fc = myFile.getChannel();
            long blockSize = Math.min(estimateBestSizeOfBlocks(file), 500 * 1024 * 1024);
            long fileSize = fc.size();
            long blockCount = fileSize / blockSize + (fileSize % blockSize == 0 ? 0 : 1);
            StringBuffer sb = new StringBuffer();
            for (long i = 0; i < blockCount; i++) {
                ArrayList<String> tmpList = new ArrayList<>();
                long position = i * blockSize;
                long size = Math.min(blockSize, fileSize - position);
                MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                long start = System.currentTimeMillis();
                for (int j = 0; j < size; j++) {
                    char c = (char) mbb.get(j);
                    if (c == 0 || c == ' ') continue;
                    if (c == '\n') {
                        // 可能为空，因为写文件的时候导致的
                        String trim = sb.toString().trim();
                        if (!trim.isEmpty()) {
                            tmpList.add(trim);
                        }
                        sb.setLength(0);
                    } else {
                        sb.append(c);
                    }
                }
                System.out.println("读取文件耗时" + (System.currentTimeMillis() - start));
                res.add(sortAndSave(tmpList, comparator, size));
            }
            fc.close();
            myFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static Path sortAndSave(ArrayList<String> list, Comparator<String> comparator, Long size) throws IOException {
        long start = System.currentTimeMillis();
        list.sort(comparator);
        System.out.println("排序耗时" + (System.currentTimeMillis() - start));
        File newTempFile = File.createTempFile("sortInBatch", "flatfile", new File(TMP_DIR));
        newTempFile.deleteOnExit();
        start = System.currentTimeMillis();
        try {
            RandomAccessFile file = new RandomAccessFile(newTempFile, "rw");
            FileChannel channel = file.getChannel();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size + 100);
            for (String s : list) {
                buffer.put(s.getBytes());
                buffer.put("\n".getBytes());
            }
            channel.close();
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("写入文件耗时" + (System.currentTimeMillis() - start));
        return newTempFile.toPath();
    }

    public static long estimateBestSizeOfBlocks(File file) {
        long sizeOfFile = file.length();
        final int MAX_BLOCKS = Runtime.getRuntime().availableProcessors();
//        final int MAX_BLOCKS = 2;
        long blockSize = sizeOfFile / MAX_BLOCKS;
        long freeMemory = Runtime.getRuntime().freeMemory();
        if (blockSize < freeMemory / 2) {
            blockSize = freeMemory / 2;
        } else {
            if (blockSize >= freeMemory) {
                System.err.println("Not enough memory, please increase the JVM heap size");
                System.exit(1);
            }
        }
        return blockSize;
    }

    public static void mergeSortedFiles(List<Path> files, Path outputfile, Comparator<String> comparator) throws IOException {
        long start = System.currentTimeMillis();
        PriorityQueue<BinaryFileBuffer> queue = new PriorityQueue<>((o1, o2) -> comparator.compare(o1.peek(), o2.peek()));
        for (Path file : files) {
            BinaryFileBuffer bfb = new BinaryFileBuffer(file);
            queue.add(bfb);
        }

        try {
            RandomAccessFile file = new RandomAccessFile(outputfile.toFile(), "rw");
            FileChannel channel = file.getChannel();
            int blockSize = 100000; // number of rows per block
            int position = 0, add = 0;
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, position, blockSize * 50L);
            while (!queue.isEmpty()) {
                BinaryFileBuffer bfb = queue.poll();
                String value = bfb.pop() + "\n";
                mbb.put(value.getBytes());
                position += value.getBytes().length;
                add += value.getBytes().length;
                // 将要超过blockSize的时候
                if ((add + 100) > blockSize) {
                    add = 0;
                    mbb = channel.map(FileChannel.MapMode.READ_WRITE, position, blockSize * 50L);
                }
                if (bfb.empty()) {
                    bfb.close();
                } else {
                    queue.add(bfb);
                }
            }
            channel.close();
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (BinaryFileBuffer bfb : queue) {
            bfb.close();
        }
        System.out.println("合并文件耗时" + (System.currentTimeMillis() - start));
    }
}

class BinaryFileBuffer {
    private BufferedReader reader;
    private String cache;
    private boolean empty;

    public BinaryFileBuffer(Path path) throws IOException {
        reader = Files.newBufferedReader(path, ExternalSort.CHARSET);
        reload();
    }

    public boolean empty() {
        return empty;
    }

    private void reload() throws IOException {
        try {
            if ((this.cache = reader.readLine()) == null || (this.cache = this.cache.trim()).isEmpty()) {
                empty = true;
                reader.close();
            } else {
                empty = false;
            }
        } catch (EOFException oef) {
            empty = true;
            reader.close();
        }
    }

    public void close() throws IOException {
        reader.close();
    }


    public String peek() {
        if (empty()) {
            return null;
        }
        return cache;
    }

    public String pop() throws IOException {
        String answer = peek();
        reload();
        return answer;
    }
}
