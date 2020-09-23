package com.alibaba.otter.canal.parse.inbound.mysql.local;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.alibaba.otter.canal.parse.exception.CanalParseException;

/**
 * 维护binlog文件列表
 * 
 * @author jianghang 2012-7-7 下午03:48:05
 * @version 1.0.0
 */
public class BinLogFileQueue {

    private String              baseName       = "mysql-bin.";
    private List<File>          binlogs        = new ArrayList<>();
    private File                directory;
    private ReentrantLock       lock           = new ReentrantLock();
    private Condition           nextCondition  = lock.newCondition();
    private Timer               timer          = new Timer(true);
    private long                reloadInterval = 10 * 1000L;           // 10秒
    private CanalParseException exception      = null;

    public BinLogFileQueue(String directory){
        this(new File(directory));
    }

    public BinLogFileQueue(File directory){
        this.directory = directory;

        if (!directory.canRead()) {
            throw new CanalParseException("Binlog index missing or unreadable;  " + directory.getAbsolutePath());
        }

        List<File> files = listBinlogFiles();
        for (File file : files) {
            offer(file);
        }

        timer.scheduleAtFixedRate(new TimerTask() {

            public void run() {
                try {
                    // File errorFile = new File(BinLogFileQueue.this.directory,
                    // errorFileName);
                    // if (errorFile.isFile() && errorFile.exists()) {
                    // String text = StringUtils.join(IOUtils.readLines(new
                    // FileInputStream(errorFile)), "\n");
                    // exception = new CanalParseException(text);
                    // }
                    List<File> files = listBinlogFiles();
                    for (File file : files) {
                        offer(file);
                    }
                } catch (Throwable e) {
                    exception = new CanalParseException(e);
                }

                if (exception != null) {
                    offer(null);
                }
            }
        }, reloadInterval, reloadInterval);
    }

    /**
     * 根据前一个文件，获取符合条件的下一个binlog文件
     * 
     * @param pre
     * @return
     */
    public File getNextFile(File pre) {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }

            if (binlogs.size() == 0) {
                return null;
            } else {
                if (pre == null) {// 第一次
                    return binlogs.get(0);
                } else {
                    int index = seek(pre);
                    if (index < binlogs.size() - 1) {
                        return binlogs.get(index + 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public File getBefore(File file) {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }

            if (binlogs.size() == 0) {
                return null;
            } else {
                if (file == null) {// 第一次
                    return binlogs.get(binlogs.size() - 1);
                } else {
                    int index = seek(file);
                    if (index > 0) {
                        return binlogs.get(index - 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据前一个文件，获取符合条件的下一个binlog文件
     * 
     * @param pre
     * @return
     * @throws InterruptedException
     */
    public File waitForNextFile(File pre) throws InterruptedException {
        try {
            lock.lockInterruptibly();
            if (binlogs.size() == 0) {
                nextCondition.await();// 等待新文件
            }

            if (exception != null) {
                throw exception;
            }
            if (pre == null) {// 第一次
                return binlogs.get(0);
            } else {
                int index = seek(pre);
                if (index < binlogs.size() - 1) {
                    return binlogs.get(index + 1);
                } else {
                    nextCondition.await();// 等待新文件
                    return waitForNextFile(pre);// 唤醒之后递归调用一下
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前所有binlog文件
     */
    public List<File> currentBinlogs() {
        return new ArrayList<>(binlogs);
    }

    public void destory() {
        try {
            lock.lockInterruptibly();
            timer.cancel();
            binlogs.clear();

            nextCondition.signalAll();// 唤醒线程，通知退出
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private boolean offer(File file) {
        try {
            lock.lockInterruptibly();
            if (file != null) {
                if (!binlogs.contains(file)) {
                    binlogs.add(file);
                    nextCondition.signalAll();// 唤醒
                    return true;
                }
            }

            nextCondition.signalAll();// 唤醒
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    private List<File> listBinlogFiles() {
        List<File> files = new ArrayList<>();
        files.addAll(FileUtils.listFiles(directory, new IOFileFilter() {

            public boolean accept(File file) {
                Pattern pattern = Pattern.compile("\\d+$");
                Matcher matcher = pattern.matcher(file.getName());
                return file.getName().startsWith(baseName) && matcher.find();
            }

            public boolean accept(File dir, String name) {
                return true;
            }
        }, null));
        // 排一下序列
        files.sort(Comparator.comparing(File::getName));
        return files;
    }

    private int seek(File file) {
        for (int i = 0; i < binlogs.size(); i++) {
            File binlog = binlogs.get(i);
            if (binlog.getName().equals(file.getName())) {
                return i;
            }
        }

        return -1;
    }

    // ================== setter / getter ===================

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }
}
