package org.kin.hbase.core.op.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.kin.framework.JvmCloseCleaner;
import org.kin.hbase.core.HBasePool;
import org.kin.hbase.core.domain.Page;
import org.kin.hbase.core.domain.ScannerStatus;
import org.kin.hbase.core.exception.IllegalScannerStatusException;
import org.kin.hbase.core.op.AbstractQueryOp;
import org.kin.hbase.core.utils.HBaseUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * @author huangjianqin
 * @date 2018/5/24
 */
public class ScanOp extends AbstractQueryOp<ScanOp> {
    private final Scan scan;

    public ScanOp(String tableName) {
        super(tableName);
        scan = new Scan();
    }

    public ScanOp(String tableName, String startRow, String stopRow) {
        this(tableName);
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
    }

    //-------------------------------------------------------------------------------------------------------
    //一些属性设置
    public ScanOp cache(int cacheSize) {
        scan.setCaching(cacheSize);
        return this;
    }

    public ScanOp batchSize(int batchSize) {
        scan.setBatch(batchSize);
        return this;
    }

    public ScanOp cacheBlocks() {
        scan.setCacheBlocks(true);
        return this;
    }

    public ScanOp startRow(String startRow) {
        scan.withStartRow(Bytes.toBytes(startRow));
        return this;
    }

    public ScanOp stopRow(String stopRow) {
        scan.withStopRow(Bytes.toBytes(stopRow));
        return this;

    }

    public ScanOp maxVersions(int version) {
        scan.readAllVersions();
        return this;
    }

    public ScanOp maxResultSize(long limit) {
        scan.setMaxResultSize(limit);
        return this;
    }

    public ScanOp maxResultsPerColumnFamily(int limit) {
        scan.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    public ScanOp rowOffsetPerFamily(int offset) {
        scan.setRowOffsetPerColumnFamily(offset);
        return this;
    }

    public ScanOp timeRange(String family, long minTimeStamp, long maxTimeStamp) {
        scan.setColumnFamilyTimeRange(Bytes.toBytes(family), minTimeStamp, maxTimeStamp);
        return this;
    }

    public ScanOp timeRange(long minTimeStamp, long maxTimeStamp) {
        try {
            scan.setTimeRange(minTimeStamp, maxTimeStamp);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return this;
    }

    public ScanOp timeStamp(long timeStamp) {
        scan.setTimestamp(timeStamp);
        return this;
    }

    //------------------------------------------过滤器-------------------------------------------------------------
    @Override
    public ScanOp includeStop(String stopRowKey) {
        ScanOp op = super.includeStop(stopRowKey);

        //这里设置了stop row, InclusiveStopFilter就会没效
        scan.withStopRow(null);

        return op;
    }


    //-------------------------------------------query操作------------------------------------------------------------
    /**
     * 分批取
     */
    public <T> Scanner scan() {
        ResultScanner scanner = scanner();
        if (scanner != null) {
            return new Scanner(scanner);
        }

        return null;
    }

    /**
     * 一次取完
     */
    public <T> List<T> batch(Class<T> entityClaxx) {
        ResultScanner scanner = scanner();
        if (scanner != null) {
            return new Scanner(scanner).batch(entityClaxx);
        }

        return Collections.emptyList();
    }

    /**
     * 分页获取
     *
     * @param pageNo 从1开始
     */
    public <T> Page<T> page(Class<T> entityClaxx, int pageSize, int pageNo) {
        int offset = (pageNo - 1) * pageSize;
        rowOffsetPerFamily(offset);

        List<T> entities = batch(entityClaxx);
        return new Page<>(pageSize, pageNo, entities);
    }

    //-------------------------------------------------------------------------------------------------------
    public static class Scanner implements Closeable {
        private ScannerStatus scannerStatus = ScannerStatus.INIT;
        private final ResultScanner scanner;

        public Scanner(ResultScanner scanner) {
            this.scanner = scanner;
        }

        /**
         * 分批取
         * <p>
         * 最好设置Scan实例的batch值大点，增加一次请求获取数据的数量
         */
        public <T> List<T> scan(Class<T> entityClaxx, int batchSize) {
            if (scannerStatus.equals(ScannerStatus.CLOSED)) {
                throw new IllegalScannerStatusException("scanner has been closed");
            }

            List<T> objs = new ArrayList<>();
            try {
                Result[] results = scanner.next(batchSize);
                if (results != null && results.length > 0) {
                    for (Result result : results) {
                        objs.add(HBaseUtils.convert2HBaseEntity(entityClaxx, result));
                    }
                } else {
                    close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

            return objs;
        }

        /**
         * 一次取完
         */
        public <T> List<T> batch(Class<T> entitiyClaxx) {
            if (scannerStatus.equals(ScannerStatus.CLOSED)) {
                throw new IllegalScannerStatusException("scanner has been closed");
            }
            List<T> objs = new ArrayList<>();
            Result result;
            try {
                while ((result = scanner.next()) != null) {
                    objs.add(HBaseUtils.convert2HBaseEntity(entitiyClaxx, result));
                }
                close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

            return objs;
        }

        @Override
        public void close() throws IOException {
            if (scanner != null) {
                scanner.close();
                scannerStatus = ScannerStatus.CLOSED;
            }
        }
    }
    //-------------------------------------------------------------------------------------------------------

    /**
     * 外部维护关闭ResultScanner
     */
    private ResultScanner scanner() {
        try (Connection connection = HBasePool.common().getConnection()) {
            Table table = connection.getTable(TableName.valueOf(getTableName()));

            HBaseUtils.setColumnAndFilter(scan, getQueryInfos(), getFilters());

            ResultScanner scanner = table.getScanner(scan);

            table.close();

            //保证程序关闭, 所有Scanner都关闭
            JvmCloseCleaner.instance().add(scanner::close);

            return scanner;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    //getter
    public Map<String, Set<String>> getColumns() {
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
            String family = new String(entry.getKey());
            result.put(family, new HashSet<>());

            for (byte[] qualifierBytes : entry.getValue()) {
                String qualifier = new String(qualifierBytes);
                result.get(family).add(qualifier);
            }
        }

        return result;
    }

    public int getCacheSize() {
        return scan.getCaching();
    }

    public int getBatchSize() {
        return scan.getBatch();
    }

    public boolean isCacheBlocks() {
        return scan.getCacheBlocks();
    }

    public String getStartRow() {
        return new String(scan.getStartRow());
    }

    public String getStopRow() {
        return new String(scan.getStopRow());

    }

    public int getMaxVersions() {
        return scan.getMaxVersions();
    }

    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }

    public int getMaxResultsPerColumnFamily() {
        return scan.getMaxResultsPerColumnFamily();
    }

    public int getRowOffsetPerFamily() {
        return scan.getRowOffsetPerColumnFamily();
    }

    public TimeRange getTimeRange() {
        return scan.getTimeRange();
    }

    public Map<String, TimeRange> getFamilyTimeRange() {
        Map<String, TimeRange> result = new HashMap<>();
        for (Map.Entry<byte[], TimeRange> entry : scan.getColumnFamilyTimeRange().entrySet()) {
            String family = new String(entry.getKey());
            TimeRange timeRange = entry.getValue();

            result.put(family, timeRange);
        }
        return result;
    }
}
