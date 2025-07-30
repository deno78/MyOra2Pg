import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 任意の移行元DBからPostgreSQLへの高速データ移行ツール
 * CSV出力とCOPYコマンドを使用した効率的な移行処理
 */
public class PostgresMigrator {
    /**
     * 接続元DBの接続確認
     */
    public boolean checkSrcConnection(String url, String user, String pass, String driver) {
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, pass);
            conn.close();
            return true;
        } catch (Exception e) {
            log("接続元DB接続失敗: " + e.getMessage());
            return false;
        }
    }

    /**
     * 接続先(PostgreSQL)の接続確認
     */
    public boolean checkPostgresConnection(String url, String user, String pass, String driver) {
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, pass);
            conn.close();
            return true;
        } catch (Exception e) {
            log("接続先DB接続失敗: " + e.getMessage());
            return false;
        }
    }
    // 外部参照用: 未処理・処理中・完了テーブル/結果
    public List<String> pendingTables = Collections.synchronizedList(new ArrayList<>());
    public List<String> processingTables = Collections.synchronizedList(new ArrayList<>());
    public List<MigrationResult> completedResults = Collections.synchronizedList(new ArrayList<>());
    public List<MigrationResult> failedResults = Collections.synchronizedList(new ArrayList<>());
    private PrintWriter logWriter;
    private String logFilePath;

    /**
     * 標準出力とログファイル両方に出力する
     */
    private void log(String msg) {
        System.out.println(msg);
        try {
            if (logWriter == null) {
                // logs/yyyymmdd.log 追記型
                String dateStr = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
                logFilePath = "logs/" + dateStr + ".log";
                logWriter = new PrintWriter(new FileWriter(logFilePath, true));
            }
            logWriter.println(msg);
            logWriter.flush();
        } catch (Exception e) {
            System.err.println("ログ出力エラー: " + e.getMessage());
        }
    }

    // 設定値
    private static final String DB_PROPERTIES_FILE = "db.properties";
    private static final String CSV_OUTPUT_DIR = "./migration_csv/";
    private static final String TABLE_LIST_FILE = "table_list.txt";
    private static final int FETCH_SIZE = 10000;
    private static final int DEFAULT_THREAD_POOL_SIZE = 4;
    private static final int DEFAULT_STREAM_BUFFER_SIZE = 8192;

    private int streamBufferSize = DEFAULT_STREAM_BUFFER_SIZE;

    private String srcUrl;
    private String srcUser;
    private String srcPassword;
    private String postgresUrl;
    private String postgresUser;
    private String postgresPassword;
    private ExecutorService executor;
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    
    public static void main(String[] args) {
        PostgresMigrator migrator = new PostgresMigrator();
        try {
            migrator.log("\n--- InMemory COPY mode ---");
            long startCopy = System.currentTimeMillis();
            migrator.migrateInMemory();
            long elapsedCopy = System.currentTimeMillis() - startCopy;
            migrator.log(String.format("[InMemory COPY] Total elapsed time: %.2f sec", elapsedCopy / 1000.0));
        } catch (Exception e) {
            migrator.log("Migration error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 初期化処理
     */
    private void initialize() throws SQLException {
        System.out.println("Initializing thread pool...");
        executor = Executors.newFixedThreadPool(threadPoolSize);
        System.out.println("Initialization complete");
    }

    /**
     * DB接続情報をdb.propertiesから読み込む
     */
    private void loadDbProperties() throws IOException {
        Properties props = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(DB_PROPERTIES_FILE);
            props.load(inputStream);
        } finally {
            if (inputStream != null) inputStream.close();
        }
        srcUrl = props.getProperty("src.url");
        srcUser = props.getProperty("src.user");
        srcPassword = props.getProperty("src.password");
        postgresUrl = props.getProperty("postgres.url");
        postgresUser = props.getProperty("postgres.user");
        postgresPassword = props.getProperty("postgres.password");

        // Load thread count if present
        String threadCountStr = props.getProperty("thread.count");
        if (threadCountStr != null) {
            try {
                threadPoolSize = Integer.parseInt(threadCountStr.trim());
            } catch (Exception e) {
                System.err.println("Invalid thread.count in properties. Using default: " + DEFAULT_THREAD_POOL_SIZE);
                threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
            }
        }

        // Load stream buffer size if present
        String bufferSizeStr = props.getProperty("stream.buffer.size");
        if (bufferSizeStr != null) {
            try {
                streamBufferSize = Integer.parseInt(bufferSizeStr.trim());
            } catch (Exception e) {
                log("Invalid stream.buffer.size in properties. Using default: " + DEFAULT_STREAM_BUFFER_SIZE);
                streamBufferSize = DEFAULT_STREAM_BUFFER_SIZE;
            }
        }
    }
    
    /**
     * テーブルリスト読み込み (CSVファイルから1列目をテーブル名として取得)
     */
    private List<String> loadTableList() throws IOException {
        List<String> tableList = new ArrayList<>();
        Path filePath = Paths.get(TABLE_LIST_FILE);
        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("Table list file not found: " + TABLE_LIST_FILE);
        }
        BufferedReader reader = null;
        try {
            reader = Files.newBufferedReader(filePath);
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    // CSVの1列目のみ取得
                    String[] parts = line.split(",");
                    if (parts.length > 0 && !parts[0].isEmpty()) {
                        tableList.add(parts[0].trim());
                    }
                }
            }
        } finally {
            if (reader != null) reader.close();
        }
        return tableList;
    }
    
    /**
     * クリーンアップ処理
     */
    private void cleanup() {
        try {
            if (executor != null) {
                executor.shutdown();
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            }
            if (logWriter != null) {
                logWriter.close();
                logWriter = null;
            }
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    /**
     * Oracle→PostgreSQLへCSVファイルを介さず直接ストリームでCOPYするメモリ高速移行
     */
    public void migrateInMemory() throws Exception {
        long startMillis = System.currentTimeMillis();
        String startTimeStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        log("=== 任意の移行元 → PostgreSQL In-Memory Data Migration Start ===");
        log("Start time: " + startTimeStr);

        try {
            loadDbProperties();
            initialize();

            List<String> tableList = loadTableList();
            log("Loaded tables: " + tableList);
            log("Number of tables to migrate: " + tableList.size());

            // 外部参照用リスト初期化
            pendingTables.clear();
            processingTables.clear();
            completedResults.clear();
            failedResults.clear();
            pendingTables.addAll(tableList);

            int successCount = 0;
            int failureCount = 0;
            long totalRecords = 0;

            // 並列実行用タスク作成
            List<Callable<MigrationResult>> tasks = new ArrayList<>();
            for (String tableName : tableList) {
                final String tn = tableName;
                final String srcUrl_ = srcUrl;
                final String srcUser_ = srcUser;
                final String srcPassword_ = srcPassword;
                final String postgresUrl_ = postgresUrl;
                final String postgresUser_ = postgresUser;
                final String postgresPassword_ = postgresPassword;
                final int streamBufferSize_ = streamBufferSize;
                tasks.add(new Callable<MigrationResult>() {
                    public MigrationResult call() {
                        synchronized (pendingTables) { pendingTables.remove(tn); }
                        synchronized (processingTables) { processingTables.add(tn); }
                        MigrationResult result = migrateTableInMemory(
                            tn, srcUrl_, srcUser_, srcPassword_,
                            postgresUrl_, postgresUser_, postgresPassword_,
                            streamBufferSize_
                        );
                        synchronized (processingTables) { processingTables.remove(tn); }
                        if (result.success) {
                            synchronized (completedResults) { completedResults.add(result); }
                        } else {
                            synchronized (failedResults) { failedResults.add(result); }
                        }
                        return result;
                    }
                });
            }

            // タスク並列実行
            List<Future<MigrationResult>> futures = executor.invokeAll(tasks);
            for (Future<MigrationResult> future : futures) {
                MigrationResult result = future.get();
                if (result.success) {
                    successCount++;
                    totalRecords += result.recordCount;
                    log(String.format("? %s: %d records (%.2f sec)",
                        result.tableName, result.recordCount, result.elapsedSeconds));
                } else {
                    failureCount++;
                    log("? " + result.tableName + ": " + result.errorMessage);
                }
            }

            String endTimeStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            double elapsedSec = elapsedMillis / 1000.0;
            log("\n=== Migration Result ===");
            log("Success: " + successCount + " tables");
            log("Failure: " + failureCount + " tables");
            log("Total records: " + totalRecords);
            log("End time: " + endTimeStr);
            log(String.format("Total elapsed time: %.2f sec", elapsedSec));

        } finally {
            cleanup();
        }
    }

    /**
     * Oracle→PostgreSQLへ直接ストリームでCOPY投入（CSVファイルを作らない）
     */
    private MigrationResult migrateTableInMemory(
        String tableName,
        String srcUrl,
        String srcUser,
        String srcPassword,
        String postgresUrl,
        String postgresUser,
        String postgresPassword,
        int streamBufferSize
    ) {
    long startTime = System.currentTimeMillis();
    MigrationResult result = new MigrationResult(tableName);
    Connection srcConn = null;
    Connection postgresConn = null;
    Statement stmt = null;
        try {
            // 各スレッドでDB接続を新規作成
            srcConn = DriverManager.getConnection(srcUrl, srcUser, srcPassword);
            srcConn.setAutoCommit(false);
            postgresConn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
            postgresConn.setAutoCommit(false);

            // 1. UNLOGGED化 & TRUNCATE target table in PostgreSQL
            stmt = postgresConn.createStatement();
            try {
                stmt.executeUpdate("ALTER TABLE " + tableName + " SET UNLOGGED");
                // ...インデックス無効化処理はPostgreSQLでは不要...

                stmt.executeUpdate("TRUNCATE TABLE " + tableName + " RESTART IDENTITY CASCADE");
                postgresConn.commit();
            } finally {
                if (stmt != null) stmt.close();
            }

            // 2. 移行元DBからSELECTしてPostgreSQLにストリームでCOPY
            String sql = "SELECT * FROM " + tableName;
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                pstmt = srcConn.prepareStatement(sql);
                pstmt.setFetchSize(FETCH_SIZE);
                rs = pstmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // ストリームでCSV成形
                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos, streamBufferSize);
                Thread writerThread = new Thread(new Runnable() {
                    public void run() {
                        try {
                            OutputStreamWriter osw = new OutputStreamWriter(pos, "UTF-8");
                            BufferedWriter bw = new BufferedWriter(osw, streamBufferSize);
                            PrintWriter pw = new PrintWriter(bw);
                            while (rs.next()) {
                                StringBuilder line = new StringBuilder();
                                for (int i = 1; i <= columnCount; i++) {
                                    if (i > 1) line.append(",");
                                    int colType = metaData.getColumnType(i);
                                    String value = "";
                                    if (colType == java.sql.Types.BLOB) {
                                        byte[] bytes = rs.getBytes(i);
                                        if (bytes != null) {
                                            value = java.util.Base64.getEncoder().encodeToString(bytes);
                                        }
                                    } else {
                                        value = rs.getString(i);
                                        if (value != null) {
                                            value = value.replace('\u0000', '');
                                        }
                                    }
                                    // CSV escape
                                    if (value != null) {
                                        value = value.replace('"', '""');
                                        if (value.contains(",") || value.contains("\n") || value.contains('"')) {
                                            value = '"' + value + '"';
                                        }
                                    } else {
                                        value = "";
                                    }
                                    line.append(value);
                                }
                                pw.println(line.toString());
                            }
                            pw.close();
                            bw.close();
                            osw.close();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
                writerThread.start();

                // COPY投入
                org.postgresql.PGConnection pgConn = postgresConn.unwrap(org.postgresql.PGConnection.class);
                org.postgresql.copy.CopyManager copyManager = new org.postgresql.copy.CopyManager(pgConn);
                InputStreamReader reader = new InputStreamReader(pis, "UTF-8");
                long recordCount = copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH (FORMAT csv, ENCODING 'UTF8', HEADER false)", reader);
                postgresConn.commit();
                writerThread.join();

                result.success = true;
                result.recordCount = recordCount;
                result.elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0;

            } finally {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
            }

        } catch (Exception e) {
            result.success = false;
            result.errorMessage = e.getMessage();
        } finally {
            // スレッド毎に接続クローズ

            // ...インデックス有効化処理はPostgreSQLでは不要...
            if (postgresConn != null) {
                Statement stmtLogged = null;
                try {
                    stmtLogged = postgresConn.createStatement();
                    try {
                        stmtLogged.executeUpdate("ALTER TABLE " + tableName + " SET LOGGED");
                        postgresConn.commit();
                    } catch (Exception ex) {
                        log("LOGGED化失敗: " + tableName + " - " + ex.getMessage());
                        try { postgresConn.rollback(); } catch (Exception ignore) {}
                    }
                } catch (Exception ex) {
                    log("LOGGED化処理エラー: " + ex.getMessage());
                } finally {
                    if (stmtLogged != null) stmtLogged.close();
                }
            }
            try { if (srcConn != null && !srcConn.isClosed()) srcConn.close(); } catch (Exception ignore) {}
            try { if (postgresConn != null && !postgresConn.isClosed()) postgresConn.close(); } catch (Exception ignore) {}
        }
        return result;
    }


    /**
     * 移行結果クラス
     */
    private static class MigrationResult {
        String tableName;
        boolean success;
        long recordCount;
        double elapsedSeconds;
        String errorMessage;

        MigrationResult(String tableName) {
            this.tableName = tableName;
        }
    }
}
