import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Main {
    private static final String DEFAULT_KEY = "email";

    public static void main(String[] args) {
        Map<String, String> options = parseArgs(args);
        if (!options.containsKey("previous") || !options.containsKey("current")) {
            printUsage();
            System.exit(2);
        }

        Instant startedAt = Instant.now();
        String previousPath = options.get("previous");
        String currentPath = options.get("current");
        String keyRaw = options.getOrDefault("key", DEFAULT_KEY);
        List<String> keyColumns = parseKeyColumns(keyRaw);
        String keyNormalize = options.getOrDefault("key-normalize", "none");
        String valueNormalize = options.getOrDefault("value-normalize", "none");
        String jsonPath = options.get("json");
        String exportDir = options.get("export-dir");
        boolean exportUnchanged = options.containsKey("export-unchanged");
        boolean exportUpdatedRows = options.containsKey("export-updated-rows");
        boolean exportStatus = options.containsKey("export-status");
        boolean summaryOnly = options.containsKey("summary-only");
        boolean dbLog = options.containsKey("db-log");
        String dbSchema = options.getOrDefault("db-schema", "roster_reconciler");
        String dbApp = options.getOrDefault("db-app", "roster-reconciler");
        int detailLimit = 0;
        Set<String> ignoredFields = parseIgnoredFields(options.get("ignore"));

        try {
            validateKeyNormalize(keyNormalize);
            validateValueNormalize(valueNormalize);
            if (dbLog) {
                validateSchemaName(dbSchema);
            }
            detailLimit = parseDetailLimit(options.get("max-detail"));
            Roster previous = readRoster(Path.of(previousPath), keyColumns, keyNormalize);
            Roster current = readRoster(Path.of(currentPath), keyColumns, keyNormalize);
            Report report = diff(previous, current, keyColumns, ignoredFields, keyNormalize, valueNormalize, summaryOnly, detailLimit);

            String output = report.toText(previousPath, currentPath);
            System.out.println(output);

            if (jsonPath != null && !jsonPath.isBlank()) {
                Files.writeString(Path.of(jsonPath), report.toJson(previousPath, currentPath), StandardCharsets.UTF_8);
            }

            if (exportDir != null && !exportDir.isBlank()) {
                report.writeExports(Path.of(exportDir), exportUnchanged, exportUpdatedRows, exportStatus);
            }
            Instant finishedAt = Instant.now();
            if (dbLog) {
                DbRunOptions runOptions = new DbRunOptions(exportDir, exportUnchanged, exportUpdatedRows, exportStatus, jsonPath);
                DbLogger.logRun(report, previousPath, currentPath, dbApp, dbSchema, keyColumns, keyNormalize, valueNormalize,
                        summaryOnly, detailLimit, runOptions, startedAt, finishedAt);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp out Main --previous <file.csv> --current <file.csv> [--key email] [--key-normalize none|lower|upper] [--value-normalize none|trim|collapse] [--ignore field1,field2] [--max-detail N] [--summary-only] [--json report.json] [--export-dir outdir] [--export-unchanged] [--export-updated-rows] [--export-status] [--db-log] [--db-schema roster_reconciler] [--db-app roster-reconciler]");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                continue;
            }
            String name = arg.substring(2);
            String value = "true";
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                value = args[i + 1];
                i++;
            }
            options.put(name, value);
        }
        return options;
    }

    private static Set<String> parseIgnoredFields(String raw) {
        Set<String> ignored = new HashSet<>();
        if (raw == null || raw.isBlank()) {
            return ignored;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isBlank()) {
                ignored.add(trimmed);
            }
        }
        return ignored;
    }

    private static List<String> parseKeyColumns(String raw) {
        List<String> columns = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            columns.add(DEFAULT_KEY);
            return columns;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isBlank()) {
                columns.add(trimmed);
            }
        }
        if (columns.isEmpty()) {
            columns.add(DEFAULT_KEY);
        }
        return columns;
    }

    private static void validateKeyNormalize(String keyNormalize) throws IOException {
        if (!keyNormalize.equals("none") && !keyNormalize.equals("lower") && !keyNormalize.equals("upper")) {
            throw new IOException("Invalid --key-normalize value: " + keyNormalize + " (use none|lower|upper)");
        }
    }

    private static void validateValueNormalize(String valueNormalize) throws IOException {
        if (!valueNormalize.equals("none") && !valueNormalize.equals("trim") && !valueNormalize.equals("collapse")) {
            throw new IOException("Invalid --value-normalize value: " + valueNormalize + " (use none|trim|collapse)");
        }
    }

    private static void validateSchemaName(String schema) throws IOException {
        if (schema == null || schema.isBlank()) {
            throw new IOException("Invalid --db-schema value: cannot be blank");
        }
        if (!schema.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IOException("Invalid --db-schema value: " + schema + " (use letters, numbers, underscores)");
        }
    }

    private static int parseDetailLimit(String raw) throws IOException {
        if (raw == null || raw.isBlank()) {
            return 0;
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 0) {
                throw new IOException("Invalid --max-detail value: " + raw + " (must be >= 0)");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid --max-detail value: " + raw + " (must be an integer)", e);
        }
    }

    static String normalizeKeyValue(String value, String keyNormalize) {
        if (value == null) {
            return "";
        }
        return switch (keyNormalize) {
            case "lower" -> value.toLowerCase();
            case "upper" -> value.toUpperCase();
            default -> value;
        };
    }

    static String normalizeFieldValue(String value, String valueNormalize) {
        if (value == null) {
            return "";
        }
        return switch (valueNormalize) {
            case "trim" -> value.trim();
            case "collapse" -> value.trim().replaceAll("\\s+", " ");
            default -> value;
        };
    }

    static Roster readRoster(Path path, List<String> keyColumns, String keyNormalize) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("CSV is empty: " + path);
        }

        List<String> header = parseCsvLine(lines.get(0));
        List<String> missingKeys = new ArrayList<>();
        for (String keyColumn : keyColumns) {
            if (!header.contains(keyColumn)) {
                missingKeys.add(keyColumn);
            }
        }
        if (!missingKeys.isEmpty()) {
            throw new IOException("Key column(s) " + String.join(", ", missingKeys) + " not found in: " + path);
        }
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        Map<String, Integer> nonEmptyCounts = new LinkedHashMap<>();
        Map<String, Integer> missingKeyCounts = new LinkedHashMap<>();
        int duplicates = 0;
        int invalid = 0;
        int totalRows = 0;
        List<String> duplicateKeys = new ArrayList<>();
        List<Integer> invalidRows = new ArrayList<>();

        for (String field : header) {
            nonEmptyCounts.put(field, 0);
        }

        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            totalRows++;
            List<String> values = parseCsvLine(line);
            if (values.size() < header.size()) {
                while (values.size() < header.size()) {
                    values.add("");
                }
            } else if (values.size() > header.size()) {
                values = values.subList(0, header.size());
            }

            Map<String, String> row = new LinkedHashMap<>();
            for (int j = 0; j < header.size(); j++) {
                String value = values.get(j);
                row.put(header.get(j), value);
                if (!value.trim().isBlank()) {
                    nonEmptyCounts.put(header.get(j), nonEmptyCounts.get(header.get(j)) + 1);
                }
            }

            List<String> keyParts = new ArrayList<>();
            boolean missingKey = false;
            for (String keyColumn : keyColumns) {
                String raw = row.getOrDefault(keyColumn, "").trim();
                if (raw.isBlank()) {
                    missingKey = true;
                    missingKeyCounts.put(keyColumn, missingKeyCounts.getOrDefault(keyColumn, 0) + 1);
                } else {
                    keyParts.add(normalizeKeyValue(raw, keyNormalize));
                }
            }
            if (missingKey) {
                invalid++;
                invalidRows.add(i + 1);
                continue;
            }
            String compositeKey = String.join("||", keyParts);

            if (rows.containsKey(compositeKey)) {
                duplicates++;
                duplicateKeys.add(compositeKey);
                continue;
            }

            rows.put(compositeKey, row);
        }

        return new Roster(header, rows, duplicates, invalid, duplicateKeys, invalidRows, missingKeyCounts, totalRows, nonEmptyCounts);
    }

    static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString());
        return fields;
    }

    private static Report diff(Roster previous, Roster current, List<String> keyColumns, Set<String> ignoredFields,
                               String keyNormalize, String valueNormalize, boolean summaryOnly, int detailLimit) {
        Set<String> prevKeys = previous.rows.keySet();
        Set<String> curKeys = current.rows.keySet();

        Set<String> added = new HashSet<>(curKeys);
        added.removeAll(prevKeys);

        Set<String> removed = new HashSet<>(prevKeys);
        removed.removeAll(curKeys);

        Set<String> shared = new HashSet<>(prevKeys);
        shared.retainAll(curKeys);

        Set<String> addedColumns = new HashSet<>(current.header);
        addedColumns.removeAll(previous.header);

        Set<String> removedColumns = new HashSet<>(previous.header);
        removedColumns.removeAll(current.header);

        Set<String> combinedHeaders = new HashSet<>(previous.header);
        combinedHeaders.addAll(current.header);
        List<String> combinedHeaderList = new ArrayList<>(previous.header);
        for (String field : current.header) {
            if (!combinedHeaderList.contains(field)) {
                combinedHeaderList.add(field);
            }
        }
        Set<String> unknownIgnored = new HashSet<>();
        for (String ignored : ignoredFields) {
            if (!combinedHeaders.contains(ignored)) {
                unknownIgnored.add(ignored);
            }
        }

        List<String> comparableFields = new ArrayList<>();
        Set<String> currentHeaderSet = new HashSet<>(current.header);
        for (String field : previous.header) {
            if (currentHeaderSet.contains(field) && !ignoredFields.contains(field)) {
                comparableFields.add(field);
            }
        }

        List<Update> updates = new ArrayList<>();
        int unchanged = 0;
        Set<String> unchangedKeys = new HashSet<>();
        Map<String, Integer> fieldChangeCounts = new LinkedHashMap<>();

        for (String sharedKey : shared) {
            Map<String, String> prevRow = previous.rows.get(sharedKey);
            Map<String, String> curRow = current.rows.get(sharedKey);
            Map<String, Change> changes = new LinkedHashMap<>();
            for (String field : comparableFields) {
                String before = prevRow.getOrDefault(field, "");
                String after = curRow.getOrDefault(field, "");
                String beforeNormalized = normalizeFieldValue(before, valueNormalize);
                String afterNormalized = normalizeFieldValue(after, valueNormalize);
                if (!beforeNormalized.equals(afterNormalized)) {
                    changes.put(field, new Change(before, after));
                    fieldChangeCounts.put(field, fieldChangeCounts.getOrDefault(field, 0) + 1);
                }
            }
            if (changes.isEmpty()) {
                unchanged++;
                unchangedKeys.add(sharedKey);
            } else {
                updates.add(new Update(sharedKey, changes));
            }
        }

        updates.sort(Comparator.comparing(update -> update.key));

        return new Report(previous, current, keyColumns, keyNormalize, valueNormalize, added, removed, updates, unchanged,
                fieldChangeCounts, ignoredFields, unknownIgnored, addedColumns, removedColumns, unchangedKeys,
                combinedHeaderList, summaryOnly, detailLimit);
    }

    record Roster(List<String> header, Map<String, Map<String, String>> rows, int duplicates, int invalid,
                  List<String> duplicateKeys, List<Integer> invalidRows, Map<String, Integer> missingKeyCounts,
                  int totalRows, Map<String, Integer> nonEmptyCounts) {}

    record Change(String before, String after) {}

    record Update(String key, Map<String, Change> changes) {}

    static class DbRunOptions {
        private final String exportDir;
        private final boolean exportUnchanged;
        private final boolean exportUpdatedRows;
        private final boolean exportStatus;
        private final String jsonPath;

        private DbRunOptions(String exportDir, boolean exportUnchanged, boolean exportUpdatedRows, boolean exportStatus, String jsonPath) {
            this.exportDir = exportDir;
            this.exportUnchanged = exportUnchanged;
            this.exportUpdatedRows = exportUpdatedRows;
            this.exportStatus = exportStatus;
            this.jsonPath = jsonPath;
        }
    }

    static class DbLogger {
        private static final String ENV_DB_URL = "ROSTER_RECONCILER_DB_URL";
        private static final String ENV_DB_USER = "ROSTER_RECONCILER_DB_USER";
        private static final String ENV_DB_PASSWORD = "ROSTER_RECONCILER_DB_PASSWORD";

        static void logRun(Report report, String previousPath, String currentPath, String appName, String schema,
                           List<String> keyColumns, String keyNormalize, String valueNormalize, boolean summaryOnly,
                           int detailLimit, DbRunOptions options, Instant startedAt, Instant finishedAt) throws IOException {
            DbConfig config = DbConfig.fromEnv();
            String table = schema + ".roster_reconciler_runs";

            try (Connection connection = DriverManager.getConnection(config.url, config.user, config.password)) {
                ensureSchema(connection, schema, table);
                insertRun(connection, table, report, previousPath, currentPath, appName, keyColumns, keyNormalize,
                        valueNormalize, summaryOnly, detailLimit, options, startedAt, finishedAt);
            } catch (SQLException e) {
                throw new IOException("Failed to log run to database: " + e.getMessage(), e);
            }
        }

        private static void ensureSchema(Connection connection, String schema, String table) throws SQLException {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS %s (
                            run_id uuid PRIMARY KEY,
                            app_name text NOT NULL,
                            started_at timestamptz NOT NULL,
                            finished_at timestamptz NOT NULL,
                            previous_path text NOT NULL,
                            current_path text NOT NULL,
                            key_columns text NOT NULL,
                            key_normalize text NOT NULL,
                            value_normalize text NOT NULL,
                            summary_only boolean NOT NULL,
                            detail_limit integer,
                            total_previous integer NOT NULL,
                            total_current integer NOT NULL,
                            added integer NOT NULL,
                            removed integer NOT NULL,
                            updated integer NOT NULL,
                            unchanged integer NOT NULL,
                            duplicate_keys_previous integer NOT NULL,
                            duplicate_keys_current integer NOT NULL,
                            invalid_rows_previous integer NOT NULL,
                            invalid_rows_current integer NOT NULL,
                            net_change integer NOT NULL,
                            shared_count integer NOT NULL,
                            added_columns text,
                            removed_columns text,
                            ignored_fields text,
                            unknown_ignored_fields text,
                            export_dir text,
                            export_unchanged boolean NOT NULL,
                            export_updated_rows boolean NOT NULL,
                            export_status boolean NOT NULL,
                            json_path text,
                            created_at timestamptz NOT NULL DEFAULT now()
                        )
                        """.formatted(table));
                statement.execute("CREATE INDEX IF NOT EXISTS roster_reconciler_runs_started_at_idx ON " + table + " (started_at)");
            }
        }

        private static void insertRun(Connection connection, String table, Report report, String previousPath, String currentPath,
                                      String appName, List<String> keyColumns, String keyNormalize, String valueNormalize,
                                      boolean summaryOnly, int detailLimit, DbRunOptions options, Instant startedAt, Instant finishedAt)
                throws SQLException {
            String sql = """
                    INSERT INTO %s (
                        run_id, app_name, started_at, finished_at, previous_path, current_path, key_columns,
                        key_normalize, value_normalize, summary_only, detail_limit, total_previous, total_current,
                        added, removed, updated, unchanged, duplicate_keys_previous, duplicate_keys_current,
                        invalid_rows_previous, invalid_rows_current, net_change, shared_count, added_columns,
                        removed_columns, ignored_fields, unknown_ignored_fields, export_dir, export_unchanged,
                        export_updated_rows, export_status, json_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """.formatted(table);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                int idx = 1;
                ps.setObject(idx++, UUID.randomUUID());
                ps.setString(idx++, appName);
                ps.setObject(idx++, startedAt);
                ps.setObject(idx++, finishedAt);
                ps.setString(idx++, previousPath);
                ps.setString(idx++, currentPath);
                ps.setString(idx++, String.join(",", keyColumns));
                ps.setString(idx++, keyNormalize);
                ps.setString(idx++, valueNormalize);
                ps.setBoolean(idx++, summaryOnly);
                if (detailLimit <= 0) {
                    ps.setNull(idx++, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(idx++, detailLimit);
                }
                ps.setInt(idx++, report.previous.rows.size());
                ps.setInt(idx++, report.current.rows.size());
                ps.setInt(idx++, report.added.size());
                ps.setInt(idx++, report.removed.size());
                ps.setInt(idx++, report.updates.size());
                ps.setInt(idx++, report.unchanged);
                ps.setInt(idx++, report.previous.duplicates);
                ps.setInt(idx++, report.current.duplicates);
                ps.setInt(idx++, report.previous.invalid);
                ps.setInt(idx++, report.current.invalid);
                ps.setInt(idx++, report.current.rows.size() - report.previous.rows.size());
                ps.setInt(idx++, report.sharedCount);
                setNullableText(ps, idx++, joinList(report.addedColumns));
                setNullableText(ps, idx++, joinList(report.removedColumns));
                setNullableText(ps, idx++, joinList(report.ignoredFields));
                setNullableText(ps, idx++, joinList(report.unknownIgnoredFields));
                setNullableText(ps, idx++, blankToNull(options.exportDir));
                ps.setBoolean(idx++, options.exportUnchanged);
                ps.setBoolean(idx++, options.exportUpdatedRows);
                ps.setBoolean(idx++, options.exportStatus);
                setNullableText(ps, idx, blankToNull(options.jsonPath));
                ps.executeUpdate();
            }
        }

        private static void setNullableText(PreparedStatement ps, int index, String value) throws SQLException {
            if (value == null || value.isBlank()) {
                ps.setNull(index, java.sql.Types.VARCHAR);
            } else {
                ps.setString(index, value);
            }
        }

        private static String blankToNull(String value) {
            if (value == null || value.isBlank()) {
                return null;
            }
            return value;
        }

        private static String joinList(Set<String> values) {
            if (values == null || values.isEmpty()) {
                return null;
            }
            List<String> sorted = new ArrayList<>(values);
            sorted.sort(String::compareTo);
            return String.join(",", sorted);
        }
    }

    static class DbConfig {
        private final String url;
        private final String user;
        private final String password;

        private DbConfig(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        private static DbConfig fromEnv() throws IOException {
            String url = System.getenv(DbLogger.ENV_DB_URL);
            String user = System.getenv(DbLogger.ENV_DB_USER);
            String password = System.getenv(DbLogger.ENV_DB_PASSWORD);
            if (url == null || url.isBlank()) {
                throw new IOException("Missing database URL env var: " + DbLogger.ENV_DB_URL);
            }
            if (user == null || user.isBlank()) {
                throw new IOException("Missing database user env var: " + DbLogger.ENV_DB_USER);
            }
            if (password == null || password.isBlank()) {
                throw new IOException("Missing database password env var: " + DbLogger.ENV_DB_PASSWORD);
            }
            return new DbConfig(url, user, password);
        }
    }

    static class Report {
        private final Roster previous;
        private final Roster current;
        private final List<String> keyColumns;
        private final String keyNormalize;
        private final String valueNormalize;
        private final Set<String> added;
        private final Set<String> removed;
        private final List<Update> updates;
        private final int unchanged;
        private final Map<String, Integer> fieldChangeCounts;
        private final Set<String> ignoredFields;
        private final Set<String> unknownIgnoredFields;
        private final Set<String> addedColumns;
        private final Set<String> removedColumns;
        private final Set<String> unchangedKeys;
        private final List<String> combinedHeaderList;
        private final boolean summaryOnly;
        private final int detailLimit;
        private final LocalDateTime timestamp;
        private final int sharedCount;

        private Report(Roster previous, Roster current, List<String> keyColumns, String keyNormalize, String valueNormalize,
                       Set<String> added, Set<String> removed, List<Update> updates, int unchanged,
                       Map<String, Integer> fieldChangeCounts, Set<String> ignoredFields,
                       Set<String> unknownIgnoredFields, Set<String> addedColumns, Set<String> removedColumns,
                       Set<String> unchangedKeys, List<String> combinedHeaderList, boolean summaryOnly, int detailLimit) {
            this.previous = previous;
            this.current = current;
            this.keyColumns = keyColumns;
            this.keyNormalize = keyNormalize;
            this.valueNormalize = valueNormalize;
            this.added = added;
            this.removed = removed;
            this.updates = updates;
            this.unchanged = unchanged;
            this.fieldChangeCounts = fieldChangeCounts;
            this.ignoredFields = ignoredFields;
            this.unknownIgnoredFields = unknownIgnoredFields;
            this.addedColumns = addedColumns;
            this.removedColumns = removedColumns;
            this.unchangedKeys = unchangedKeys;
            this.combinedHeaderList = combinedHeaderList;
            this.summaryOnly = summaryOnly;
            this.detailLimit = detailLimit;
            this.timestamp = LocalDateTime.now();
            this.sharedCount = updates.size() + unchanged;
        }

        private String toText(String previousPath, String currentPath) {
            StringBuilder sb = new StringBuilder();
            sb.append("Roster Reconciler Report\n");
            sb.append("Previous: ").append(previousPath).append("\n");
            sb.append("Current: ").append(currentPath).append("\n");
            sb.append("Key Columns: ").append(String.join(", ", keyColumns)).append("\n");
            sb.append("Key Normalize: ").append(keyNormalize).append("\n");
            sb.append("Value Normalize: ").append(valueNormalize).append("\n");
            sb.append("Summary Only: ").append(summaryOnly).append("\n");
            sb.append("Detail Limit: ").append(detailLimit <= 0 ? "none" : detailLimit).append("\n");
            sb.append("Timestamp: ").append(timestamp).append("\n\n");

            sb.append("Summary:\n");
            sb.append("- total_previous: ").append(previous.rows.size()).append("\n");
            sb.append("- total_current: ").append(current.rows.size()).append("\n");
            sb.append("- added: ").append(added.size()).append("\n");
            sb.append("- removed: ").append(removed.size()).append("\n");
            sb.append("- updated: ").append(updates.size()).append("\n");
            sb.append("- unchanged: ").append(unchanged).append("\n");
            sb.append("- duplicate_keys_previous: ").append(previous.duplicates).append("\n");
            sb.append("- duplicate_keys_current: ").append(current.duplicates).append("\n");
            sb.append("- invalid_rows_previous: ").append(previous.invalid).append("\n");
            sb.append("- invalid_rows_current: ").append(current.invalid).append("\n");
            sb.append("- net_change: ").append(current.rows.size() - previous.rows.size()).append("\n");
            sb.append("- net_change_pct_previous: ")
                    .append(formatPercent(current.rows.size() - previous.rows.size(), previous.rows.size()))
                    .append("\n");
            sb.append("- added_pct_current: ").append(formatPercent(added.size(), current.rows.size())).append("\n");
            sb.append("- removed_pct_previous: ").append(formatPercent(removed.size(), previous.rows.size())).append("\n");
            sb.append("- updated_pct_shared: ").append(formatPercent(updates.size(), sharedCount)).append("\n");
            sb.append("- unchanged_pct_shared: ").append(formatPercent(unchanged, sharedCount)).append("\n\n");

            if (summaryOnly) {
                return sb.toString();
            }

            if (!ignoredFields.isEmpty()) {
                sb.append("Ignored Fields:\n");
                ignoredFields.stream().sorted().forEach(field -> sb.append("  - ").append(field).append("\n"));
                sb.append("\n");
            }

            if (!unknownIgnoredFields.isEmpty()) {
                sb.append("Unknown Ignored Fields:\n");
                unknownIgnoredFields.stream().sorted().forEach(field -> sb.append("  - ").append(field).append("\n"));
                sb.append("\n");
            }

            if (!addedColumns.isEmpty() || !removedColumns.isEmpty()) {
                sb.append("Column Changes:\n");
                if (!addedColumns.isEmpty()) {
                    sb.append("  added: ").append(String.join(", ", addedColumns.stream().sorted().toList())).append("\n");
                }
                if (!removedColumns.isEmpty()) {
                    sb.append("  removed: ").append(String.join(", ", removedColumns.stream().sorted().toList())).append("\n");
                }
                sb.append("\n");
            }

            if (!fieldChangeCounts.isEmpty()) {
                sb.append("Field Change Counts:\n");
                fieldChangeCounts.entrySet().stream()
                        .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                        .forEach(entry -> sb.append("  - ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));
                sb.append("\n");
            }

            if (!previous.duplicateKeys.isEmpty() || !current.duplicateKeys.isEmpty()) {
                sb.append("Duplicate Key Values:\n");
                if (!previous.duplicateKeys.isEmpty()) {
                    sb.append("  previous: ").append(String.join(", ", previous.duplicateKeys)).append("\n");
                }
                if (!current.duplicateKeys.isEmpty()) {
                    sb.append("  current: ").append(String.join(", ", current.duplicateKeys)).append("\n");
                }
                sb.append("\n");
            }

            if (!previous.invalidRows.isEmpty() || !current.invalidRows.isEmpty()) {
                sb.append("Invalid Rows (1-based row numbers):\n");
                if (!previous.invalidRows.isEmpty()) {
                    sb.append("  previous: ").append(joinIntList(previous.invalidRows)).append("\n");
                }
                if (!current.invalidRows.isEmpty()) {
                    sb.append("  current: ").append(joinIntList(current.invalidRows)).append("\n");
                }
                sb.append("\n");
            }

            if (!previous.missingKeyCounts.isEmpty() || !current.missingKeyCounts.isEmpty()) {
                sb.append("Missing Key Field Counts:\n");
                if (!previous.missingKeyCounts.isEmpty()) {
                    sb.append("  previous:\n");
                    appendCountLines(sb, previous.missingKeyCounts, "    ");
                }
                if (!current.missingKeyCounts.isEmpty()) {
                    sb.append("  current:\n");
                    appendCountLines(sb, current.missingKeyCounts, "    ");
                }
                sb.append("\n");
            }

            if (!previous.header.isEmpty() || !current.header.isEmpty()) {
                sb.append("Field Completeness (non-empty/total):\n");
                if (!previous.header.isEmpty()) {
                    sb.append("  previous:\n");
                    appendCompletenessLines(sb, previous, "    ");
                }
                if (!current.header.isEmpty()) {
                    sb.append("  current:\n");
                    appendCompletenessLines(sb, current, "    ");
                }
                sb.append("\n");
            }

            if (!added.isEmpty()) {
                List<String> addedList = sortedList(added);
                int shown = Math.min(addedList.size(), detailLimitValue());
                sb.append("Added (" + addedList.size() + "):\n");
                for (int i = 0; i < shown; i++) {
                    sb.append("  + ").append(addedList.get(i)).append("\n");
                }
                if (shown < addedList.size()) {
                    sb.append("  ... (showing ").append(shown).append(" of ").append(addedList.size()).append(")\n");
                }
                sb.append("\n");
            }

            if (!removed.isEmpty()) {
                List<String> removedList = sortedList(removed);
                int shown = Math.min(removedList.size(), detailLimitValue());
                sb.append("Removed (" + removedList.size() + "):\n");
                for (int i = 0; i < shown; i++) {
                    sb.append("  - ").append(removedList.get(i)).append("\n");
                }
                if (shown < removedList.size()) {
                    sb.append("  ... (showing ").append(shown).append(" of ").append(removedList.size()).append(")\n");
                }
                sb.append("\n");
            }

            if (!updates.isEmpty()) {
                int shown = Math.min(updates.size(), detailLimitValue());
                sb.append("Updated (" + updates.size() + "):\n");
                for (int i = 0; i < shown; i++) {
                    Update update = updates.get(i);
                    sb.append("  * ").append(update.key).append("\n");
                    for (Map.Entry<String, Change> entry : update.changes.entrySet()) {
                        sb.append("      ").append(entry.getKey())
                                .append(": \"")
                                .append(entry.getValue().before)
                                .append("\" -> \"")
                                .append(entry.getValue().after)
                                .append("\"\n");
                    }
                }
                if (shown < updates.size()) {
                    sb.append("  ... (showing ").append(shown).append(" of ").append(updates.size()).append(")\n");
                }
                sb.append("\n");
            }

            return sb.toString();
        }

        private String toJson(String previousPath, String currentPath) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"previous\": \"").append(escape(previousPath)).append("\",\n");
            sb.append("  \"current\": \"").append(escape(currentPath)).append("\",\n");
            sb.append("  \"key\": \"").append(escape(String.join(", ", keyColumns))).append("\",\n");
            sb.append("  \"key_columns\": [\n");
            sb.append(joinJsonArray(keyColumns, "    "));
            sb.append("  ],\n");
            sb.append("  \"key_normalize\": \"").append(escape(keyNormalize)).append("\",\n");
            sb.append("  \"value_normalize\": \"").append(escape(valueNormalize)).append("\",\n");
            sb.append("  \"ignored_fields\": [\n");
            sb.append(joinJsonArray(ignoredFields));
            sb.append("  ],\n");
            sb.append("  \"unknown_ignored_fields\": [\n");
            sb.append(joinJsonArray(unknownIgnoredFields));
            sb.append("  ],\n");
            sb.append("  \"timestamp\": \"").append(timestamp).append("\",\n");
            sb.append("  \"summary_only\": ").append(summaryOnly).append(",\n");
            sb.append("  \"detail\": {\n");
            sb.append("    \"limit\": ").append(detailLimit <= 0 ? "null" : detailLimit).append(",\n");
            sb.append("    \"truncated\": {\n");
            sb.append("      \"added\": ").append(isTruncated(added.size())).append(",\n");
            sb.append("      \"removed\": ").append(isTruncated(removed.size())).append(",\n");
            sb.append("      \"updated\": ").append(isTruncated(updates.size())).append("\n");
            sb.append("    }\n");
            sb.append("  },\n");
            sb.append("  \"summary\": {\n");
            sb.append("    \"total_previous\": ").append(previous.rows.size()).append(",\n");
            sb.append("    \"total_current\": ").append(current.rows.size()).append(",\n");
            sb.append("    \"added\": ").append(added.size()).append(",\n");
            sb.append("    \"removed\": ").append(removed.size()).append(",\n");
            sb.append("    \"updated\": ").append(updates.size()).append(",\n");
            sb.append("    \"unchanged\": ").append(unchanged).append(",\n");
            sb.append("    \"duplicate_keys_previous\": ").append(previous.duplicates).append(",\n");
            sb.append("    \"duplicate_keys_current\": ").append(current.duplicates).append(",\n");
            sb.append("    \"invalid_rows_previous\": ").append(previous.invalid).append(",\n");
            sb.append("    \"invalid_rows_current\": ").append(current.invalid).append(",\n");
            sb.append("    \"net_change\": ").append(current.rows.size() - previous.rows.size()).append(",\n");
            sb.append("    \"net_change_pct_previous\": ")
                    .append(formatRatio(current.rows.size() - previous.rows.size(), previous.rows.size()))
                    .append("\n");
            sb.append("  },\n");
            sb.append("  \"change_rates\": {\n");
            sb.append("    \"added_of_current\": ").append(formatRatio(added.size(), current.rows.size())).append(",\n");
            sb.append("    \"removed_of_previous\": ").append(formatRatio(removed.size(), previous.rows.size())).append(",\n");
            sb.append("    \"updated_of_shared\": ").append(formatRatio(updates.size(), sharedCount)).append(",\n");
            sb.append("    \"unchanged_of_shared\": ").append(formatRatio(unchanged, sharedCount)).append("\n");
            sb.append("  },\n");
            sb.append("  \"column_changes\": {\n");
            sb.append("    \"added\": [\n");
            sb.append(joinJsonArray(addedColumns));
            sb.append("    ],\n");
            sb.append("    \"removed\": [\n");
            sb.append(joinJsonArray(removedColumns));
            sb.append("    ]\n");
            sb.append("  },\n");
            sb.append("  \"field_change_counts\": {\n");
            sb.append(joinJsonMap(fieldChangeCounts));
            sb.append("  },\n");
            sb.append("  \"duplicate_key_values\": {\n");
            sb.append("    \"previous\": [\n");
            sb.append(joinJsonArray(previous.duplicateKeys));
            sb.append("    ],\n");
            sb.append("    \"current\": [\n");
            sb.append(joinJsonArray(current.duplicateKeys));
            sb.append("    ]\n");
            sb.append("  },\n");
            sb.append("  \"missing_key_counts\": {\n");
            sb.append("    \"previous\": {\n");
            sb.append(joinJsonMap(previous.missingKeyCounts, "      "));
            sb.append("    },\n");
            sb.append("    \"current\": {\n");
            sb.append(joinJsonMap(current.missingKeyCounts, "      "));
            sb.append("    }\n");
            sb.append("  },\n");
            sb.append("  \"invalid_rows\": {\n");
            sb.append("    \"previous\": [\n");
            sb.append(joinJsonIntArray(previous.invalidRows));
            sb.append("    ],\n");
            sb.append("    \"current\": [\n");
            sb.append(joinJsonIntArray(current.invalidRows));
            sb.append("    ]\n");
            sb.append("  },\n");
            sb.append("  \"field_completeness\": {\n");
            sb.append("    \"previous\": {\n");
            sb.append(joinCompletenessJson(previous, "      "));
            sb.append("    },\n");
            sb.append("    \"current\": {\n");
            sb.append(joinCompletenessJson(current, "      "));
            sb.append("    }\n");
            sb.append("  },\n");
            sb.append("  \"added\": [\n");
            sb.append(joinJsonArray(sortedList(added), "    ", detailLimitValue()));
            sb.append("  ],\n");
            sb.append("  \"removed\": [\n");
            sb.append(joinJsonArray(sortedList(removed), "    ", detailLimitValue()));
            sb.append("  ],\n");
            sb.append("  \"updated\": [\n");
            int updatedShown = Math.min(updates.size(), detailLimitValue());
            for (int i = 0; i < updatedShown; i++) {
                Update update = updates.get(i);
                sb.append("    {\n");
                sb.append("      \"key\": \"").append(escape(update.key)).append("\",\n");
                sb.append("      \"changes\": {");
                int j = 0;
                for (Map.Entry<String, Change> entry : update.changes.entrySet()) {
                    sb.append("\n        \"").append(escape(entry.getKey())).append("\": {")
                            .append("\"before\": \"").append(escape(entry.getValue().before)).append("\", ")
                            .append("\"after\": \"").append(escape(entry.getValue().after)).append("\"}");
                    if (j < update.changes.size() - 1) {
                        sb.append(",");
                    }
                    j++;
                }
                if (!update.changes.isEmpty()) {
                    sb.append("\n      }");
                } else {
                    sb.append("}");
                }
                sb.append("\n    }");
                if (i < updatedShown - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("  ]\n");
            sb.append("}\n");
            return sb.toString();
        }

        private void writeExports(Path exportDir, boolean includeUnchanged, boolean includeUpdatedRows,
                                  boolean includeStatus) throws IOException {
            Files.createDirectories(exportDir);
            writeRosterExport(exportDir.resolve("added.csv"), current.header, added, current.rows);
            writeRosterExport(exportDir.resolve("removed.csv"), previous.header, removed, previous.rows);
            writeUpdatedExport(exportDir.resolve("updated.csv"));
            if (includeUnchanged) {
                writeRosterExport(exportDir.resolve("unchanged.csv"), current.header, unchangedKeys, current.rows);
            }
            if (includeUpdatedRows) {
                writeUpdatedRowsExport(exportDir.resolve("updated_rows.csv"));
            }
            if (includeStatus) {
                writeStatusExport(exportDir.resolve("status.csv"));
            }
        }

        private void writeRosterExport(Path output, List<String> header, Set<String> keys, Map<String, Map<String, String>> rows)
                throws IOException {
            List<String> lines = new ArrayList<>();
            lines.add(joinCsvLine(header));
            List<String> sortedKeys = new ArrayList<>(keys);
            sortedKeys.sort(String::compareTo);
            for (String keyValue : sortedKeys) {
                Map<String, String> row = rows.get(keyValue);
                if (row == null) {
                    continue;
                }
                List<String> values = new ArrayList<>();
                for (String field : header) {
                    values.add(row.getOrDefault(field, ""));
                }
                lines.add(joinCsvLine(values));
            }
            Files.write(output, lines, StandardCharsets.UTF_8);
        }

        private void writeUpdatedExport(Path output) throws IOException {
            List<String> lines = new ArrayList<>();
            List<String> header = List.of("key", "field", "before", "after");
            lines.add(joinCsvLine(header));
            for (Update update : updates) {
                for (Map.Entry<String, Change> entry : update.changes.entrySet()) {
                    List<String> values = List.of(update.key, entry.getKey(), entry.getValue().before, entry.getValue().after);
                    lines.add(joinCsvLine(values));
                }
            }
            Files.write(output, lines, StandardCharsets.UTF_8);
        }

        private void writeUpdatedRowsExport(Path output) throws IOException {
            List<String> lines = new ArrayList<>();
            List<String> header = new ArrayList<>();
            header.add("key");
            for (String field : combinedHeaderList) {
                header.add(field + "_before");
                header.add(field + "_after");
            }
            lines.add(joinCsvLine(header));
            for (Update update : updates) {
                Map<String, String> prevRow = previous.rows.get(update.key);
                Map<String, String> curRow = current.rows.get(update.key);
                if (prevRow == null || curRow == null) {
                    continue;
                }
                List<String> values = new ArrayList<>();
                values.add(update.key);
                for (String field : combinedHeaderList) {
                    values.add(prevRow.getOrDefault(field, ""));
                    values.add(curRow.getOrDefault(field, ""));
                }
                lines.add(joinCsvLine(values));
            }
            Files.write(output, lines, StandardCharsets.UTF_8);
        }

        private void writeStatusExport(Path output) throws IOException {
            List<String> lines = new ArrayList<>();
            List<String> header = List.of("key", "status", "changed_fields");
            lines.add(joinCsvLine(header));

            Map<String, String> status = new LinkedHashMap<>();
            Map<String, String> changedFields = new LinkedHashMap<>();
            for (String key : added) {
                status.put(key, "added");
                changedFields.put(key, "");
            }
            for (String key : removed) {
                status.put(key, "removed");
                changedFields.put(key, "");
            }
            for (Update update : updates) {
                status.put(update.key, "updated");
                changedFields.put(update.key, String.join(";", update.changes.keySet()));
            }
            for (String key : unchangedKeys) {
                status.put(key, "unchanged");
                changedFields.put(key, "");
            }

            List<String> keys = new ArrayList<>(status.keySet());
            keys.sort(String::compareTo);
            for (String key : keys) {
                List<String> values = List.of(key, status.get(key), changedFields.getOrDefault(key, ""));
                lines.add(joinCsvLine(values));
            }

            Files.write(output, lines, StandardCharsets.UTF_8);
        }

        private String joinCsvLine(List<String> values) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append(escapeCsv(values.get(i)));
                if (i < values.size() - 1) {
                    sb.append(",");
                }
            }
            return sb.toString();
        }

        private String escapeCsv(String value) {
            if (value == null) {
                return "";
            }
            boolean needsQuotes = value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r");
            String escaped = value.replace("\"", "\"\"");
            if (needsQuotes) {
                return "\"" + escaped + "\"";
            }
            return escaped;
        }

        private String joinJsonArray(Set<String> values) {
            StringBuilder sb = new StringBuilder();
            List<String> sorted = new ArrayList<>(values);
            sorted.sort(String::compareTo);
            for (int i = 0; i < sorted.size(); i++) {
                sb.append("    \"").append(escape(sorted.get(i))).append("\"");
                if (i < sorted.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        private String joinJsonArray(List<String> values) {
            return joinJsonArray(values, "      ");
        }

        private String joinJsonArray(List<String> values, String indent) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append(indent).append("\"").append(escape(values.get(i))).append("\"");
                if (i < values.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        private String joinJsonArray(List<String> values, String indent, int limit) {
            StringBuilder sb = new StringBuilder();
            int shown = Math.min(values.size(), limit);
            for (int i = 0; i < shown; i++) {
                sb.append(indent).append("\"").append(escape(values.get(i))).append("\"");
                if (i < shown - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        private List<String> sortedList(Set<String> values) {
            List<String> sorted = new ArrayList<>(values);
            sorted.sort(String::compareTo);
            return sorted;
        }

        private boolean isTruncated(int total) {
            return detailLimit > 0 && total > detailLimit;
        }

        private int detailLimitValue() {
            return detailLimit > 0 ? detailLimit : Integer.MAX_VALUE;
        }

        private String joinJsonIntArray(List<Integer> values) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append("      ").append(values.get(i));
                if (i < values.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        private String joinJsonMap(Map<String, Integer> values) {
            return joinJsonMap(values, "    ");
        }

        private String joinJsonMap(Map<String, Integer> values, String indent) {
            StringBuilder sb = new StringBuilder();
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(values.entrySet());
            entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<String, Integer> entry = entries.get(i);
                sb.append(indent).append("\"").append(escape(entry.getKey())).append("\": ").append(entry.getValue());
                if (i < entries.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }

        private void appendCountLines(StringBuilder sb, Map<String, Integer> values, String indent) {
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(values.entrySet());
            entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            for (Map.Entry<String, Integer> entry : entries) {
                sb.append(indent).append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        private void appendCompletenessLines(StringBuilder sb, Roster roster, String indent) {
            List<String> fields = sortedByCompleteness(roster);
            for (String field : fields) {
                int nonEmpty = roster.nonEmptyCounts.getOrDefault(field, 0);
                sb.append(indent)
                        .append("- ")
                        .append(field)
                        .append(": ")
                        .append(nonEmpty)
                        .append("/")
                        .append(roster.totalRows)
                        .append(" (")
                        .append(formatPercent(nonEmpty, roster.totalRows))
                        .append(")\n");
            }
        }

        private List<String> sortedByCompleteness(Roster roster) {
            List<String> fields = new ArrayList<>(roster.header);
            fields.sort((a, b) -> {
                double ratioA = completenessRatio(roster, a);
                double ratioB = completenessRatio(roster, b);
                int cmp = Double.compare(ratioA, ratioB);
                if (cmp != 0) {
                    return cmp;
                }
                return a.compareTo(b);
            });
            return fields;
        }

        private double completenessRatio(Roster roster, String field) {
            if (roster.totalRows == 0) {
                return Double.POSITIVE_INFINITY;
            }
            return (double) roster.nonEmptyCounts.getOrDefault(field, 0) / (double) roster.totalRows;
        }

        private String joinIntList(List<Integer> values) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append(values.get(i));
                if (i < values.size() - 1) {
                    sb.append(", ");
                }
            }
            return sb.toString();
        }

        private String formatPercent(double numerator, double denominator) {
            if (denominator == 0) {
                return "n/a";
            }
            double pct = (numerator / denominator) * 100.0;
            return String.format(Locale.US, "%.2f%%", pct);
        }

        private String formatRatio(double numerator, double denominator) {
            if (denominator == 0) {
                return "null";
            }
            double ratio = numerator / denominator;
            return String.format(Locale.US, "%.4f", ratio);
        }

        private String escape(String input) {
            return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
        }

        private String joinCompletenessJson(Roster roster, String indent) {
            StringBuilder sb = new StringBuilder();
            List<String> fields = sortedByCompleteness(roster);
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                int nonEmpty = roster.nonEmptyCounts.getOrDefault(field, 0);
                sb.append(indent)
                        .append("\"")
                        .append(escape(field))
                        .append("\": {\n");
                sb.append(indent)
                        .append("  \"non_empty\": ")
                        .append(nonEmpty)
                        .append(",\n");
                sb.append(indent)
                        .append("  \"total\": ")
                        .append(roster.totalRows)
                        .append(",\n");
                sb.append(indent)
                        .append("  \"pct\": ")
                        .append(formatRatio(nonEmpty, roster.totalRows))
                        .append("\n");
                sb.append(indent).append("}");
                if (i < fields.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }
}
