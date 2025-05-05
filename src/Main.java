import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    private static final String DEFAULT_KEY = "email";

    public static void main(String[] args) {
        Map<String, String> options = parseArgs(args);
        if (!options.containsKey("previous") || !options.containsKey("current")) {
            printUsage();
            System.exit(2);
        }

        String previousPath = options.get("previous");
        String currentPath = options.get("current");
        String key = options.getOrDefault("key", DEFAULT_KEY);
        String keyNormalize = options.getOrDefault("key-normalize", "none");
        String valueNormalize = options.getOrDefault("value-normalize", "none");
        String jsonPath = options.get("json");
        String exportDir = options.get("export-dir");
        boolean exportUnchanged = options.containsKey("export-unchanged");
        Set<String> ignoredFields = parseIgnoredFields(options.get("ignore"));

        try {
            validateKeyNormalize(keyNormalize);
            validateValueNormalize(valueNormalize);
            Roster previous = readRoster(Path.of(previousPath), key, keyNormalize);
            Roster current = readRoster(Path.of(currentPath), key, keyNormalize);
            Report report = diff(previous, current, key, ignoredFields, keyNormalize, valueNormalize);

            String output = report.toText(previousPath, currentPath);
            System.out.println(output);

            if (jsonPath != null && !jsonPath.isBlank()) {
                Files.writeString(Path.of(jsonPath), report.toJson(previousPath, currentPath), StandardCharsets.UTF_8);
            }

            if (exportDir != null && !exportDir.isBlank()) {
                report.writeExports(Path.of(exportDir), exportUnchanged);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp out Main --previous <file.csv> --current <file.csv> [--key email] [--key-normalize none|lower|upper] [--value-normalize none|trim|collapse] [--ignore field1,field2] [--json report.json] [--export-dir outdir] [--export-unchanged]");
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

    private static String normalizeKeyValue(String value, String keyNormalize) {
        if (value == null) {
            return "";
        }
        return switch (keyNormalize) {
            case "lower" -> value.toLowerCase();
            case "upper" -> value.toUpperCase();
            default -> value;
        };
    }

    private static String normalizeFieldValue(String value, String valueNormalize) {
        if (value == null) {
            return "";
        }
        return switch (valueNormalize) {
            case "trim" -> value.trim();
            case "collapse" -> value.trim().replaceAll("\\s+", " ");
            default -> value;
        };
    }

    private static Roster readRoster(Path path, String key, String keyNormalize) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("CSV is empty: " + path);
        }

        List<String> header = parseCsvLine(lines.get(0));
        if (!header.contains(key)) {
            throw new IOException("Key column '" + key + "' not found in: " + path);
        }
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        int duplicates = 0;
        int invalid = 0;
        List<String> duplicateKeys = new ArrayList<>();
        List<Integer> invalidRows = new ArrayList<>();

        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
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
                row.put(header.get(j), values.get(j));
            }

            String keyValue = row.getOrDefault(key, "").trim();
            if (keyValue.isBlank()) {
                invalid++;
                invalidRows.add(i + 1);
                continue;
            }

            String normalizedKey = normalizeKeyValue(keyValue, keyNormalize);
            if (rows.containsKey(normalizedKey)) {
                duplicates++;
                duplicateKeys.add(normalizedKey);
                continue;
            }

            rows.put(normalizedKey, row);
        }

        return new Roster(header, rows, duplicates, invalid, duplicateKeys, invalidRows);
    }

    private static List<String> parseCsvLine(String line) {
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

    private static Report diff(Roster previous, Roster current, String key, Set<String> ignoredFields,
                               String keyNormalize, String valueNormalize) {
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

        return new Report(previous, current, key, keyNormalize, valueNormalize, added, removed, updates, unchanged,
                fieldChangeCounts, ignoredFields, unknownIgnored, addedColumns, removedColumns, unchangedKeys);
    }

    private record Roster(List<String> header, Map<String, Map<String, String>> rows, int duplicates, int invalid,
                          List<String> duplicateKeys, List<Integer> invalidRows) {}

    private record Change(String before, String after) {}

    private record Update(String key, Map<String, Change> changes) {}

    private static class Report {
        private final Roster previous;
        private final Roster current;
        private final String key;
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
        private final LocalDateTime timestamp;

        private Report(Roster previous, Roster current, String key, String keyNormalize, String valueNormalize,
                       Set<String> added, Set<String> removed, List<Update> updates, int unchanged,
                       Map<String, Integer> fieldChangeCounts, Set<String> ignoredFields,
                       Set<String> unknownIgnoredFields, Set<String> addedColumns, Set<String> removedColumns,
                       Set<String> unchangedKeys) {
            this.previous = previous;
            this.current = current;
            this.key = key;
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
            this.timestamp = LocalDateTime.now();
        }

        private String toText(String previousPath, String currentPath) {
            StringBuilder sb = new StringBuilder();
            sb.append("Roster Reconciler Report\n");
            sb.append("Previous: ").append(previousPath).append("\n");
            sb.append("Current: ").append(currentPath).append("\n");
            sb.append("Key: ").append(key).append("\n");
            sb.append("Key Normalize: ").append(keyNormalize).append("\n");
            sb.append("Value Normalize: ").append(valueNormalize).append("\n");
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
            sb.append("- invalid_rows_current: ").append(current.invalid).append("\n\n");

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

            if (!added.isEmpty()) {
                sb.append("Added (" + added.size() + "):\n");
                added.stream().sorted().forEach(k -> sb.append("  + ").append(k).append("\n"));
                sb.append("\n");
            }

            if (!removed.isEmpty()) {
                sb.append("Removed (" + removed.size() + "):\n");
                removed.stream().sorted().forEach(k -> sb.append("  - ").append(k).append("\n"));
                sb.append("\n");
            }

            if (!updates.isEmpty()) {
                sb.append("Updated (" + updates.size() + "):\n");
                for (Update update : updates) {
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
                sb.append("\n");
            }

            return sb.toString();
        }

        private String toJson(String previousPath, String currentPath) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"previous\": \"").append(escape(previousPath)).append("\",\n");
            sb.append("  \"current\": \"").append(escape(currentPath)).append("\",\n");
            sb.append("  \"key\": \"").append(escape(key)).append("\",\n");
            sb.append("  \"key_normalize\": \"").append(escape(keyNormalize)).append("\",\n");
            sb.append("  \"value_normalize\": \"").append(escape(valueNormalize)).append("\",\n");
            sb.append("  \"ignored_fields\": [\n");
            sb.append(joinJsonArray(ignoredFields));
            sb.append("  ],\n");
            sb.append("  \"unknown_ignored_fields\": [\n");
            sb.append(joinJsonArray(unknownIgnoredFields));
            sb.append("  ],\n");
            sb.append("  \"timestamp\": \"").append(timestamp).append("\",\n");
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
            sb.append("    \"invalid_rows_current\": ").append(current.invalid).append("\n");
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
            sb.append("  \"invalid_rows\": {\n");
            sb.append("    \"previous\": [\n");
            sb.append(joinJsonIntArray(previous.invalidRows));
            sb.append("    ],\n");
            sb.append("    \"current\": [\n");
            sb.append(joinJsonIntArray(current.invalidRows));
            sb.append("    ]\n");
            sb.append("  },\n");
            sb.append("  \"added\": [\n");
            sb.append(joinJsonArray(added));
            sb.append("  ],\n");
            sb.append("  \"removed\": [\n");
            sb.append(joinJsonArray(removed));
            sb.append("  ],\n");
            sb.append("  \"updated\": [\n");
            for (int i = 0; i < updates.size(); i++) {
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
                if (i < updates.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("  ]\n");
            sb.append("}\n");
            return sb.toString();
        }

        private void writeExports(Path exportDir, boolean includeUnchanged) throws IOException {
            Files.createDirectories(exportDir);
            writeRosterExport(exportDir.resolve("added.csv"), current.header, added, current.rows);
            writeRosterExport(exportDir.resolve("removed.csv"), previous.header, removed, previous.rows);
            writeUpdatedExport(exportDir.resolve("updated.csv"));
            if (includeUnchanged) {
                writeRosterExport(exportDir.resolve("unchanged.csv"), current.header, unchangedKeys, current.rows);
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
            List<String> header = List.of(key, "field", "before", "after");
            lines.add(joinCsvLine(header));
            for (Update update : updates) {
                for (Map.Entry<String, Change> entry : update.changes.entrySet()) {
                    List<String> values = List.of(update.key, entry.getKey(), entry.getValue().before, entry.getValue().after);
                    lines.add(joinCsvLine(values));
                }
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
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append("      \"").append(escape(values.get(i))).append("\"");
                if (i < values.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
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
            StringBuilder sb = new StringBuilder();
            List<Map.Entry<String, Integer>> entries = new ArrayList<>(values.entrySet());
            entries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<String, Integer> entry = entries.get(i);
                sb.append("    \"").append(escape(entry.getKey())).append("\": ").append(entry.getValue());
                if (i < entries.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            return sb.toString();
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

        private String escape(String input) {
            return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
        }
    }
}
