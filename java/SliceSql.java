import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.logging.*;

public class SliceSql {
    private static final Logger LOGGER = Logger.getLogger("SliceSql");

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);
        String dbtype = required(cli, "dbtype");
        String url = required(cli, "url");
        String user = required(cli, "user");
        String password = cli.getOrDefault("password", "");
        String table = required(cli, "table");
        int slices = Integer.parseInt(cli.getOrDefault("slices", "4"));

        setupLogger(cli);
        TableSpec tSpec = parseTableSpec(table);
        LOGGER.info("start dbtype=" + dbtype + " table=" + tSpec.qualified + " slices=" + slices);

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            List<String> pkCols = getPkColumns(conn, dbtype, tSpec);
            if (pkCols.isEmpty()) {
                throw new IllegalStateException("Primary key not found");
            }
            List<Object[]> boundaries = fetchBoundaries(conn, dbtype, tSpec, pkCols, slices);
            LOGGER.info("pk_cols=" + pkCols + " boundaries=" + boundaries.size());
            if (boundaries.isEmpty()) {
                return;
            }
            List<String> sqls = buildSlices(pkCols, boundaries, tSpec.qualified);
            for (String s : sqls) {
                System.out.println(s);
            }
        }
    }

    private static TableSpec parseTableSpec(String raw) {
        int dot = raw.indexOf('.');
        if (dot < 0) return new TableSpec(null, raw, raw);
        String schema = raw.substring(0, dot);
        String name = raw.substring(dot + 1);
        return new TableSpec(schema, name, schema + "." + name);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                int idx = a.indexOf('=');
                m.put(a.substring(2, idx), a.substring(idx + 1));
            }
        }
        return m;
    }

    private static String required(Map<String, String> m, String k) {
        String v = m.get(k);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("Missing --" + k);
        return v;
    }

    private static void setupLogger(Map<String, String> cli) throws Exception {
        String levelStr = cli.getOrDefault("loglevel", "INFO").toUpperCase(Locale.ROOT);
        Level level;
        switch (levelStr) {
            case "DEBUG":
                level = Level.FINE;
                break;
            case "TRACE":
                level = Level.FINEST;
                break;
            default:
                try {
                    level = Level.parse(levelStr);
                } catch (IllegalArgumentException e) {
                    level = Level.INFO;
                }
        }
        String logFile = cli.getOrDefault("logfile", "slice_sql.log");
        LOGGER.setUseParentHandlers(false);
        for (Handler h : LOGGER.getHandlers()) LOGGER.removeHandler(h);
        FileHandler fh = new FileHandler(logFile, true);
        fh.setFormatter(new SimpleFormatter());
        fh.setLevel(level);
        LOGGER.addHandler(fh);
        LOGGER.setLevel(level);
    }

    private static List<String> getPkColumns(Connection conn, String dbtype, TableSpec table) throws Exception {
        LOGGER.info("fetching PK for " + table.qualified + " dbtype=" + dbtype);
        if (dbtype.equals("ora")) {
            String sql = table.schema == null
                    ? "SELECT acc.column_name FROM user_constraints ac JOIN user_cons_columns acc ON ac.constraint_name = acc.constraint_name WHERE ac.table_name = ? AND ac.constraint_type = 'P' ORDER BY acc.position"
                    : "SELECT acc.column_name FROM all_constraints ac JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name WHERE ac.owner = ? AND ac.table_name = ? AND ac.constraint_type = 'P' ORDER BY acc.position";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = 1;
                if (table.schema != null) ps.setString(idx++, table.schema.toUpperCase(Locale.ROOT));
                ps.setString(idx, table.name.toUpperCase(Locale.ROOT));
                try (ResultSet rs = ps.executeQuery()) {
                    List<String> cols = new ArrayList<>();
                    while (rs.next()) cols.add(rs.getString(1));
                    return cols;
                }
            }
        }
        // PG/psycopg2: fetch conkey then map attnum -> attname
        List<Integer> conkey = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT conkey FROM pg_constraint WHERE conrelid = ?::regclass AND contype = 'p'")) {
            ps.setString(1, table.qualified);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || rs.getArray(1) == null) return Collections.emptyList();
                Array arr = rs.getArray(1);
                Object raw = arr.getArray();
                if (raw instanceof Integer[] ints) {
                    for (Integer i : ints) if (i != null) conkey.add(i);
                } else if (raw instanceof Short[] shorts) {
                    for (Short s : shorts) if (s != null) conkey.add((int) s);
                } else if (raw instanceof int[] ints) {
                    for (int i : ints) conkey.add(i);
                } else if (raw instanceof short[] shorts) {
                    for (short s : shorts) conkey.add((int) s);
                } else if (raw instanceof Object[] objs) {
                    for (Object o : objs) if (o instanceof Number n) conkey.add(n.intValue());
                }
            }
        }
        Map<Integer, String> attMap = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT attnum, attname FROM pg_attribute WHERE attrelid = ?::regclass")) {
            ps.setString(1, table.qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) attMap.put(rs.getInt(1), rs.getString(2));
            }
        }
        List<String> cols = new ArrayList<>();
        for (int k : conkey) {
            if (attMap.containsKey(k)) cols.add(attMap.get(k));
        }
        LOGGER.info("pk columns=" + cols);
        return cols;
    }

    private static List<Object[]> fetchBoundaries(Connection conn, String dbtype, TableSpec table, List<String> pkCols, int k) throws Exception {
        String colList = String.join(", ", pkCols);
        String orderBy = colList;
        long total;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table.qualified)) {
            rs.next();
            total = rs.getLong(1);
        }
        if (total == 0) return Collections.emptyList();

        long step = Math.max(1, (long)Math.ceil((double) total / k));
        Set<Long> rnSet = new LinkedHashSet<>();
        for (int i = 0; i <= k; i++) {
            long rn = 1L + i * step;
            if (rn > total) rn = total;
            rnSet.add(rn);
        }
        rnSet.add(total);
        List<Long> rns = new ArrayList<>(rnSet);
        Collections.sort(rns);
        LOGGER.fine("count=" + total + " step=" + step + " rns=" + rns);

        String placeholders = String.join(",", Collections.nCopies(rns.size(), dbtype.equals("ora") ? "?" : "?"));
        String sql = "SELECT " + colList + " FROM ( SELECT " + colList + ", ROW_NUMBER() OVER (ORDER BY " + orderBy + ") rn FROM " + table.qualified + " ) t WHERE rn IN (" + placeholders + ") ORDER BY rn";
        LOGGER.fine("boundary SQL=" + sql);

        List<Object[]> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Long rn : rns) ps.setLong(idx++, rn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Object[] row = new Object[pkCols.size()];
                    for (int i = 0; i < pkCols.size(); i++) row[i] = rs.getObject(i + 1);
                    out.add(row);
                }
            }
        }
        LOGGER.fine("fetched boundary rows=" + out.size());
        return out;
    }

    private static List<String> buildSlices(List<String> pkCols, List<Object[]> boundaries, String table) {
        List<String> sqls = new ArrayList<>();
        if (pkCols.size() == 1) {
            List<String> conds = makeSingleRanges(pkCols.get(0), boundaries);
            for (String c : conds) sqls.add("SELECT * FROM " + table + " WHERE " + c + ";");
            return sqls;
        }
        for (int i = 0; i < boundaries.size() - 1; i++) {
            Object[] lo = boundaries.get(i);
            Object[] hi = boundaries.get(i + 1);
            boolean isLast = i == boundaries.size() - 2;
            for (String where : buildComposite(pkCols, lo, hi, isLast)) {
                sqls.add("SELECT * FROM " + table + " WHERE " + where + ";");
            }
        }
        LOGGER.info("sqls=" + sqls.size());
        return sqls;
    }

    private static List<String> makeSingleRanges(String col, List<Object[]> bounds) {
        List<String> conds = new ArrayList<>();
        List<Object> dedup = new ArrayList<>();
        for (Object[] b : bounds) {
            Object v = b[0];
            if (dedup.isEmpty() || !dedup.get(dedup.size() - 1).equals(v)) dedup.add(v);
        }
        if (dedup.size() == 1) dedup.add(dedup.get(0));
        for (int i = 0; i < dedup.size() - 1; i++) {
            Object lo = dedup.get(i), hi = dedup.get(i + 1);
            if (i == dedup.size() - 2) {
                conds.add(col + " >= " + lit(lo) + " AND " + col + " <= " + lit(hi));
            } else {
                conds.add(col + " >= " + lit(lo) + " AND " + col + " < " + lit(hi));
            }
        }
        return conds;
    }

    private static List<String> buildComposite(List<String> cols, Object[] left, Object[] right, boolean isLast) {
        if (cols.size() == 1) {
            String op = isLast ? "<=" : "<";
            return List.of(cols.get(0) + " >= " + lit(left[0]) + " AND " + cols.get(0) + " " + op + " " + lit(right[0]));
        }
        if (Objects.equals(left[0], right[0])) {
            List<String> tail = buildComposite(cols.subList(1, cols.size()),
                    Arrays.copyOfRange(left, 1, left.length),
                    Arrays.copyOfRange(right, 1, right.length),
                    isLast);
            List<String> res = new ArrayList<>();
            for (String w : tail) {
                res.add(cols.get(0) + " = " + lit(left[0]) + " AND " + w);
            }
            return res;
        }
        List<String> wheres = new ArrayList<>();
        // lower band
        for (List<String> seg : geSegments(cols.subList(1, cols.size()), Arrays.copyOfRange(left, 1, left.length))) {
            wheres.add(cols.get(0) + " = " + lit(left[0]) + " AND " + String.join(" AND ", seg));
        }
        // middle band
        wheres.add(cols.get(0) + " > " + lit(left[0]) + " AND " + cols.get(0) + " < " + lit(right[0]));
        // upper band
        for (List<String> seg : leSegments(cols.subList(1, cols.size()), Arrays.copyOfRange(right, 1, right.length), isLast)) {
            wheres.add(cols.get(0) + " = " + lit(right[0]) + " AND " + String.join(" AND ", seg));
        }
        return wheres;
    }

    private static List<List<String>> geSegments(List<String> cols, Object[] bounds) {
        if (cols.isEmpty()) return List.of(List.of("1=1"));
        if (cols.size() == 1) return List.of(List.of(cols.get(0) + " >= " + lit(bounds[0])));
        String head = cols.get(0);
        Object hv = bounds[0];
        List<List<String>> out = new ArrayList<>();
        // head = hv, tail >=
        for (List<String> sub : geSegments(cols.subList(1, cols.size()), Arrays.copyOfRange(bounds, 1, bounds.length))) {
            List<String> seg = new ArrayList<>();
            seg.add(head + " = " + lit(hv));
            seg.addAll(sub);
            out.add(seg);
        }
        // head > hv
        out.add(List.of(head + " > " + lit(hv)));
        return out;
    }

    private static List<List<String>> leSegments(List<String> cols, Object[] bounds, boolean inclusive) {
        if (cols.isEmpty()) return List.of(List.of("1=1"));
        if (cols.size() == 1) {
            String op = inclusive ? "<=" : "<";
            return List.of(List.of(cols.get(0) + " " + op + " " + lit(bounds[0])));
        }
        String head = cols.get(0);
        Object hv = bounds[0];
        List<List<String>> out = new ArrayList<>();
        // head < hv
        out.add(List.of(head + " < " + lit(hv)));
        // head = hv, tail <=
        for (List<String> sub : leSegments(cols.subList(1, cols.size()), Arrays.copyOfRange(bounds, 1, bounds.length), inclusive)) {
            List<String> seg = new ArrayList<>();
            seg.add(head + " = " + lit(hv));
            seg.addAll(sub);
            out.add(seg);
        }
        return out;
    }

    private static String lit(Object v) {
        if (v == null) return "NULL";
        if (v instanceof Number) return v.toString();
        if (v instanceof java.sql.Timestamp ts) return "TIMESTAMP '" + ts.toLocalDateTime().toString().replace('T',' ') + "'";
        if (v instanceof java.sql.Date d) return "DATE '" + d.toLocalDate().toString() + "'";
        if (v instanceof java.sql.Time t) return "TIME '" + t.toLocalTime().toString() + "'";
        String s = v.toString().replace("'", "''");
        return "'" + s + "'";
    }

    private static final class TableSpec {
        final String schema;
        final String name;
        final String qualified;

        TableSpec(String schema, String name, String qualified) {
            this.schema = schema;
            this.name = name;
            this.qualified = qualified;
        }
    }
}
