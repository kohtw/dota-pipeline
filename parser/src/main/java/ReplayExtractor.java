import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import skadistats.clarity.Clarity;
import skadistats.clarity.event.Insert;
import skadistats.clarity.model.CombatLogEntry;
import skadistats.clarity.model.Entity;
import skadistats.clarity.processor.entities.Entities;
import skadistats.clarity.processor.entities.OnEntityCreated;
import skadistats.clarity.processor.entities.OnEntityDeleted;
import skadistats.clarity.processor.entities.UsesEntities;
import skadistats.clarity.processor.gameevents.OnCombatLogEntry;
import skadistats.clarity.processor.reader.OnTickStart;
import skadistats.clarity.processor.runner.Context;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.processor.stringtables.StringTables;
import skadistats.clarity.processor.stringtables.UsesStringTable;
import skadistats.clarity.source.InputStreamSource;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generic Dota 2 replay extractor for Spark. Which tables and columns to extract is defined in Python
 * (spark_jobs/replay_spec.py) and passed in as JSON; this class only knows how to read a replay.
 * Values are written as the replay holds them (no computing, resolving, filtering or diffing).
 *
 * Table kinds:
 *   file_info     one row: match_id, game_mode, game_winner, end_time (replay file header)
 *   file_players  one row per player in the file header: slot (list position), hero_name, game_team
 *   combatlog     every combat log entry, every CombatLogEntry field (name fields as their raw *_idx)
 *   string_table  one row per entry of the named string table: idx, name
 *   snapshot      every sample_ticks: one row per entity whose class matches, with the listed props.
 *                 With index_count, props containing "{i}" are read per array index (0..index_count-1);
 *                 indexes whose values are all null (unused array entries) are skipped.
 *                 A table may override sample_ticks and set changes_only to emit the initial
 *                 properties and subsequent changes, without converting counters to deltas.
 *   lifecycle     one row when a matching entity is created and one when it is deleted, with the listed props
 * Every row starts with match_id (and tick, except for file_* and string_table).
 * Class patterns: exact class name, or a prefix ending in "*".
 */
public final class ReplayExtractor {

    private ReplayExtractor() {}

    /** Entry point called from PySpark: one Dataset per spec table, rows from every replay matching path. */
    public static Map<String, Dataset<Row>> parse(SparkSession spark, String path, String specJson) throws Exception {
        var spec = Spec.parse(specJson);
        var sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Tuple2<String, Row>> rows = sc.binaryFiles(path)
                .flatMap(f -> extract(f._2().toArray(), Spec.parse(specJson)).iterator());
        rows.persist(StorageLevel.MEMORY_AND_DISK());  // parse each replay once, split into tables below

        var out = new LinkedHashMap<String, Dataset<Row>>();
        for (var t : spec.tables) {
            String name = t.name;
            var fields = new ArrayList<StructField>();
            for (var c : t.cols) fields.add(DataTypes.createStructField(c.name, sparkType(c.type), true));
            out.put(name, spark.createDataFrame(rows.filter(r -> r._1().equals(name)).map(Tuple2::_2),
                    DataTypes.createStructType(fields)));
        }
        return out;
    }

    /** All rows of one replay, tagged with their table name. */
    static List<Tuple2<String, Row>> extract(byte[] dem, Spec spec) throws Exception {
        var out = new ArrayList<Tuple2<String, Row>>();
        Emit emit = (t, values) -> {
            Object[] typed = new Object[values.size()];
            for (int i = 0; i < typed.length; i++) typed[i] = coerce(t.cols.get(i).type, values.get(i));
            out.add(new Tuple2<>(t.name, RowFactory.create(typed)));
        };

        var infoSource = new InputStreamSource(new ByteArrayInputStream(dem));
        var dota = Clarity.infoForSource(infoSource).getGameInfo().getDota();
        infoSource.close();
        long matchId = dota.getMatchId();

        // Clarity keeps one processor instance per class, so all entity tables share one EntityTables
        var processors = new ArrayList<Object>();
        var strings = new StringTableReader();
        var entityTables = new EntityTables(emit, matchId);
        processors.add(entityTables);
        for (var t : spec.tables) {
            switch (t.kind) {
                case "file_info" -> emit.row(t, List.of(matchId, dota.getGameMode(), dota.getGameWinner(),
                        dota.getEndTime()));
                case "file_players" -> {
                    int slot = 0;
                    for (var p : dota.getPlayerInfoList()) {
                        emit.row(t, List.of(matchId, slot++, p.getHeroName(), p.getGameTeam()));
                    }
                }
                case "combatlog" -> processors.add(new CombatLog(t, emit, matchId));
                case "snapshot" -> entityTables.snapshots.add(t);
                case "lifecycle" -> entityTables.lifecycles.add(t);
                case "string_table" -> { }  // read after the run
                default -> throw new IllegalArgumentException("unknown table kind: " + t.kind);
            }
        }
        processors.add(strings);
        try (var source = new InputStreamSource(new ByteArrayInputStream(dem))) {
            new SimpleRunner(source).runWith(processors.toArray());
        }
        for (var t : spec.tables) {
            if (!t.kind.equals("string_table")) continue;
            var table = strings.tables.forName(t.stringTable);
            if (table == null) continue;
            for (int i = 0; i < table.getEntryCount(); i++) emit.row(t, List.of(matchId, i, table.getNameByIndex(i)));
        }
        return out;
    }

    interface Emit { void row(Table t, List<Object> values) throws Exception; }

    // ---------------------------------------------------------------- processors

    /** Holds the StringTables processor so tables can be read after the run. */
    @UsesStringTable("*")
    public static class StringTableReader {
        @Insert StringTables tables;
    }

    public static class CombatLog {
        private final Table t;
        private final Emit emit;
        private final long matchId;
        @Insert private Context ctx;

        CombatLog(Table t, Emit emit, long matchId) {
            this.t = t;
            this.emit = emit;
            this.matchId = matchId;
        }

        @OnCombatLogEntry
        public void onEntry(CombatLogEntry e) throws Exception {
            var row = new ArrayList<Object>(List.of(matchId, ctx.getTick()));
            for (Method[] m : COMBATLOG_FIELDS.values()) row.add((boolean) m[0].invoke(e) ? m[1].invoke(e) : null);
            emit.row(t, row);
        }
    }

    /** All snapshot and lifecycle tables. */
    @UsesEntities
    public static class EntityTables {
        final List<Table> snapshots = new ArrayList<>();
        final List<Table> lifecycles = new ArrayList<>();
        private final Emit emit;
        private final long matchId;
        @Insert private Context ctx;
        @Insert private Entities entities;

        EntityTables(Emit emit, long matchId) {
            this.emit = emit;
            this.matchId = matchId;
        }

        @OnTickStart
        public void onTickStart(boolean synthetic) throws Exception {
            int tick = ctx.getTick();
            if (synthetic) return;
            for (Table t : snapshots) {
                if (tick % t.sampleTicks == 0) snapshot(t, tick);
            }
        }

        @OnEntityCreated
        public void onCreated(Entity e) throws Exception {
            for (Table t : lifecycles) lifecycle(t, e, "created");
        }

        @OnEntityDeleted
        public void onDeleted(Entity e) throws Exception {
            for (Table t : lifecycles) lifecycle(t, e, "deleted");
        }

        private void lifecycle(Table t, Entity e, String event) throws Exception {
            if (!t.matches(e)) return;
            var row = new ArrayList<Object>(List.of(matchId, ctx.getTick(), event, e.getHandle(),
                    e.getDtClass().getDtName()));
            for (String p : t.props) row.add(prop(e, p));
            emit.row(t, row);
        }

        private void snapshot(Table t, int tick) throws Exception {
            for (Entity e : (Iterable<Entity>) entities.stream().filter(x -> t.matches(x))::iterator) {
                var head = List.<Object>of(matchId, tick, e.getHandle(), e.getDtClass().getDtName());
                if (t.indexCount == 0) {
                    var row = new ArrayList<>(head);
                    var vals = new ArrayList<Object>();
                    for (String p : t.props) vals.add(prop(e, p));
                    if (t.shouldEmit(e.getHandle(), -1, vals)) {
                        row.addAll(vals);
                        emit.row(t, row);
                    }
                    continue;
                }
                for (int i = 0; i < t.indexCount; i++) {
                    String idx = String.format("%04d", i);
                    var vals = new ArrayList<Object>();
                    for (String p : t.props) vals.add(prop(e, p.replace("{i}", idx)));
                    if (vals.stream().allMatch(v -> v == null)) {
                        t.forget(e.getHandle(), i);
                        continue;
                    }
                    if (!t.shouldEmit(e.getHandle(), i, vals)) continue;
                    var row = new ArrayList<>(head);
                    row.add(i);
                    row.addAll(vals);
                    emit.row(t, row);
                }
            }
        }
    }

    static Object prop(Entity e, String name) {
        return e == null || !e.hasProperty(name) ? null : e.getProperty(name);
    }

    // ---------------------------------------------------------------- spec

    record Col(String name, String type) implements java.io.Serializable {}
    record SnapshotKey(int handle, int index) implements java.io.Serializable {}

    static final class Table implements java.io.Serializable {
        String name, kind, stringTable;
        List<String> classes = List.of();
        List<String> props = new ArrayList<>();  // property paths, "{i}" = array index placeholder
        int indexCount;
        int sampleTicks;
        boolean changesOnly;
        final Map<SnapshotKey, List<Object>> previousValues = new LinkedHashMap<>();
        List<Col> cols = new ArrayList<>();      // output columns, in row order

        boolean shouldEmit(int handle, int index, List<Object> values) {
            if (!changesOnly) return true;
            // Copy because later reads must not mutate the retained baseline.
            var old = previousValues.put(new SnapshotKey(handle, index), new ArrayList<>(values));
            return !values.equals(old);
        }

        void forget(int handle, int index) {
            previousValues.remove(new SnapshotKey(handle, index));
        }

        boolean matches(Entity e) {
            String cls = e.getDtClass().getDtName();
            for (String c : classes) {
                if (c.endsWith("*") ? cls.startsWith(c.substring(0, c.length() - 1)) : cls.equals(c)) return true;
            }
            return false;
        }
    }

    static final class Spec implements java.io.Serializable {
        int sampleTicks;
        List<Table> tables = new ArrayList<>();

        static Spec parse(String json) throws Exception {
            JsonNode root = new ObjectMapper().readTree(json);
            var spec = new Spec();
            spec.sampleTicks = root.path("sample_ticks").asInt(30);
            if (spec.sampleTicks < 1) throw new IllegalArgumentException("sample_ticks must be positive");
            for (JsonNode n : root.get("tables")) {
                var t = new Table();
                t.name = n.get("name").asText();
                t.kind = n.get("kind").asText();
                t.stringTable = n.path("string_table").asText(null);
                t.indexCount = n.path("index_count").asInt(0);
                t.sampleTicks = n.path("sample_ticks").asInt(spec.sampleTicks);
                t.changesOnly = n.path("changes_only").asBoolean(false);
                if (t.sampleTicks < 1) throw new IllegalArgumentException("sample_ticks must be positive: " + t.name);
                var classes = new ArrayList<String>();
                n.path("classes").forEach(c -> classes.add(c.asText()));
                t.classes = classes;
                var propTypes = new ArrayList<Col>();
                n.path("props").forEach(p -> propTypes.add(new Col(p.get(0).asText(), p.get(1).asText())));
                propTypes.forEach(p -> t.props.add(p.name()));

                t.cols.add(new Col("match_id", "long"));
                switch (t.kind) {
                    case "file_info" -> t.cols.addAll(List.of(new Col("game_mode", "int"),
                            new Col("game_winner", "int"), new Col("end_time", "long")));
                    case "file_players" -> t.cols.addAll(List.of(new Col("slot", "int"),
                            new Col("hero_name", "string"), new Col("game_team", "int")));
                    case "string_table" -> t.cols.addAll(List.of(new Col("idx", "int"), new Col("name", "string")));
                    case "combatlog" -> {
                        t.cols.add(new Col("tick", "int"));
                        for (Method[] m : COMBATLOG_FIELDS.values()) {
                            t.cols.add(new Col(snake(m[1].getName().replaceFirst("^(get|is)", "")),
                                    javaType(m[1].getReturnType())));
                        }
                    }
                    case "snapshot" -> {
                        t.cols.addAll(List.of(new Col("tick", "int"), new Col("handle", "int"),
                                new Col("class", "string")));
                        if (t.indexCount > 0) t.cols.add(new Col("index", "int"));
                        for (var p : propTypes) t.cols.add(new Col(p.name().replace("{i}.", ""), p.type()));
                    }
                    case "lifecycle" -> {
                        t.cols.addAll(List.of(new Col("tick", "int"), new Col("event", "string"),
                                new Col("handle", "int"), new Col("class", "string")));
                        t.cols.addAll(propTypes);
                    }
                    default -> throw new IllegalArgumentException("unknown table kind: " + t.kind);
                }
                spec.tables.add(t);
            }
            return spec;
        }
    }

    // ---------------------------------------------------------------- types

    private static DataType sparkType(String t) {
        return switch (t) {
            case "long" -> DataTypes.LongType;
            case "int" -> DataTypes.IntegerType;
            case "float" -> DataTypes.FloatType;
            case "bool" -> DataTypes.BooleanType;
            case "string" -> DataTypes.StringType;
            default -> throw new IllegalArgumentException("unknown type: " + t);
        };
    }

    private static String javaType(Class<?> c) {
        if (c == int.class || c == Integer.class) return "int";
        if (c == long.class || c == Long.class) return "long";
        if (c == float.class || c == Float.class || c == double.class) return "float";
        if (c == boolean.class || c == Boolean.class) return "bool";
        return "string";  // enums (type), lists (assist_players)
    }

    /** Raw value in the column's Java type (numbers widened/narrowed, enums by name, lists ';'-joined). */
    static Object coerce(String t, Object v) {
        if (v == null) return null;
        return switch (t) {
            case "long" -> v instanceof Number n ? n.longValue() : Long.valueOf(v.toString());
            case "int" -> v instanceof Number n ? n.intValue() : Integer.valueOf(v.toString());
            case "float" -> v instanceof Number n ? n.floatValue() : Float.valueOf(v.toString());
            case "bool" -> v instanceof Boolean b ? b : Boolean.valueOf(v.toString());
            default -> v instanceof List<?> l
                    ? String.join(";", l.stream().map(String::valueOf).toList())
                    : v instanceof Enum<?> e ? e.name() : v.toString();
        };
    }

    // CombatLogEntry field -> {has*, getter}: core fields first, then alphabetical.
    // Name fields use the raw get*NameIdx getter, not the resolved name.
    static final Map<String, Method[]> COMBATLOG_FIELDS = combatLogFields();

    private static Map<String, Method[]> combatLogFields() {
        var rest = new TreeMap<String, Method[]>();
        for (Method has : CombatLogEntry.class.getMethods()) {
            if (!has.getName().startsWith("has") || has.getParameterCount() != 0) continue;
            String f = has.getName().substring(3);
            for (String getter : List.of("get" + f + "Idx", "get" + f, "is" + f)) {
                try {
                    rest.put(f, new Method[]{has, CombatLogEntry.class.getMethod(getter)});
                    break;
                } catch (NoSuchMethodException ignored) {
                }
            }
        }
        var ordered = new LinkedHashMap<String, Method[]>();
        for (String f : List.of("Type", "Timestamp", "AttackerName", "TargetName", "InflictorName", "Value")) {
            ordered.put(f, rest.remove(f));
        }
        ordered.putAll(rest);
        return ordered;
    }

    private static String snake(String camel) {
        return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }
}
