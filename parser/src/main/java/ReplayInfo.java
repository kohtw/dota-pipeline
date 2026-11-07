import skadistats.clarity.Clarity;
import skadistats.clarity.event.Insert;
import skadistats.clarity.model.CombatLogEntry;
import skadistats.clarity.model.Entity;
import skadistats.clarity.model.StringTable;
import skadistats.clarity.processor.stringtables.OnStringTableEntry;
import skadistats.clarity.protobuf.ByteString;
import skadistats.clarity.processor.entities.Entities;
import skadistats.clarity.processor.entities.UsesEntities;
import skadistats.clarity.processor.gameevents.OnCombatLogEntry;
import skadistats.clarity.processor.reader.OnTickStart;
import skadistats.clarity.processor.runner.Context;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.source.InputStreamSource;
import skadistats.clarity.source.MappedFileSource;
import skadistats.clarity.source.Source;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Usage:
 *   ReplayInfo <replay.dem> <out.json>
 *     local: writes basic match info as JSON, plus <out>.combatlog.csv, <out>.positions.csv and <out>.names.csv.
 *   ReplayInfo s3://bucket/replays/<id>.dem s3://bucket/parsed/<id>/
 *     S3 (Garage): reads the replay into memory, uploads match.json, combatlog.csv, positions.csv, names.csv
 *     under the prefix. Needs env S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY.
 */
public class ReplayInfo {

    interface SourceFactory { Source open() throws IOException; }

    public static void main(String[] args) throws Exception {
        if (args[0].startsWith("s3://")) {
            byte[] dem = S3Io.get(args[0]);
            Path tmp = Files.createTempDirectory("parsed");  // container-local scratch, uploaded then discarded
            parse(() -> new InputStreamSource(new ByteArrayInputStream(dem)),
                    tmp.resolve("match.json"), tmp.resolve("combatlog.csv"),
                    tmp.resolve("positions.csv"), tmp.resolve("names.csv"));
            S3Io.putDir(tmp, args[1]);
            return;
        }
        // logs next to the json: <out>.json -> <out>.combatlog.csv, <out>.positions.csv, <out>.names.csv
        String base = args[1].replaceFirst("\\.json$", "");
        parse(() -> new MappedFileSource(args[0]), Path.of(args[1]), Path.of(base + ".combatlog.csv"),
                Path.of(base + ".positions.csv"), Path.of(base + ".names.csv"));
    }

    static void parse(SourceFactory src, Path jsonOut, Path combatOut, Path posOut, Path namesOut) throws Exception {
        var infoSource = src.open();
        var info = Clarity.infoForSource(infoSource);
        infoSource.close();
        var dota = info.getGameInfo().getDota();

        long matchId = dota.getMatchId();
        List<String> heroes = dota.getPlayerInfoList().stream().map(p -> p.getHeroName()).toList();

        var names = new CombatLogNames(matchId);
        var gameStart = new GameStart();
        try (var out = Files.newBufferedWriter(combatOut);
             var posWriter = Files.newBufferedWriter(posOut);
             var source = src.open()) {
            new SimpleRunner(source)
                    .runWith(new CombatLogCsv(out, matchId), new PositionDump(posWriter, matchId, heroes),
                            names, gameStart);
        }
        names.write(namesOut);

        var players = new StringBuilder();
        int slot = 0;
        for (var p : dota.getPlayerInfoList()) {
            if (players.length() > 0) players.append(",\n");
            players.append("    {\"slot\": ").append(slot++).append(", \"hero\": \"").append(p.getHeroName())
                   .append("\", \"team\": ").append(p.getGameTeam()).append("}");
        }

        String json = "{\n"
                + "  \"match_id\": " + matchId + ",\n"
                + "  \"game_mode\": " + dota.getGameMode() + ",\n"
                + "  \"winner\": " + dota.getGameWinner() + ",\n"
                + "  \"end_time\": " + dota.getEndTime() + ",\n"
                + "  \"game_start_tick\": " + gameStart.tick + ",\n"
                + "  \"game_start_time\": " + gameStart.time + ",\n"
                + "  \"players\": [\n" + players + "\n  ]\n"
                + "}\n";
        Files.writeString(jsonOut, json);
    }

    /**
     * Horn (0:00 on the game clock): first tick the gamerules state is GAME_IN_PROGRESS (5).
     * time is m_flGameStartTime, on the same clock as the combat log timestamp.
     */
    @UsesEntities
    public static class GameStart {
        private static final int GAME_IN_PROGRESS = 5;
        Integer tick;
        Float time;

        @Insert private Context ctx;
        @Insert private Entities entities;

        @OnTickStart
        public void onTickStart(boolean synthetic) {
            if (synthetic || tick != null) return;
            entities.stream().filter(e -> e.getDtClass().getDtName().equals("CDOTAGamerulesProxy")).findFirst()
                    .ifPresent(g -> {
                        int state = g.getProperty("m_pGameRules.m_nGameState");
                        if (state == GAME_IN_PROGRESS) {
                            tick = ctx.getTick();
                            time = g.getProperty("m_pGameRules.m_flGameStartTime");
                        }
                    });
        }
    }

    /** CombatLogNames string table as CSV: match_id, idx, name (idx is what the combat log's *NameIdx fields hold). */
    public static class CombatLogNames {
        private final Map<Integer, String> byIndex = new TreeMap<>();
        private final long matchId;

        CombatLogNames(long matchId) {
            this.matchId = matchId;
        }

        @OnStringTableEntry("CombatLogNames")
        public void onEntry(StringTable table, int index, String name, ByteString value) {
            byIndex.put(index, name);
        }

        void write(Path file) throws IOException {
            var sb = new StringBuilder("match_id,idx,name\n");
            byIndex.forEach((i, n) -> sb.append(matchId).append(',').append(i).append(',')
                    .append(CombatLogCsv.csv(n)).append('\n'));
            Files.writeString(file, sb);
        }
    }

    /**
     * Hero positions as CSV, one row per real hero (no illusions) per second (30 ticks):
     * match_id, tick, hero, team, x, y. hero is the npc_dota_hero_* name, matching the combat log.
     */
    @UsesEntities
    public static class PositionDump {
        private static final int SAMPLE_TICKS = 30;
        private static final int NULL_HANDLE = 16777215;
        private final Writer out;
        private final long matchId;
        private final List<String> heroes; // by player slot

        @Insert private Context ctx;
        @Insert private Entities entities;

        PositionDump(Writer out, long matchId, List<String> heroes) throws IOException {
            this.out = out;
            this.matchId = matchId;
            this.heroes = heroes;
            out.write("match_id,tick,hero,team,x,y\n");
        }

        @OnTickStart
        public void onTickStart(boolean synthetic) throws IOException {
            int tick = ctx.getTick();
            if (synthetic || tick % SAMPLE_TICKS != 0) return;
            for (Entity e : (Iterable<Entity>) entities.stream()
                    .filter(h -> h.getDtClass().getDtName().startsWith("CDOTA_Unit_Hero_"))::iterator) {
                // illusions replicate another hero's model; real heroes have a null handle here
                int replicating = e.getProperty("m_hReplicatingOtherHeroModel");
                if (replicating != NULL_HANDLE) continue;
                int slot = (int) e.getProperty("m_iPlayerID") / 2; // player id = 2 * slot
                String hero = slot >= 0 && slot < heroes.size() ? heroes.get(slot) : e.getDtClass().getDtName();
                int team = e.getProperty("m_iTeamNum");
                int cellX = e.getProperty("CBodyComponent.m_cellX");
                int cellY = e.getProperty("CBodyComponent.m_cellY");
                float vecX = e.getProperty("CBodyComponent.m_vecX");
                float vecY = e.getProperty("CBodyComponent.m_vecY");
                out.write(String.format("%d,%d,%s,%d,%.1f,%.1f%n", matchId, tick, hero, team,
                        cellX * 128f + vecX - 16384f, cellY * 128f + vecY - 16384f));
            }
        }
    }

    /**
     * Every combat log entry as one CSV row: match_id, tick, core fields, value_name, then every other
     * CombatLogEntry field alphabetically. A cell is blank when that entry's has*() is false.
     */
    public static class CombatLogCsv {
        private static final List<String> CORE =
                List.of("Type", "Timestamp", "AttackerName", "TargetName", "InflictorName", "Value");
        private static final Map<String, Method[]> FIELDS = fields(); // field -> {has, getter}

        private final Writer out;
        private final long matchId;

        @Insert private Context ctx;

        CombatLogCsv(Writer out, long matchId) throws IOException {
            this.out = out;
            this.matchId = matchId;
            var header = new StringJoiner(",");
            header.add("match_id").add("tick");
            for (String f : FIELDS.keySet()) {
                header.add(snake(f));
                if (f.equals("Value")) header.add("value_name");
            }
            out.write(header + "\n");
        }

        @OnCombatLogEntry
        public void onEntry(CombatLogEntry e) throws Exception {
            var row = new StringJoiner(",");
            row.add(String.valueOf(matchId)).add(String.valueOf(ctx.getTick()));
            for (var entry : FIELDS.entrySet()) {
                Method[] m = entry.getValue();
                boolean has = (boolean) m[0].invoke(e);
                row.add(has ? csv(m[1].invoke(e)) : "");
                if (entry.getKey().equals("Value")) row.add(has ? csv(valueName(e)) : "");
            }
            out.write(row + "\n");
        }

        // value is an index into CombatLogNames for some types (e.g. PURCHASE); meaningless otherwise
        private static String valueName(CombatLogEntry e) {
            try {
                return e.getValueName();
            } catch (RuntimeException ex) {
                return null;
            }
        }

        private static Map<String, Method[]> fields() {
            var rest = new TreeMap<String, Method[]>();
            for (Method has : CombatLogEntry.class.getMethods()) {
                if (!has.getName().startsWith("has") || has.getParameterCount() != 0) continue;
                String f = has.getName().substring(3);
                for (String prefix : List.of("get", "is")) {
                    try {
                        rest.put(f, new Method[]{has, CombatLogEntry.class.getMethod(prefix + f)});
                        break;
                    } catch (NoSuchMethodException ignored) {
                    }
                }
            }
            var ordered = new LinkedHashMap<String, Method[]>();
            for (String f : CORE) ordered.put(f, rest.remove(f));
            ordered.putAll(rest);
            return ordered;
        }

        private static String snake(String camel) {
            return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
        }

        static String csv(Object v) {
            if (v == null) return "";
            String s = v instanceof List<?> l
                    ? String.join(";", l.stream().map(String::valueOf).toList())
                    : String.valueOf(v);
            if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
                s = "\"" + s.replace("\"", "\"\"") + "\"";
            }
            return s;
        }
    }
}
