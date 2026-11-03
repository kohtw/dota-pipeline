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
import skadistats.clarity.source.MappedFileSource;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/** Usage: ReplayInfo <replay.dem> <out.json> -- writes basic match info as JSON, plus <out>.combatlog.txt and <out>.positions.txt. */
public class ReplayInfo {

    public static void main(String[] args) throws Exception {
        var info = Clarity.infoForFile(args[0]);
        var dota = info.getGameInfo().getDota();

        var players = new StringBuilder();
        for (var p : dota.getPlayerInfoList()) {
            if (players.length() > 0) players.append(",\n");
            players.append("    {\"hero\": \"").append(p.getHeroName())
                   .append("\", \"team\": ").append(p.getGameTeam()).append("}");
        }

        String json = "{\n"
                + "  \"match_id\": " + dota.getMatchId() + ",\n"
                + "  \"game_mode\": " + dota.getGameMode() + ",\n"
                + "  \"winner\": " + dota.getGameWinner() + ",\n"
                + "  \"end_time\": " + dota.getEndTime() + ",\n"
                + "  \"players\": [\n" + players + "\n  ]\n"
                + "}\n";
        Files.writeString(Path.of(args[1]), json);

        // full combat log next to the json: <out>.json -> <out>.combatlog.txt
        String logName = args[1].replaceFirst("\\.json$", "") + ".combatlog.txt";
        String posName = args[1].replaceFirst("\\.json$", "") + ".positions.txt";
        String namesName = args[1].replaceFirst("\\.json$", "") + ".combatlognames.txt";
        var names = new CombatLogNames();
        try (var out = Files.newBufferedWriter(Path.of(logName));
             var posOut = Files.newBufferedWriter(Path.of(posName))) {
            new SimpleRunner(new MappedFileSource(args[0]))
                    .runWith(new CombatLogDump(out), new PositionDump(posOut), names);
        }
        names.write(Path.of(namesName));
    }

    /** CombatLogNames string table: index -> name, as referenced by the combat log's *NameIdx fields. */
    public static class CombatLogNames {
        private final Map<Integer, String> byIndex = new TreeMap<>();

        @OnStringTableEntry("CombatLogNames")
        public void onEntry(StringTable table, int index, String name, ByteString value) {
            byIndex.put(index, name);
        }

        void write(Path file) throws IOException {
            var sb = new StringBuilder();
            byIndex.forEach((i, n) -> sb.append(i).append('\t').append(n).append('\n'));
            Files.writeString(file, sb);
        }
    }

    /** One row per hero per second (30 ticks): tick, hero, world x, world y. */
    @UsesEntities
    public static class PositionDump {
        private static final int SAMPLE_TICKS = 30;
        private final Writer out;

        @Insert private Context ctx;
        @Insert private Entities entities;

        PositionDump(Writer out) {
            this.out = out;
        }

        @OnTickStart
        public void onTickStart(boolean synthetic) throws IOException {
            int tick = ctx.getTick();
            if (synthetic || tick % SAMPLE_TICKS != 0) return;
            for (Entity e : (Iterable<Entity>) entities.stream()
                    .filter(h -> h.getDtClass().getDtName().startsWith("CDOTA_Unit_Hero_"))::iterator) {
                int cellX = e.getProperty("CBodyComponent.m_cellX");
                int cellY = e.getProperty("CBodyComponent.m_cellY");
                float vecX = e.getProperty("CBodyComponent.m_vecX");
                float vecY = e.getProperty("CBodyComponent.m_vecY");
                out.write(String.format("[tick %d] %s x=%.1f y=%.1f%n", tick, e.getDtClass().getDtName(),
                        cellX * 128f + vecX - 16384f, cellY * 128f + vecY - 16384f));
            }
        }
    }

    public static class CombatLogDump {
        private final Writer out;

        CombatLogDump(Writer out) {
            this.out = out;
        }

        @OnCombatLogEntry
        public void onEntry(CombatLogEntry e) throws IOException {
            out.write(String.format("[%.2f] %s attacker=%s target=%s inflictor=%s value=%d%n",
                    e.getTimestamp(), e.getType(), e.getAttackerName(), e.getTargetName(),
                    e.getInflictorName(), e.getValue()));
        }
    }
}
