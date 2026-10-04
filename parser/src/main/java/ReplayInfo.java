import skadistats.clarity.Clarity;

import java.nio.file.Files;
import java.nio.file.Path;

/** Usage: ReplayInfo <replay.dem> <out.json> -- writes basic match info as JSON. */
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
    }
}
