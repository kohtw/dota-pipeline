import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** No external test framework: run ./gradlew extractorTests. */
public final class ReplayExtractorTest {
    public static void main(String[] args) throws Exception {
        var spec = ReplayExtractor.Spec.parse("""
                {"sample_ticks":30,"tables":[
                  {"name":"old","kind":"snapshot","props":[]},
                  {"name":"kda","kind":"snapshot","sample_ticks":1,
                   "changes_only":true,"index_count":64,
                   "props":[["m_vecPlayerTeamData.{i}.m_iKills","int"]]}
                ]}
                """);
        var old = spec.tables.get(0);
        var kda = spec.tables.get(1);
        assert old.sampleTicks == 30 && !old.changesOnly;
        assert kda.sampleTicks == 1 && kda.changesOnly;
        assert kda.cols.get(5).name().equals("m_vecPlayerTeamData.m_iKills");
        assert old.shouldEmit(1, 0, List.of(0));
        assert old.shouldEmit(1, 0, List.of(0));
        assert kda.shouldEmit(1, 0, List.of(0)); // zero baseline must survive
        assert !kda.shouldEmit(1, 0, List.of(0));
        assert kda.shouldEmit(1, 0, List.of(1));
        assert kda.shouldEmit(1, 1, List.of(1)); // independent player index
        assert kda.shouldEmit(2, 0, List.of(1)); // independent entity handle
        var values = new ArrayList<Object>(Arrays.asList(1, null));
        assert kda.shouldEmit(1, 2, values);
        values.set(0, 2); // retained baseline must be a copy
        assert kda.shouldEmit(1, 2, values);
        assert !kda.shouldEmit(1, 2, Arrays.asList(2, null));
        kda.forget(1, 0);
        assert kda.shouldEmit(1, 0, List.of(1)); // reappearing entry emits again
        try {
            ReplayExtractor.Spec.parse("""
                    {"tables":[{"name":"bad","kind":"snapshot","sample_ticks":0}]}
                    """);
            throw new AssertionError("invalid sampling rate accepted");
        } catch (IllegalArgumentException expected) {
            assert expected.getMessage().contains("sample_ticks");
        }
        System.out.println("ReplayExtractor sampling/change checks passed");
    }
}
