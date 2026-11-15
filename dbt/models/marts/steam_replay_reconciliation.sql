-- Mart: one player of one match that is both in our replay scoreboard and in Valve's match summary.
-- Two independent sources for hero and K/D/A; the event counts come from kill_participation.
with steam as (
    select
        match_id,
        -- Steam numbers Dire 128-132; clean numbers Dire 5-9 (docs/clean-contract.md)
        case when player_slot >= 128 then player_slot - 128 + 5 else player_slot end as player_slot,
        hero_id, kills, deaths, assists
    from {{ source('stg', 'steam_players') }}
),

joined as (
    select
        s.match_id,
        s.player_slot,
        p.hero_id               as hero_id_replay,
        st.hero_id              as hero_id_steam,
        s.kills                 as kills_replay,
        st.kills                as kills_steam,
        k.kills_from_events     as kills_events,
        s.deaths                as deaths_replay,
        st.deaths               as deaths_steam,
        k.deaths_from_events    as deaths_events,
        s.assists               as assists_replay,
        st.assists              as assists_steam,
        k.assists_from_events   as assists_events,
        k.assist_attribution_complete,
        k.assist_attribution_assumed
    from {{ ref('match_scoreboard') }} s
    join {{ source('clean', 'players') }} p on p.match_id = s.match_id and p.player_slot = s.player_slot
    join steam st on st.match_id = s.match_id and st.player_slot = s.player_slot
    left join {{ ref('kill_participation') }} k on k.player_key = s.player_key
)

select
    match_id, player_slot,
    hero_id_replay, hero_id_steam,
    kills_replay, kills_steam, kills_events,
    deaths_replay, deaths_steam, deaths_events,
    assists_replay, assists_steam, assists_events,
    assist_attribution_complete, assist_attribution_assumed,
    -- a NULL on either side is a disagreement, not a pass
    coalesce(hero_id_replay = hero_id_steam and kills_replay = kills_steam
             and deaths_replay = deaths_steam and assists_replay = assists_steam, false) as scoreboard_matches_steam,
    case when assist_attribution_complete is false then NULL else
        coalesce(kills_replay = kills_events and deaths_replay = deaths_events
                 and assists_replay = assists_events, false) end as events_match_scoreboard
from joined
