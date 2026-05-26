"""
Tests for scripts.dialfire_common — pure-function unit tests (no mocking needed).
"""
import datetime
from datetime import date, timedelta, timezone
from unittest.mock import patch

import pytest

from scripts.dialfire_common import (
    BENCHMARKS,
    RM_CAMPAIGNS,
    _norm_camp,
    dates_to_timespan,
    finalize,
    merge_agent_row,
    parse_row,
)

# get_week_bounds lives in fetch_dialfire.py but that file uses bare
# `from dialfire_common import ...` (no package prefix) so it can't be
# imported as scripts.fetch_dialfire from the test harness.  We reproduce
# the (tiny) pure function here so we can still test it thoroughly.

def get_week_bounds(now_sast):
    """Return (monday, sunday) for the week we should fetch.

    On Mondays we fetch the PREVIOUS completed week; on Tue-Sun the current.
    """
    today = now_sast.date()
    weekday = today.weekday()  # 0=Mon ... 6=Sun
    if weekday == 0:
        monday = today - timedelta(days=7)
    else:
        monday = today - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


# =========================================================================
# dates_to_timespan
# =========================================================================
class TestDatesToTimespan:
    """dates_to_timespan(date_from, date_to) -> 'X-Yday' relative string."""

    def _call(self, date_from, date_to, fake_today):
        """Call dates_to_timespan with a pinned 'today'."""
        fake_now = datetime.datetime(
            fake_today.year, fake_today.month, fake_today.day,
            tzinfo=timezone.utc,
        )
        with patch("scripts.dialfire_common.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fake_now
            mock_dt.datetime.side_effect = lambda *a, **kw: datetime.datetime(*a, **kw)
            return dates_to_timespan(date_from, date_to)

    def test_one_week_ago(self):
        today = date(2026, 5, 26)  # Monday
        mon = date(2026, 5, 18)
        sun = date(2026, 5, 24)
        result = self._call(mon, sun, today)
        # days_from = 8, days_to = 2-1 = 1
        assert result == "8-1day"

    def test_same_day(self):
        today = date(2026, 5, 26)
        result = self._call(today, today, today)
        # days_from = 0, days_to = 0 - 1 = -1 -> clamped to 0
        # days_from < days_to? 0 < 0 is false, so stays 0
        assert result == "0-0day"

    def test_current_week_partial(self):
        today = date(2026, 5, 28)  # Wednesday
        mon = date(2026, 5, 25)
        sun = date(2026, 5, 31)
        result = self._call(mon, sun, today)
        # days_from = 3, days_to = -3 - 1 = -4 -> clamped to 0
        assert result == "3-0day"

    def test_future_end_date_clamps_to_zero(self):
        today = date(2026, 5, 26)
        result = self._call(date(2026, 5, 20), date(2026, 5, 30), today)
        # days_from = 6, days_to = -4-1 = -5 -> clamped to 0
        assert result == "6-0day"

    def test_very_old_dates(self):
        today = date(2026, 5, 26)
        result = self._call(date(2026, 1, 1), date(2026, 1, 7), today)
        # days_from = 145, days_to = 139-1 = 138
        assert result == "145-138day"

    def test_days_from_less_than_days_to_clamped(self):
        """If days_from < days_to (shouldn't happen normally), clamp."""
        today = date(2026, 5, 26)
        # Reverse the dates intentionally: from is more recent than to
        result = self._call(date(2026, 5, 25), date(2026, 5, 20), today)
        # days_from = 1, days_to = 6-1 = 5 -> days_from < days_to, so days_from = 5
        assert result == "5-5day"


# =========================================================================
# get_week_bounds (from fetch_dialfire.py)
# =========================================================================
class TestGetWeekBounds:
    """get_week_bounds(now_sast) -> (monday, sunday) dates."""

    def _make_sast(self, y, m, d):
        # SAST is UTC+2; we just need a timezone-aware datetime with .date()
        sast = datetime.timezone(timedelta(hours=2))
        return datetime.datetime(y, m, d, 10, 0, 0, tzinfo=sast)

    def test_tuesday_returns_current_week(self):
        # Tuesday 2026-05-26 -> current week's Monday = 2026-05-25
        now = self._make_sast(2026, 5, 26)
        mon, sun = get_week_bounds(now)
        assert mon == date(2026, 5, 25)
        assert sun == date(2026, 5, 31)

    def test_monday_returns_previous_week(self):
        # Monday 2026-05-25 -> previous week's Monday = 2026-05-18
        now = self._make_sast(2026, 5, 25)
        mon, sun = get_week_bounds(now)
        assert mon == date(2026, 5, 18)
        assert sun == date(2026, 5, 24)

    def test_sunday_returns_current_week(self):
        # Sunday 2026-05-31 -> current week's Monday = 2026-05-25
        now = self._make_sast(2026, 5, 31)
        mon, sun = get_week_bounds(now)
        assert mon == date(2026, 5, 25)
        assert sun == date(2026, 5, 31)

    def test_wednesday_returns_current_week(self):
        now = self._make_sast(2026, 5, 27)
        mon, sun = get_week_bounds(now)
        assert mon == date(2026, 5, 25)
        assert sun == date(2026, 5, 31)

    def test_saturday_returns_current_week(self):
        now = self._make_sast(2026, 5, 30)
        mon, sun = get_week_bounds(now)
        assert mon == date(2026, 5, 25)
        assert sun == date(2026, 5, 31)

    def test_sunday_span_is_always_6_days(self):
        """Sunday - Monday should always be 6 days."""
        for d in range(1, 29):
            now = self._make_sast(2026, 5, d)
            mon, sun = get_week_bounds(now)
            assert (sun - mon).days == 6


# =========================================================================
# parse_row
# =========================================================================
class TestParseRow:
    """parse_row(row) -> agent dict or None."""

    def test_basic_columns_format(self):
        row = {"value": "Alice", "columns": [100, 20, "20.0", 5.5]}
        result = parse_row(row)
        assert result is not None
        assert result["name"] == "Alice"
        assert result["calls"] == 100
        assert result["success"] == 20
        assert result["workTime"] == 5.5
        assert result["cph"] == round(100 / 5.5, 1)
        assert result["successRate"] == 20.0

    def test_worktime_in_milliseconds(self):
        """workTime > 1000 is treated as milliseconds."""
        row = {"value": "Bob", "columns": [50, 10, "20.0", 7200000]}
        result = parse_row(row)
        assert result is not None
        # 7200000 ms = 2 hours
        assert result["workTime"] == 2.0
        assert result["cph"] == 25.0

    def test_worktime_in_hours(self):
        """workTime <= 1000 is treated as hours."""
        row = {"value": "Carol", "columns": [200, 40, "20.0", 4.0]}
        result = parse_row(row)
        assert result["workTime"] == 4.0
        assert result["cph"] == 50.0

    def test_zero_work_hours(self):
        row = {"value": "Dave", "columns": [10, 2, "20.0", 0]}
        result = parse_row(row)
        assert result["cph"] == 0.0

    def test_zero_calls(self):
        row = {"value": "Eve", "columns": [0, 0, "0", 5.0]}
        result = parse_row(row)
        assert result["calls"] == 0
        assert result["successRate"] == 0.0
        assert result["cph"] == 0.0

    def test_missing_columns(self):
        """Row with no columns key should default to zeros."""
        row = {"value": "Frank"}
        result = parse_row(row)
        assert result is not None
        assert result["calls"] == 0
        assert result["success"] == 0
        assert result["workTime"] == 0.0

    def test_partial_columns(self):
        """Fewer columns than expected should not crash."""
        row = {"value": "Grace", "columns": [50]}
        result = parse_row(row)
        assert result is not None
        assert result["calls"] == 50

    def test_non_numeric_columns(self):
        """Non-numeric column values should fall back to defaults."""
        row = {"value": "Hank", "columns": ["abc", "def", "ghi", "jkl"]}
        result = parse_row(row)
        assert result is not None
        assert result["calls"] == 0
        assert result["success"] == 0
        assert result["workTime"] == 0.0

    def test_none_name_returns_none(self):
        row = {"value": None}
        assert parse_row(row) is None

    def test_dash_name_returns_none(self):
        row = {"value": "-"}
        assert parse_row(row) is None

    def test_unknown_name_returns_none(self):
        row = {"value": "Unknown"}
        assert parse_row(row) is None

    def test_empty_string_name_returns_none(self):
        row = {"value": ""}
        assert parse_row(row) is None

    def test_em_dash_returns_none(self):
        row = {"value": "\u2014"}
        assert parse_row(row) is None

    def test_en_dash_returns_none(self):
        row = {"value": "\u2013"}
        assert parse_row(row) is None

    def test_none_string_returns_none(self):
        row = {"value": "None"}
        assert parse_row(row) is None

    def test_name_from_alternate_keys(self):
        """Name can come from 'name', 'user', 'username', 'agent_name'."""
        for key in ("name", "user", "username", "agent_name"):
            row = {key: "TestAgent", "columns": [10, 2, "20.0", 1.0]}
            result = parse_row(row)
            assert result is not None
            assert result["name"] == "TestAgent"

    def test_seller_rental_email_from_row(self):
        row = {
            "value": "Agent1",
            "columns": [50, 10, "20.0", 2.0],
            "seller": 5,
            "rental": 3,
            "email": 2,
        }
        result = parse_row(row)
        assert result["seller"] == 5
        assert result["rental"] == 3
        assert result["email"] == 2

    def test_seller_from_seller_lead_key(self):
        row = {"value": "Agent2", "columns": [10, 1, "10.0", 1.0], "seller_lead": 7}
        result = parse_row(row)
        assert result["seller"] == 7

    def test_completed_key_overrides_columns(self):
        """'completed' key takes priority over columns[0]."""
        row = {"value": "Agent3", "completed": 99, "columns": [50, 10, "20.0", 1.0]}
        result = parse_row(row)
        assert result["calls"] == 99

    def test_defaults_for_new_agent(self):
        row = {"value": "NewAgent", "columns": [10, 2, "20.0", 1.0]}
        result = parse_row(row)
        assert result["is_rm"] is False
        assert result["meetsTarget"] is False
        assert result["campaigns"] == []

    def test_success_rate_calculation(self):
        row = {"value": "Agent4", "columns": [200, 35, "17.5", 4.0]}
        result = parse_row(row)
        assert result["successRate"] == 17.5

    def test_name_whitespace_stripped(self):
        row = {"value": "  Padded Name  ", "columns": [10, 1, "10.0", 1.0]}
        result = parse_row(row)
        assert result["name"] == "Padded Name"


# =========================================================================
# _norm_camp
# =========================================================================
class TestNormCamp:
    """_norm_camp(name) strips CM/NA suffixes."""

    def test_strip_cm_suffix_dash(self):
        assert _norm_camp("Goal Diggers - CM") == "Goal Diggers"

    def test_strip_na_suffix_dash(self):
        assert _norm_camp("Goal Diggers - NA") == "Goal Diggers"

    def test_strip_cm_suffix_underscore(self):
        assert _norm_camp("Assassins_CM") == "Assassins"

    def test_strip_na_suffix_underscore(self):
        assert _norm_camp("Assassins_NA") == "Assassins"

    def test_strip_cm_suffix_space(self):
        assert _norm_camp("Amigos CM") == "Amigos"

    def test_strip_na_suffix_space(self):
        assert _norm_camp("Amigos NA") == "Amigos"

    def test_case_insensitive(self):
        assert _norm_camp("Team - cm") == "Team"
        assert _norm_camp("Team - Cm") == "Team"
        assert _norm_camp("Team - na") == "Team"

    def test_no_suffix_unchanged(self):
        assert _norm_camp("CLIENTHUB") == "CLIENTHUB"

    def test_empty_string(self):
        assert _norm_camp("") == ""

    def test_cm_in_middle_not_stripped(self):
        """CM/NA only stripped at the end, not in the middle."""
        assert _norm_camp("CM Team") == "CM Team"

    def test_just_cm(self):
        # "CM" alone is treated as a suffix and stripped entirely
        assert _norm_camp("CM") == ""

    def test_multiple_suffixes_only_last_stripped(self):
        assert _norm_camp("Team - CM - NA") == "Team - CM"

    def test_whitespace_around_result(self):
        assert _norm_camp("  Padded  - CM  ") == "Padded"


# =========================================================================
# merge_agent_row
# =========================================================================
class TestMergeAgentRow:
    """merge_agent_row(agents, parsed, cname) accumulates agent stats."""

    def _make_parsed(self, name="Agent1", calls=100, success=20, seller=5,
                     rental=2, email=1, work_time=4.0):
        return {
            "name": name,
            "calls": calls,
            "success": success,
            "seller": seller,
            "rental": rental,
            "email": email,
            "cph": round(calls / work_time, 1) if work_time > 0 else 0.0,
            "successRate": round(success / calls * 100, 1) if calls > 0 else 0.0,
            "workTime": work_time,
            "is_rm": False,
            "meetsTarget": False,
            "campaigns": [],
        }

    def test_first_campaign_creates_entry(self):
        agents = {}
        parsed = self._make_parsed()
        merge_agent_row(agents, parsed, "CampaignA")
        assert "Agent1" in agents
        assert agents["Agent1"]["calls"] == 100
        assert agents["Agent1"]["campaigns"] == ["CampaignA"]

    def test_second_campaign_adds_counts(self):
        agents = {}
        p1 = self._make_parsed(calls=100, success=20, seller=5, rental=2, email=1, work_time=4.0)
        p2 = self._make_parsed(calls=50, success=10, seller=3, rental=1, email=0, work_time=2.0)
        merge_agent_row(agents, p1, "CampA")
        merge_agent_row(agents, p2, "CampB")
        a = agents["Agent1"]
        assert a["calls"] == 150
        assert a["success"] == 30
        assert a["seller"] == 8
        assert a["rental"] == 3
        assert a["email"] == 1
        assert a["workTime"] == 6.0
        assert a["campaigns"] == ["CampA", "CampB"]

    def test_duplicate_campaign_not_appended(self):
        agents = {}
        p1 = self._make_parsed(calls=100, success=20)
        p2 = self._make_parsed(calls=50, success=10)
        merge_agent_row(agents, p1, "CampA")
        merge_agent_row(agents, p2, "CampA")
        a = agents["Agent1"]
        # Counts still accumulate (same agent in same campaign, e.g. different rows)
        assert a["calls"] == 150
        # But campaign name not duplicated
        assert a["campaigns"] == ["CampA"]

    def test_empty_campaign_name_not_added(self):
        agents = {}
        parsed = self._make_parsed()
        merge_agent_row(agents, parsed, "")
        assert agents["Agent1"]["campaigns"] == []

    def test_none_campaign_name_not_added(self):
        """cname=None should not be appended."""
        agents = {}
        parsed = self._make_parsed()
        merge_agent_row(agents, parsed, None)
        assert agents["Agent1"]["campaigns"] == []

    def test_three_campaigns_accumulate(self):
        agents = {}
        for i, camp in enumerate(["A", "B", "C"]):
            p = self._make_parsed(calls=10 * (i + 1), success=i + 1, work_time=1.0)
            merge_agent_row(agents, p, camp)
        a = agents["Agent1"]
        assert a["calls"] == 60  # 10 + 20 + 30
        assert a["success"] == 6  # 1 + 2 + 3
        assert a["workTime"] == 3.0
        assert a["campaigns"] == ["A", "B", "C"]

    def test_multiple_agents_separate(self):
        agents = {}
        p1 = self._make_parsed(name="Alice", calls=100)
        p2 = self._make_parsed(name="Bob", calls=200)
        merge_agent_row(agents, p1, "Camp1")
        merge_agent_row(agents, p2, "Camp1")
        assert agents["Alice"]["calls"] == 100
        assert agents["Bob"]["calls"] == 200

    def test_work_time_rounded_to_4_decimals(self):
        agents = {}
        p1 = self._make_parsed(work_time=1.11111)
        p2 = self._make_parsed(work_time=2.22222)
        merge_agent_row(agents, p1, "A")
        merge_agent_row(agents, p2, "B")
        # round(1.11111 + 2.22222, 4) = round(3.33333, 4) = 3.3333
        assert agents["Agent1"]["workTime"] == 3.3333


# =========================================================================
# finalize
# =========================================================================
class TestFinalize:
    """finalize(agents) computes cph, successRate, is_rm, meetsTarget."""

    def _make_agent(self, name="Agent1", calls=100, success=20, work_time=4.0,
                    campaigns=None):
        return {
            "name": name,
            "calls": calls,
            "success": success,
            "seller": 0,
            "rental": 0,
            "email": 0,
            "cph": 0.0,
            "successRate": 0.0,
            "workTime": work_time,
            "is_rm": False,
            "meetsTarget": False,
            "campaigns": campaigns or [],
        }

    def test_cph_calculation(self):
        agents = {"A": self._make_agent(calls=200, work_time=4.0)}
        finalize(agents)
        assert agents["A"]["cph"] == 50.0

    def test_cph_zero_work_time(self):
        agents = {"A": self._make_agent(calls=100, work_time=0.0)}
        finalize(agents)
        assert agents["A"]["cph"] == 0.0

    def test_success_rate_calculation(self):
        agents = {"A": self._make_agent(calls=200, success=34)}
        finalize(agents)
        assert agents["A"]["successRate"] == 17.0

    def test_success_rate_zero_calls(self):
        agents = {"A": self._make_agent(calls=0, success=0, work_time=0.0)}
        finalize(agents)
        assert agents["A"]["successRate"] == 0.0

    def test_rm_classification_clienthub_only(self):
        agents = {"A": self._make_agent(campaigns=["CLIENTHUB"])}
        finalize(agents)
        assert agents["A"]["is_rm"] is True

    def test_rm_classification_multiple_rm_campaigns(self):
        agents = {"A": self._make_agent(
            campaigns=["Clienthub Master", "New Contacts"]
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is True

    def test_rm_classification_all_rm_campaigns(self):
        agents = {"A": self._make_agent(
            campaigns=list(RM_CAMPAIGNS)
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is True

    def test_fancy_classification_mixed_campaigns(self):
        agents = {"A": self._make_agent(
            campaigns=["CLIENTHUB", "Goal Diggers"]
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is False

    def test_fancy_classification_non_rm_campaign(self):
        agents = {"A": self._make_agent(campaigns=["Assassins"])}
        finalize(agents)
        assert agents["A"]["is_rm"] is False

    def test_empty_campaigns_not_rm(self):
        agents = {"A": self._make_agent(campaigns=[])}
        finalize(agents)
        assert agents["A"]["is_rm"] is False

    def test_meets_target_rm_agent(self):
        """RM threshold: cph >= 45 and successRate >= 17."""
        agents = {"A": self._make_agent(
            calls=200, success=35, work_time=4.0,  # cph=50, sr=17.5
            campaigns=["CLIENTHUB"],
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is True
        assert agents["A"]["meetsTarget"] is True

    def test_does_not_meet_target_rm_low_cph(self):
        agents = {"A": self._make_agent(
            calls=100, success=20, work_time=4.0,  # cph=25, sr=20
            campaigns=["CLIENTHUB"],
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is True
        assert agents["A"]["meetsTarget"] is False  # cph 25 < 45

    def test_does_not_meet_target_rm_low_sr(self):
        agents = {"A": self._make_agent(
            calls=200, success=30, work_time=4.0,  # cph=50, sr=15
            campaigns=["CLIENTHUB"],
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is True
        assert agents["A"]["meetsTarget"] is False  # sr 15 < 17

    def test_meets_target_fancy_agent(self):
        """Fancy threshold: cph >= 45 and successRate >= 20."""
        agents = {"A": self._make_agent(
            calls=200, success=42, work_time=4.0,  # cph=50, sr=21
            campaigns=["Goal Diggers"],
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is False
        assert agents["A"]["meetsTarget"] is True

    def test_does_not_meet_target_fancy_low_sr(self):
        """Fancy needs sr >= 20, so sr=19 should fail."""
        agents = {"A": self._make_agent(
            calls=200, success=38, work_time=4.0,  # cph=50, sr=19
            campaigns=["Goal Diggers"],
        )}
        finalize(agents)
        assert agents["A"]["is_rm"] is False
        assert agents["A"]["meetsTarget"] is False

    def test_meets_target_false_when_zero_calls(self):
        agents = {"A": self._make_agent(
            calls=0, success=0, work_time=0.0,
            campaigns=["CLIENTHUB"],
        )}
        finalize(agents)
        assert agents["A"]["meetsTarget"] is False

    def test_exactly_at_benchmark_thresholds_rm(self):
        """Exactly cph=45 and sr=17 should meet target for RM."""
        # We need calls/workTime = 45 and success/calls*100 = 17
        # calls=1000, workTime=1000/45, success = 170
        work = round(1000 / 45, 4)
        agents = {"A": self._make_agent(
            calls=1000, success=170, work_time=work,
            campaigns=["CLIENTHUB"],
        )}
        finalize(agents)
        assert agents["A"]["cph"] >= 45
        assert agents["A"]["successRate"] == 17.0
        assert agents["A"]["meetsTarget"] is True

    def test_exactly_at_benchmark_thresholds_fancy(self):
        """Exactly cph=45 and sr=20 should meet target for Fancy."""
        work = round(1000 / 45, 4)
        agents = {"A": self._make_agent(
            calls=1000, success=200, work_time=work,
            campaigns=["Goal Diggers"],
        )}
        finalize(agents)
        assert agents["A"]["cph"] >= 45
        assert agents["A"]["successRate"] == 20.0
        assert agents["A"]["meetsTarget"] is True

    def test_finalize_multiple_agents(self):
        agents = {
            "RM1": self._make_agent(
                name="RM1", calls=200, success=40, work_time=4.0,
                campaigns=["CLIENTHUB"],
            ),
            "FC1": self._make_agent(
                name="FC1", calls=300, success=70, work_time=5.0,
                campaigns=["Assassins"],
            ),
        }
        finalize(agents)
        assert agents["RM1"]["is_rm"] is True
        assert agents["FC1"]["is_rm"] is False
        assert agents["RM1"]["cph"] == 50.0
        assert agents["FC1"]["cph"] == 60.0


# =========================================================================
# Integration-style: parse_row -> merge_agent_row -> finalize
# =========================================================================
class TestEndToEnd:
    """Ensure the full pipeline of parse -> merge -> finalize works."""

    def test_two_campaigns_one_agent(self):
        row1 = {"value": "Alice", "columns": [100, 20, "20.0", 4.0], "seller": 3}
        row2 = {"value": "Alice", "columns": [50, 10, "20.0", 2.0], "seller": 2}
        agents = {}
        p1 = parse_row(row1)
        p2 = parse_row(row2)
        merge_agent_row(agents, p1, "CLIENTHUB")
        merge_agent_row(agents, p2, "Goal Diggers")
        finalize(agents)

        a = agents["Alice"]
        assert a["calls"] == 150
        assert a["success"] == 30
        assert a["seller"] == 5
        assert a["workTime"] == 6.0
        assert a["cph"] == 25.0
        assert a["successRate"] == 20.0
        # Mixed campaigns -> not RM
        assert a["is_rm"] is False
        assert a["campaigns"] == ["CLIENTHUB", "Goal Diggers"]

    def test_rm_only_agent(self):
        row = {"value": "Bob", "columns": [300, 60, "20.0", 5.0]}
        agents = {}
        p = parse_row(row)
        merge_agent_row(agents, p, "Clienthub Master")
        merge_agent_row(agents, parse_row(
            {"value": "Bob", "columns": [100, 20, "20.0", 1.5]}
        ), "New Contacts")
        finalize(agents)

        a = agents["Bob"]
        assert a["is_rm"] is True
        assert a["calls"] == 400
        assert a["cph"] == round(400 / 6.5, 1)
