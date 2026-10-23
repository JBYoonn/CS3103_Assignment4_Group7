"""Per-client lag detection and rate control (liverecall/flow.py)."""

import math

import pytest

from liverecall.flow import FlowController

OFFERED = 4.0     # source events per second in these scenarios


def fc(**kw):
    return FlowController(**kw)


def send_n(f, n, start, rate):
    t = start
    for _ in range(n):
        f.on_sent(t)
        t += 1.0 / rate
    return t


def lagging(**kw):
    """A controller that has just detected lag at time t after sending 4 msg/s."""
    f = fc(**kw)
    f.on_pong(now=0.01, sent_at=0.0, offered_rate=OFFERED)      # baseline RTT 10 ms
    t = send_n(f, 8, 0.0, 4.0)
    f.on_ping_age(now=t, sent_at=t - 1.0)
    assert f.limited
    return f, t


def recover(f, at):
    """First timely pong after a cut."""
    f.on_pong(now=at + 0.01, sent_at=at, offered_rate=OFFERED)


# --- detection ---------------------------------------------------------------

def test_starts_unlimited_and_never_delays_sends():
    f = fc()
    assert not f.limited
    assert f.ready_in(0.0) == 0.0
    assert not f.gated(0.0, ping_sent_at=None)


def test_baseline_rtt_is_not_counted_as_lag():
    # 250 ms path RTT (delay profile) must not look like a slow client.
    f = fc(lag_high=0.5)
    f.on_pong(now=1.25, sent_at=1.0, offered_rate=OFFERED)
    f.on_ping_age(now=2.0 + 0.3, sent_at=2.0)
    assert not f.limited


def test_first_lag_cuts_to_quarter_of_send_rate():
    # The client already holds a hidden backlog when lag becomes visible, so
    # the post-drain rate must be well below what it failed to keep up with.
    f, _ = lagging(lag_high=0.5, min_rate=0.5)
    assert f.rate == pytest.approx(1.0, rel=0.3)


def test_one_backlog_episode_only_cuts_once():
    f, t = lagging(lag_high=0.5)
    r = f.rate
    f.on_ping_age(now=t + 2.0, sent_at=t - 1.0)            # same old ping, still late
    assert f.rate == r


def test_rate_never_drops_below_minimum():
    f = fc(lag_high=0.5, min_rate=0.5)
    f.on_pong(now=0.01, sent_at=0.0, offered_rate=OFFERED)
    t = 1.0
    for _ in range(20):
        f.on_ping_age(now=t + 1.0, sent_at=t)
        f.on_pong(now=t + 1.1, sent_at=t, offered_rate=OFFERED)
        t += 2.0
    assert f.rate >= 0.5


# --- gating & pacing -----------------------------------------------------------

def test_sending_is_gated_while_backlog_is_visible():
    f, t = lagging(lag_high=0.5)
    assert f.gated(t + 0.1, ping_sent_at=t - 1.0)           # outstanding ping is late
    assert not f.gated(t + 0.1, ping_sent_at=t)             # fresh ping, not late yet
    assert not f.gated(t + 0.1, ping_sent_at=None)


def test_tokens_pace_sends_when_limited():
    f, t = lagging(lag_high=0.5, burst=1.0)
    rate = f.rate
    assert f.ready_in(t) == 0.0
    f.on_sent(t)
    wait = f.ready_in(t)
    assert wait == pytest.approx(1.0 / rate, rel=0.01)
    assert f.ready_in(t + wait) == 0.0


# --- hold / step-up / backoff ---------------------------------------------------

def test_hold_length_follows_how_long_recovery_took():
    f, t = lagging(min_hold_s=1.0, plateau_s=30.0, step=1.25)
    held = f.rate
    recover(f, t + 4.0)                                      # drained after ~4 s
    f.on_pong(now=t + 7.01, sent_at=t + 7.0, offered_rate=OFFERED)
    assert f.rate == held                                   # still holding (4 s hold)
    f.on_pong(now=t + 8.51, sent_at=t + 8.5, offered_rate=OFFERED)
    assert f.rate == pytest.approx(held * 1.25)


def test_hold_is_at_least_min_hold_and_at_most_plateau():
    f, t = lagging(min_hold_s=2.0, plateau_s=10.0)
    recover(f, t + 0.2)
    assert f.hold_until == pytest.approx(t + 0.21 + 2.0, abs=0.02)
    f2, t2 = lagging(min_hold_s=2.0, plateau_s=10.0)
    recover(f2, t2 + 50.0)
    assert f2.hold_until == pytest.approx(t2 + 50.01 + 10.0, abs=0.02)


def test_rate_is_held_flat_until_hold_ends():
    f, t = lagging(min_hold_s=10.0, step=2.0)
    held = f.rate
    now = t + 0.5
    for _ in range(10):
        f.on_pong(now=now + 0.01, sent_at=now, offered_rate=OFFERED)
        now += 0.5
    assert f.rate == held


def test_failed_probe_falls_back_to_last_proven_rate_and_doubles_plateau():
    f, t = lagging(min_hold_s=2.0, plateau_s=4.0, max_plateau_s=60.0, step=1.25)
    proven = f.rate
    recover(f, t + 1.0)                                       # hold until ~t+3
    f.on_pong(now=t + 3.51, sent_at=t + 3.5, offered_rate=OFFERED)   # step up (probe)
    assert f.rate > proven
    f.on_ping_age(now=t + 5.0, sent_at=t + 4.0)                     # probe late
    assert f.rate == pytest.approx(proven)
    assert f.plateau_s == 8.0


def test_lag_at_proven_rate_halves_it():
    f, t = lagging(min_rate=0.1)
    r = f.rate
    recover(f, t + 1.0)
    f.on_ping_age(now=t + 3.0, sent_at=t + 2.0)                     # behind again, no probe
    assert f.rate == pytest.approx(r / 2)


def test_successful_holds_step_up_and_eventually_return_to_normal():
    f, t = lagging(min_hold_s=1.0, plateau_s=1.0, step=1.5, exit_factor=1.25)
    now = t + 0.5
    rates = []
    for _ in range(80):
        f.on_pong(now=now + 0.01, sent_at=now, offered_rate=OFFERED)
        if not f.limited:
            break
        rates.append(f.rate)
        now += 0.5
    assert rates == sorted(rates)
    assert not f.limited
    assert f.rate == math.inf and f.ready_in(now) == 0.0
