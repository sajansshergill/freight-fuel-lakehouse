"""Streamlit + Plotly console over the Freight & Fuel Lakehouse Gold layer.

Mirrors the repo's real Gold schemas (src/common/schemas.py,
src/batch/build_gold_tables.py, src/batch/kpi_daily_ops.py):

- gold_fact_fuel_txn      — fuel transactions + rule-based anomaly_flag
                            (gallons > 80 OR gallons x price != total_amount)
- gold_fact_load_status   — latest status per load per day
- gold_fact_driver_movement — GPS-derived ping_count / avg_speed / est_miles
                            (est_miles = avg_speed_mph x hours_observed)
- gold_kpi_daily_ops      — delivered_loads, on_time_rate, fuel_spend,
                            fuel_gallons, fuel_anomaly_txns, est_miles,
                            fuel_spend_per_mile

The committed Delta snapshots ship empty, so the console generates a
deterministic demo dataset (fixed seed) using the exact distributions of
the Kafka producers in src/producers/ — same ID spaces, same fuel/GPS/load
logic — and then applies the Gold transformations above in pandas.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Freight Fuel Lakehouse", page_icon="🚛", layout="wide")

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 15% -10%, #7a4a08 0%, transparent 55%),
                    radial-gradient(1000px 500px at 90% 0%, #0b4f4a 0%, transparent 50%),
                    #0e1117;
    }
    .ff-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(190,120,20,.50), rgba(10,110,100,.45));
        border: 1px solid rgba(255,190,90,.35);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .ff-hero h1 {
        margin: 0; font-size: 2.1rem; letter-spacing: .5px;
        background: linear-gradient(90deg,#ffd28a,#7ef0d4,#ffe9b0);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .ff-hero p { margin: .25rem 0 0 0; color: #d8dee6; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,.85); border: 1px solid rgba(255,190,90,.30);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(126,240,212,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,34,.8); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(14,17,23,.92); }
    </style>
    <div class="ff-hero">
      <h1>🚛 Freight &amp; Fuel Lakehouse — Ops Console</h1>
      <p>Bronze → Silver → Gold, live: Kafka load / GPS / fuel events through Spark
         Structured Streaming into Delta Lake. Drag the 3D fleet space, press play on
         the animated charts, slice everything from the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# Deterministic demo data — producer distributions from src/producers/*,
# Gold logic from src/batch/build_gold_tables.py + kpi_daily_ops.py
# ----------------------------------------------------------------------------
LANES = {
    "LN-001": ("Newark, NJ", "Philadelphia, PA", 40.14, -74.16, 39.95, -75.16),
    "LN-002": ("Newark, NJ", "Baltimore, MD", 40.14, -74.16, 39.29, -76.61),
    "LN-003": ("Edison, NJ", "Boston, MA", 40.52, -74.41, 42.36, -71.06),
    "LN-004": ("Harrisburg, PA", "Pittsburgh, PA", 40.27, -76.88, 40.44, -79.99),
    "LN-005": ("Allentown, PA", "Albany, NY", 40.60, -75.47, 42.65, -73.75),
    "LN-006": ("Scranton, PA", "Cleveland, OH", 41.41, -75.66, 41.50, -81.69),
}
CARRIERS = [f"CR-{i:03d}" for i in range(1, 9)]
DAYS = 45


@st.cache_data
def build_demo() -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(42)
    start = pd.Timestamp("2026-09-28")
    dates = pd.date_range(start, periods=DAYS, freq="D")

    # ---- fuel transactions (produce_fuel_txn_events.py distributions) ----
    fuel_rows = []
    for d in dates:
        n = int(rng.integers(55, 85))
        gallons = np.round(np.maximum(5.0, rng.normal(40, 12, n)), 2)
        price = np.round(rng.uniform(3.0, 4.5, n), 2)
        anomaly = rng.random(n) < 0.02
        gallons = np.where(anomaly, np.round(rng.uniform(80, 140, n), 2), gallons)
        total = np.round(gallons * price, 2)
        # a few amount-mismatch anomalies, as the Gold rule also catches
        mismatch = rng.random(n) < 0.01
        total = np.where(mismatch, total + rng.uniform(3, 25, n).round(2), total)
        for i in range(n):
            fuel_rows.append(
                dict(
                    event_date=d,
                    truck_id=f"TR-{rng.integers(1, 80):04d}",
                    driver_id=f"DR-{rng.integers(1, 120):04d}",
                    station_id=f"ST-{rng.integers(1, 300):05d}",
                    carrier_id=str(rng.choice(CARRIERS)),
                    lane_id=str(rng.choice(list(LANES))),
                    gallons=round(float(gallons[i]), 2),
                    price_per_gallon=float(price[i]),
                    total_amount=float(total[i]),
                )
            )
    fuel = pd.DataFrame(fuel_rows)
    # Gold rule (build_gold_tables.py): anomaly_flag
    fuel["anomaly_flag"] = (
        (fuel["gallons"] > 80)
        | (np.round(fuel["gallons"] * fuel["price_per_gallon"], 2) != fuel["total_amount"])
    ).astype(int)

    # ---- load status (produce_load_events.py statuses / weights) ----
    load_rows = []
    for d in dates:
        n = int(rng.integers(90, 130))
        statuses = rng.choice(
            ["created", "assigned", "picked_up", "delivered", "cancelled"],
            size=n, p=[0.40, 0.25, 0.15, 0.15, 0.05],
        )
        for s in statuses:
            expected = d + pd.Timedelta(hours=int(rng.integers(6, 48)))
            actual = None
            on_time = None
            if s == "delivered":
                actual = expected + pd.Timedelta(hours=int(rng.integers(-2, 9)))
                on_time = int(actual <= expected)
            load_rows.append(
                dict(
                    event_date=d,
                    load_id=f"LD-{rng.integers(1, 200):05d}",
                    carrier_id=str(rng.choice(CARRIERS)),
                    lane_id=str(rng.choice(list(LANES))),
                    latest_status=s,
                    on_time=on_time,
                )
            )
    loads = pd.DataFrame(load_rows)

    # ---- driver movement from GPS pings (fact_driver_movement logic) ----
    move_rows = []
    for d in dates:
        for t in range(1, 41):
            avg_speed = float(np.clip(rng.normal(55, 10), 5, 85))
            hours = float(rng.uniform(2, 11))
            move_rows.append(
                dict(
                    event_date=d,
                    truck_id=f"TR-{t:04d}",
                    driver_id=f"DR-{rng.integers(1, 120):04d}",
                    carrier_id=str(rng.choice(CARRIERS)),
                    lane_id=str(rng.choice(list(LANES))),
                    ping_count=int(hours * 60),
                    avg_speed_mph=round(avg_speed, 1),
                    hours_observed=round(hours, 2),
                    est_miles=round(avg_speed * hours, 1),  # Gold: speed x hours
                )
            )
    move = pd.DataFrame(move_rows)

    # ---- GPS pings for the 3D fleet space (lane corridors) ----
    gps_rows = []
    for lane_id, (origin, dest, olat, olon, dlat, dlon) in LANES.items():
        for d in dates[::3]:
            for k in range(14):
                frac = k / 13
                jitter = rng.normal(0, 0.05, 2)
                gps_rows.append(
                    dict(
                        event_date=d,
                        lane_id=lane_id,
                        lane=f"{origin} → {dest}",
                        lat=olat + (dlat - olat) * frac + jitter[0],
                        lon=olon + (dlon - olon) * frac + jitter[1],
                        speed_mph=float(np.clip(rng.normal(55, 10), 0, 85)),
                        truck_id=f"TR-{rng.integers(1, 80):04d}",
                    )
                )
    gps = pd.DataFrame(gps_rows)

    # ---- gold_kpi_daily_ops (kpi_daily_ops.py joins) ----
    kpi_fuel = fuel.groupby("event_date").agg(
        fuel_spend=("total_amount", "sum"),
        fuel_gallons=("gallons", "sum"),
        fuel_anomaly_txns=("anomaly_flag", "sum"),
    )
    delivered = loads[loads["latest_status"] == "delivered"]
    kpi_loads = delivered.groupby("event_date").agg(
        delivered_loads=("load_id", "nunique"), on_time_rate=("on_time", "mean")
    )
    kpi_miles = move.groupby("event_date").agg(
        est_miles=("est_miles", "sum"), avg_speed_mph=("avg_speed_mph", "mean")
    )
    kpi = kpi_loads.join(kpi_fuel, how="outer").join(kpi_miles, how="outer").reset_index()
    kpi["fuel_spend_per_mile"] = kpi["fuel_spend"] / kpi["est_miles"]
    return {"fuel": fuel, "loads": loads, "move": move, "gps": gps, "kpi": kpi}


DATA = build_demo()
fuel_all, loads_all, move_all, gps_all, kpi_all = (
    DATA["fuel"], DATA["loads"], DATA["move"], DATA["gps"], DATA["kpi"],
)

# ----------------------------------------------------------------------------
# Sidebar — global filters (every view below reads the filtered frames)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
dmin, dmax = kpi_all["event_date"].min(), kpi_all["event_date"].max()
picked = st.sidebar.date_input(
    "Date range", value=(dmin.date(), dmax.date()),
    min_value=dmin.date(), max_value=dmax.date(),
)
if isinstance(picked, (list, tuple)) and len(picked) == 2:
    start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
else:
    start, end = dmin, dmax
sel_carriers = st.sidebar.multiselect("Carriers", CARRIERS, default=CARRIERS)
sel_lanes = st.sidebar.multiselect("Lanes", sorted(LANES), default=sorted(LANES))
anomalies_only = st.sidebar.toggle("Fuel anomalies only", value=False)
min_gallons = st.sidebar.slider("Min gallons per fuel txn", 0.0, 100.0, 0.0, 5.0)


def _window(df: pd.DataFrame) -> pd.DataFrame:
    out = df[(df["event_date"] >= start) & (df["event_date"] <= end)]
    if "carrier_id" in out.columns and sel_carriers:
        out = out[out["carrier_id"].isin(sel_carriers)]
    if "lane_id" in out.columns and sel_lanes:
        out = out[out["lane_id"].isin(sel_lanes)]
    return out


f_fuel = _window(fuel_all)
if min_gallons > 0:
    f_fuel = f_fuel[f_fuel["gallons"] >= min_gallons]
if anomalies_only:
    f_fuel = f_fuel[f_fuel["anomaly_flag"] == 1]
f_loads = _window(loads_all)
f_move = _window(move_all)
f_gps = gps_all[
    (gps_all["event_date"] >= start) & (gps_all["event_date"] <= end)
    & (gps_all["lane_id"].isin(sel_lanes if sel_lanes else gps_all["lane_id"].unique()))
]
# KPI frame rebuilt from the filtered Gold facts, exactly like kpi_daily_ops.py
_kf = f_fuel.groupby("event_date").agg(
    fuel_spend=("total_amount", "sum"), fuel_gallons=("gallons", "sum"),
    fuel_anomaly_txns=("anomaly_flag", "sum"),
)
_kd = f_loads[f_loads["latest_status"] == "delivered"].groupby("event_date").agg(
    delivered_loads=("load_id", "nunique"), on_time_rate=("on_time", "mean")
)
_km = f_move.groupby("event_date").agg(
    est_miles=("est_miles", "sum"), avg_speed_mph=("avg_speed_mph", "mean")
)
f_kpi = _kd.join(_kf, how="outer").join(_km, how="outer").reset_index()
if not f_kpi.empty:
    f_kpi["fuel_spend_per_mile"] = f_kpi["fuel_spend"] / f_kpi["est_miles"]

# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period (unfiltered base)
# ----------------------------------------------------------------------------
def _money(x: float) -> str:
    return f"${x:,.0f}"


if not f_kpi.empty:
    spend = float(f_kpi["fuel_spend"].sum())
    span = max((end - start).days, 1)
    prior_fuel = fuel_all[
        (fuel_all["event_date"] >= start - pd.Timedelta(days=span))
        & (fuel_all["event_date"] < start)
    ]
    if sel_carriers:
        prior_fuel = prior_fuel[prior_fuel["carrier_id"].isin(sel_carriers)]
    prior_spend = float(prior_fuel["total_amount"].sum()) if not prior_fuel.empty else 0.0
    miles = float(f_kpi["est_miles"].sum())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Fuel spend (filtered)", _money(spend),
              delta=f"{_money(spend - prior_spend)} vs prior period" if prior_spend else None)
    c2.metric("On-time delivery", f"{float(f_kpi['on_time_rate'].mean()) * 100:.1f}%",
              delta=f"{int(f_kpi['delivered_loads'].sum())} delivered loads")
    c3.metric("Fuel spend / mile", f"${spend / miles:.2f}" if miles else "—",
              delta=f"{miles:,.0f} est. miles")
    c4.metric("Fuel anomalies", f"{int(f_kpi['fuel_anomaly_txns'].sum())}",
              delta="gallons > 80 or amount mismatch", delta_color="inverse")
    c5.metric("Gallons", f"{float(f_kpi['fuel_gallons'].sum()):,.0f}",
              delta=f"{len(f_fuel):,} fuel transactions")

tab_ops, tab_anim, tab_3d, tab_fuel, tab_drivers = st.tabs(
    ["📊 Daily ops KPIs", "🎬 Animated ops", "🧊 3D fleet space",
     "⛽ Fuel & anomalies", "🧑‍✈️ Drivers & lanes"]
)

with tab_ops:
    st.subheader("gold_kpi_daily_ops — the daily operating picture")
    if f_kpi.empty:
        st.info("No rows survive the current filters.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=f_kpi["event_date"], y=f_kpi["fuel_spend"],
                             name="Fuel spend ($)", marker_color="#f0a832"))
        fig.add_trace(go.Scatter(x=f_kpi["event_date"],
                                 y=f_kpi["on_time_rate"] * 100,
                                 name="On-time %", yaxis="y2", mode="lines+markers",
                                 line=dict(color="#2ee6c3", width=3)))
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)",
                          yaxis=dict(title="Fuel spend ($)"),
                          yaxis2=dict(title="On-time %", overlaying="y", side="right"),
                          legend=dict(orientation="h"))
        st.plotly_chart(fig, width="stretch")
        c1, c2 = st.columns(2)
        fig2 = px.area(f_kpi, x="event_date", y="fuel_spend_per_mile",
                       labels={"fuel_spend_per_mile": "Fuel $ / mile"},
                       template="plotly_dark", title="Fuel spend per mile")
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        c1.plotly_chart(fig2, width="stretch")
        fig3 = px.bar(f_kpi, x="event_date", y="delivered_loads",
                      labels={"delivered_loads": "Delivered loads"},
                      template="plotly_dark", title="Delivered loads per day",
                      color="delivered_loads", color_continuous_scale="Tealgrn")
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        c2.plotly_chart(fig3, width="stretch")
        st.dataframe(f_kpi.sort_values("event_date", ascending=False),
                     width="stretch", hide_index=True)

with tab_anim:
    st.subheader("Press play — daily fuel spend by lane, evolving day by day")
    if f_fuel.empty:
        st.info("No fuel rows survive the current filters.")
    else:
        daily = (f_fuel.groupby(["event_date", "lane_id"], as_index=False)
                 .agg(spend=("total_amount", "sum"), gallons=("gallons", "sum")))
        daily["frame"] = daily["event_date"].dt.strftime("%Y-%m-%d")
        lim = float(daily["spend"].max()) or 1.0
        fig_a = px.bar(daily.sort_values(["event_date", "lane_id"]),
                       x="lane_id", y="spend", color="lane_id",
                       animation_frame="frame", range_y=[0, lim * 1.15],
                       labels={"spend": "Fuel spend ($)", "lane_id": "Lane"},
                       template="plotly_dark",
                       title="Daily fuel spend by lane — animated")
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_a, width="stretch")

        cum = daily.copy()
        cum["cum_spend"] = cum.groupby("lane_id")["spend"].cumsum()
        fig_c = px.line(cum.sort_values(["event_date", "lane_id"]),
                        x="event_date", y="cum_spend", color="lane_id",
                        animation_frame="frame",
                        labels={"cum_spend": "Cumulative spend ($)"},
                        template="plotly_dark",
                        title="Cumulative spend race by lane — animated")
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_c.layout.updatemenus:
            fig_c.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_c, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

with tab_3d:
    st.subheader("🧊 3D fleet space — GPS pings in lat × lon × speed space")
    if f_gps.empty:
        st.info("No GPS pings survive the current filters.")
    else:
        fig3 = px.scatter_3d(f_gps.sort_values("event_date"), x="lon", y="lat",
                             z="speed_mph", color="lane", size_max=7, opacity=0.8,
                             hover_data=["truck_id", "event_date"],
                             labels={"lon": "Longitude", "lat": "Latitude",
                                     "speed_mph": "Speed (mph)"},
                             template="plotly_dark",
                             title="Every GPS ping as a point in lon × lat × speed space — drag to rotate")
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                           scene=dict(xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      zaxis=dict(backgroundcolor="rgba(20,24,32,.9)")),
                           margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig3, width="stretch")

        st.subheader("Fuel-spend surface — lane × date terrain")
        surf = (f_fuel.pivot_table(index="event_date", columns="lane_id",
                                   values="total_amount", aggfunc="sum")
                .sort_index().fillna(0.0))
        if surf.shape[0] > 2 and surf.shape[1] > 1:
            fig_s = go.Figure(data=[go.Surface(
                z=surf.to_numpy(),
                x=[d.strftime("%Y-%m-%d") for d in surf.index],
                y=list(surf.columns), colorscale="YlOrBr",
                colorbar=dict(title="Spend ($)"))])
            fig_s.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                scene=dict(xaxis_title="Date", yaxis_title="Lane",
                                           zaxis_title="Fuel spend ($)",
                                           camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9))),
                                margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_s, width="stretch")
        else:
            st.info("Not enough lane/date coverage in the current filter for a surface.")

with tab_fuel:
    st.subheader("gold_fact_fuel_txn — spend, gallons and the anomaly rule")
    if f_fuel.empty:
        st.info("No fuel rows survive the current filters.")
    else:
        fig = px.scatter(f_fuel, x="gallons", y="total_amount",
                         color=f_fuel["anomaly_flag"].map({0: "normal", 1: "anomaly"}),
                         hover_data=["truck_id", "station_id", "event_date"],
                         labels={"gallons": "Gallons", "total_amount": "Total ($)",
                                 "color": "Gold anomaly_flag"},
                         template="plotly_dark",
                         title="Gallons vs amount — anomalies are gallons > 80 or amount ≠ gallons × price")
        fig.add_vline(x=80, line_dash="dash", line_color="#ff7b72",
                      annotation_text="80 gal rule")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        by_station = (f_fuel.groupby("station_id", as_index=False)
                      .agg(spend=("total_amount", "sum"),
                           anomalies=("anomaly_flag", "sum"))
                      .sort_values("spend", ascending=False).head(12))
        fig_b = px.bar(by_station, x="station_id", y="spend", color="anomalies",
                       color_continuous_scale="Reds",
                       labels={"spend": "Spend ($)"}, template="plotly_dark",
                       title="Top stations by spend (colour = anomaly count)")
        fig_b.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_b, width="stretch")
        st.dataframe(f_fuel[f_fuel["anomaly_flag"] == 1]
                     .sort_values("event_date", ascending=False).head(50),
                     width="stretch", hide_index=True)

with tab_drivers:
    st.subheader("gold_fact_driver_movement + gold_fact_load_status")
    if f_move.empty:
        st.info("No movement rows survive the current filters.")
    else:
        util = (f_move.groupby("driver_id", as_index=False)
                .agg(est_miles=("est_miles", "sum"),
                     avg_speed=("avg_speed_mph", "mean"),
                     days=("event_date", "nunique"))
                .sort_values("est_miles", ascending=False))
        fig = px.bar(util.head(15), x="driver_id", y="est_miles", color="avg_speed",
                     color_continuous_scale="Viridis",
                     labels={"est_miles": "Est. miles (speed × hours)"},
                     template="plotly_dark", title="Driver utilization — top 15 by est. miles")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        status_mix = (f_loads.groupby(["event_date", "latest_status"], as_index=False)
                      .size().rename(columns={"size": "loads"}))
        status_mix["frame"] = status_mix["event_date"].dt.strftime("%Y-%m-%d")
        fig_s = px.bar(status_mix.sort_values(["event_date", "latest_status"]),
                       x="latest_status", y="loads", color="latest_status",
                       animation_frame="frame",
                       labels={"loads": "Loads", "latest_status": "Latest status"},
                       template="plotly_dark",
                       title="Load lifecycle mix by day — animated (created → assigned → picked_up → delivered)")
        fig_s.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_s.layout.updatemenus:
            fig_s.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_s, width="stretch")
        lane_tbl = (f_loads[f_loads["latest_status"] == "delivered"]
                    .groupby("lane_id", as_index=False)
                    .agg(delivered=("load_id", "nunique"), on_time_rate=("on_time", "mean")))
        lane_tbl["lane"] = lane_tbl["lane_id"].map(
            {k: f"{v[0]} → {v[1]}" for k, v in LANES.items()})
        st.dataframe(lane_tbl, width="stretch", hide_index=True)
