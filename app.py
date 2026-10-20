"""
TransitPulse | Public Transport Ridership Pattern Analysis Dashboard.

A high-performance, interactive Streamlit analytics dashboard backed by
Apache PySpark data processing. Demonstrates big data aggregations,
time-series trends, route demand profiling, and operational comparisons.
"""

import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configure page layout and visual identity
st.set_page_config(
    page_title="TransitPulse | Ridership Analytics",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling for Academic & Professional Aesthetics
CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Academic Banner */
    .academic-banner {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 18px 24px;
        margin-bottom: 24px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
    }
    .academic-title {
        color: #38bdf8;
        font-size: 1.5rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin-bottom: 4px;
    }
    .academic-subtitle {
        color: #94a3b8;
        font-size: 0.92rem;
        line-height: 1.5;
    }
    .synthetic-badge {
        display: inline-block;
        background-color: #f59e0b;
        color: #000;
        font-size: 0.75rem;
        font-weight: 700;
        padding: 2px 10px;
        border-radius: 9999px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-left: 10px;
        vertical-align: middle;
    }
    
    /* Metric Cards */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
    }
    .metric-label {
        font-size: 0.82rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #0f172a;
        line-height: 1.1;
    }
    .metric-subtext {
        font-size: 0.8rem;
        color: #10b981;
        font-weight: 500;
        margin-top: 4px;
    }

    /* Insights Box */
    .insight-card {
        background: #f8fafc;
        border-left: 4px solid #38bdf8;
        border-radius: 0 8px 8px 0;
        padding: 14px 18px;
        margin-bottom: 12px;
        font-size: 0.9rem;
        color: #334155;
    }

    /* Chart Containers */
    .chart-container {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 20px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ==========================================
# Data Loading & Caching
# ==========================================
DATA_DIR = Path("data/processed")


@st.cache_data(show_spinner=False)
def load_all_processed_data():
    """Load precomputed PySpark pipeline output files into memory."""
    files = {
        "route_summary": DATA_DIR / "route_summary.csv",
        "monthly_summary": DATA_DIR / "monthly_summary.csv",
        "day_type_summary": DATA_DIR / "day_type_summary.csv",
        "daily_summary": DATA_DIR / "daily_summary.csv",
        "route_day_type": DATA_DIR / "route_day_type_summary.csv",
        "heatmap": DATA_DIR / "route_monthly_heatmap.csv",
        "parquet_clean": DATA_DIR / "ridership.parquet",
    }

    missing = [name for name, p in files.items() if not p.exists()]
    if missing:
        return None, missing

    dfs = {
        "route_summary": pd.read_csv(files["route_summary"]),
        "monthly_summary": pd.read_csv(files["monthly_summary"]),
        "day_type_summary": pd.read_csv(files["day_type_summary"]),
        "daily_summary": pd.read_csv(files["daily_summary"]),
        "route_day_type": pd.read_csv(files["route_day_type"]),
        "heatmap": pd.read_csv(files["heatmap"]),
        "clean_records": pd.read_parquet(files["parquet_clean"]),
    }
    # Ensure datetime formats
    dfs["clean_records"]["service_date"] = pd.to_datetime(dfs["clean_records"]["service_date"]).dt.date
    dfs["daily_summary"]["service_date"] = pd.to_datetime(dfs["daily_summary"]["service_date"]).dt.date

    return dfs, []


def render_pipeline_missing_view(missing_files: List[str]):
    """Guide the user to run the PySpark pipeline when outputs are not found."""
    st.error("⚠️ Processed analytical datasets are missing or not yet generated.")
    st.info(
        "To generate the dataset and execute the PySpark distributed pipeline, run:\n\n"
        "```powershell\n"
        "python scripts/generate_data.py\n"
        "python -m src.pipeline\n"
        "```"
    )
    st.stop()


# ==========================================
# Main Application Flow
# ==========================================
def main():
    data, missing = load_all_processed_data()
    if data is None:
        render_pipeline_missing_view(missing)

    clean_df = data["clean_records"]
    all_routes = sorted(clean_df["route_name"].unique())
    all_day_types = ["Weekday", "Saturday", "Sunday/Holiday"]
    min_date = clean_df["service_date"].min()
    max_date = clean_df["service_date"].max()

    # Academic Header
    st.markdown(
        f"""
        <div class="academic-banner">
            <div class="academic-title">
                TransitPulse | Public Transport Ridership Pattern Analysis
                <span class="synthetic-badge">Synthetic Demo Data</span>
            </div>
            <div class="academic-subtitle">
                Big Data Analytics College Mini-Project powered by <strong>Apache PySpark 4.0</strong>, 
                <strong>Spark SQL</strong>, and <strong>Streamlit</strong>.<br>
                <em>Academic Disclosure: Records represent mathematically simulated urban transit patterns across 20 fictional bus routes (2024–2025). Not real transit agency records.</em>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ==========================================
    # Sidebar Filters
    # ==========================================
    with st.sidebar:
        st.markdown("### 🎛️ Analytics Filters")

        # Preset date filters
        preset = st.radio(
            "Quick Date Presets:",
            options=["All Time (2024-2025)", "Full Year 2024", "Full Year 2025", "Custom Range"],
            index=0,
        )

        if preset == "Full Year 2024":
            default_start = datetime.date(2024, 1, 1)
            default_end = datetime.date(2024, 12, 31)
        elif preset == "Full Year 2025":
            default_start = datetime.date(2025, 1, 1)
            default_end = datetime.date(2025, 12, 31)
        else:
            default_start = min_date
            default_end = max_date

        date_range = st.date_input(
            "Service Date Range:",
            value=(default_start, default_end),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_filter, end_filter = date_range
        else:
            start_filter, end_filter = min_date, max_date

        st.markdown("---")

        # Day type filter
        selected_day_types = st.multiselect(
            "Day Types:",
            options=all_day_types,
            default=all_day_types,
        )

        st.markdown("---")

        # Route Selection with Select All toggle
        select_all_routes = st.checkbox("Select All 20 Routes", value=True)
        if select_all_routes:
            selected_routes = st.multiselect(
                "Bus Routes:",
                options=all_routes,
                default=all_routes,
            )
        else:
            selected_routes = st.multiselect(
                "Bus Routes:",
                options=all_routes,
                default=all_routes[:5],
            )

        st.markdown("---")
        st.caption("🚀 Pipeline: PySpark 4.0.4 Local Engine\n📊 Visuals: Plotly Interactive")

    # ==========================================
    # Filter Dataset
    # ==========================================
    mask = (
        (clean_df["service_date"] >= start_filter)
        & (clean_df["service_date"] <= end_filter)
        & (clean_df["day_type"].isin(selected_day_types))
        & (clean_df["route_name"].isin(selected_routes))
    )
    filtered_df = clean_df.loc[mask]

    # Empty State Handling
    if filtered_df.empty:
        st.warning("⚠️ No ridership records match the selected filter combination. Please broaden your date range or route selection.")
        st.stop()

    # ==========================================
    # Key Performance Indicators (KPIs)
    # ==========================================
    total_rides = int(filtered_df["rides"].sum())
    unique_dates = filtered_df["service_date"].nunique()
    active_routes_count = filtered_df["route_name"].nunique()
    avg_daily_system = round(total_rides / unique_dates, 1) if unique_dates > 0 else 0
    peak_record = filtered_df.loc[filtered_df["rides"].idxmax()]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Total Passenger Rides</div>
                <div class="metric-value">{total_rides:,}</div>
                <div class="metric-subtext">Across {unique_dates} service days</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Avg Daily System Rides</div>
                <div class="metric-value">{avg_daily_system:,.0f}</div>
                <div class="metric-subtext">Rides per calendar date</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Active Routes</div>
                <div class="metric-value">{active_routes_count} / {len(all_routes)}</div>
                <div class="metric-subtext">Selected network corridors</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">Peak Route-Day Rides</div>
                <div class="metric-value">{int(peak_record['rides']):,}</div>
                <div class="metric-subtext">{peak_record['route_id']} on {peak_record['service_date']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # Tabbed Analytical Views
    # ==========================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Time Trends & Rolling Demand",
        "🚌 Route Performance & Rankings",
        "🗓️ Day-Type & Operational Patterns",
        "🔥 Route-Month Demand Heatmap",
        "📋 Filtered Data & Export",
    ])

    # ------------------------------------------
    # TAB 1: Time Series Trends
    # ------------------------------------------
    with tab1:
        st.subheader("Daily System Ridership Trend & 7-Day Moving Average")
        daily_agg = (
            filtered_df.groupby(["service_date", "day_type"])["rides"]
            .sum()
            .reset_index()
            .rename(columns={"rides": "daily_rides"})
            .sort_values("service_date")
        )
        daily_agg["rolling_7d"] = daily_agg["daily_rides"].rolling(7, min_periods=1).mean()

        fig_daily = go.Figure()
        fig_daily.add_trace(
            go.Scatter(
                x=daily_agg["service_date"],
                y=daily_agg["daily_rides"],
                mode="lines",
                name="Daily Ridership",
                line=dict(color="#94a3b8", width=1),
                opacity=0.6,
            )
        )
        fig_daily.add_trace(
            go.Scatter(
                x=daily_agg["service_date"],
                y=daily_agg["rolling_7d"],
                mode="lines",
                name="7-Day Rolling Trend",
                line=dict(color="#0284c7", width=2.5),
            )
        )
        fig_daily.update_layout(
            template="plotly_white",
            height=380,
            margin=dict(l=20, r=20, t=30, b=20),
            hovermode="x unified",
            xaxis_title="Service Date",
            yaxis_title="Total Daily Passenger Rides",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_daily, use_container_width=True)

        # Monthly YoY Comparison
        st.subheader("Monthly Total System Demand (YoY Comparison)")
        filtered_df["year"] = pd.to_datetime(filtered_df["service_date"]).dt.year
        filtered_df["month"] = pd.to_datetime(filtered_df["service_date"]).dt.month
        monthly_comp = (
            filtered_df.groupby(["year", "month"])["rides"]
            .sum()
            .reset_index()
            .sort_values(["year", "month"])
        )
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        monthly_comp["month_name"] = monthly_comp["month"].map(month_names)

        fig_monthly = px.bar(
            monthly_comp,
            x="month_name",
            y="rides",
            color="year",
            barmode="group",
            text_auto=".2s",
            color_discrete_sequence=["#38bdf8", "#0369a1"],
            category_orders={"month_name": list(month_names.values())},
            labels={"rides": "Total Rides", "month_name": "Month", "year": "Year"},
        )
        fig_monthly.update_layout(
            template="plotly_white",
            height=340,
            margin=dict(l=20, r=20, t=30, b=20),
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

    # ------------------------------------------
    # TAB 2: Route Rankings & Demand Profile
    # ------------------------------------------
    with tab2:
        st.subheader("Route Demand Rankings (Filtered Range)")
        route_agg = (
            filtered_df.groupby(["route_id", "route_name"])
            .agg(
                total_rides=("rides", "sum"),
                avg_daily_rides=("rides", "mean"),
                min_rides=("rides", "min"),
                max_rides=("rides", "max"),
            )
            .reset_index()
            .sort_values("total_rides", ascending=True)
        )

        fig_routes = px.bar(
            route_agg,
            x="total_rides",
            y="route_name",
            orientation="h",
            color="avg_daily_rides",
            color_continuous_scale="Blues",
            labels={"total_rides": "Total Rides", "route_name": "Bus Route", "avg_daily_rides": "Daily Mean"},
            text_auto=".2s",
        )
        fig_routes.update_layout(
            template="plotly_white",
            height=max(400, len(route_agg) * 24),
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig_routes, use_container_width=True)

        # Top 5 and Bottom 5 summary tables
        c_top, c_bot = st.columns(2)
        with c_top:
            st.markdown("##### 🏆 Top 5 High-Demand Routes")
            top_5_table = (
                route_agg.sort_values("total_rides", ascending=False)
                .head(5)[["route_id", "route_name", "total_rides", "avg_daily_rides"]]
                .copy()
            )
            top_5_table["avg_daily_rides"] = top_5_table["avg_daily_rides"].round(1)
            st.dataframe(top_5_table, use_container_width=True, hide_index=True)

        with c_bot:
            st.markdown("##### 📍 Bottom 5 Feeder/Local Routes")
            bot_5_table = (
                route_agg.sort_values("total_rides", ascending=True)
                .head(5)[["route_id", "route_name", "total_rides", "avg_daily_rides"]]
                .copy()
            )
            bot_5_table["avg_daily_rides"] = bot_5_table["avg_daily_rides"].round(1)
            st.dataframe(bot_5_table, use_container_width=True, hide_index=True)

    # ------------------------------------------
    # TAB 3: Day Type Operational Patterns
    # ------------------------------------------
    with tab3:
        st.subheader("Operational Schedule Demand Comparison")
        day_type_agg = (
            filtered_df.groupby("day_type")
            .agg(
                total_rides=("rides", "sum"),
                service_days=("service_date", "nunique"),
                avg_route_rides=("rides", "mean"),
            )
            .reset_index()
        )
        day_type_agg["avg_daily_system"] = (
            day_type_agg["total_rides"] / day_type_agg["service_days"]
        ).round(1)
        day_type_agg["pct_share"] = (
            (day_type_agg["total_rides"] / day_type_agg["total_rides"].sum()) * 100
        ).round(1)

        c1, c2 = st.columns(2)
        with c1:
            fig_dt_bar = px.bar(
                day_type_agg,
                x="day_type",
                y="avg_daily_system",
                color="day_type",
                text_auto=".2s",
                color_discrete_map={
                    "Weekday": "#0284c7",
                    "Saturday": "#38bdf8",
                    "Sunday/Holiday": "#94a3b8",
                },
                labels={"avg_daily_system": "Average Daily System Rides", "day_type": "Day Type"},
                title="Average Daily System Passenger Volume",
            )
            fig_dt_bar.update_layout(template="plotly_white", showlegend=False, height=350)
            st.plotly_chart(fig_dt_bar, use_container_width=True)

        with c2:
            fig_dt_pie = px.pie(
                day_type_agg,
                names="day_type",
                values="total_rides",
                color="day_type",
                color_discrete_map={
                    "Weekday": "#0284c7",
                    "Saturday": "#38bdf8",
                    "Sunday/Holiday": "#94a3b8",
                },
                title="Total Ridership Share by Day Type",
                hole=0.45,
            )
            fig_dt_pie.update_layout(template="plotly_white", height=350)
            st.plotly_chart(fig_dt_pie, use_container_width=True)

        st.markdown("##### Detailed Day-Type Distribution Table")
        st.dataframe(day_type_agg, use_container_width=True, hide_index=True)

    # ------------------------------------------
    # TAB 4: Heatmap Matrix
    # ------------------------------------------
    with tab4:
        st.subheader("Route-by-Month Passenger Intensity Matrix")
        filtered_df["year_month"] = pd.to_datetime(filtered_df["service_date"]).dt.strftime("%Y-%m")
        heat_matrix = (
            filtered_df.groupby(["route_name", "year_month"])["rides"]
            .sum()
            .reset_index()
            .pivot(index="route_name", columns="year_month", values="rides")
            .fillna(0)
        )

        fig_heat = px.imshow(
            heat_matrix,
            color_continuous_scale="Blues",
            aspect="auto",
            labels=dict(x="Year-Month", y="Bus Route", color="Monthly Rides"),
        )
        fig_heat.update_layout(
            template="plotly_white",
            height=max(420, len(heat_matrix) * 22),
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    # ------------------------------------------
    # TAB 5: Data Preview & Export
    # ------------------------------------------
    with tab5:
        st.subheader("Filtered Dataset Preview & Data Export")
        st.caption(f"Showing first 500 rows out of {len(filtered_df):,} matching observations.")
        preview_cols = ["route_id", "route_name", "service_date", "day_type", "rides"]
        st.dataframe(filtered_df[preview_cols].head(500), use_container_width=True, hide_index=True)

        csv_bytes = filtered_df[preview_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Filtered Records (CSV)",
            data=csv_bytes,
            file_name=f"transitpulse_filtered_{start_filter}_to_{end_filter}.csv",
            mime="text/csv",
        )

    # ==========================================
    # Automated Analytical Insights
    # ==========================================
    st.markdown("---")
    st.subheader("💡 Automated Operational Insights")

    # Math-derived dynamic observations
    most_popular_route = route_agg.sort_values("total_rides", ascending=False).iloc[0]
    weekday_share = (
        day_type_agg.loc[day_type_agg["day_type"] == "Weekday", "pct_share"].values[0]
        if "Weekday" in day_type_agg["day_type"].values
        else 0
    )
    weekend_share = 100.0 - weekday_share

    st.markdown(
        f"""
        <div class="insight-card">
            <strong>Key Driver Route:</strong> The highest ridership route in the current filter window is 
            <strong>{most_popular_route['route_name']}</strong>, contributing 
            <strong>{int(most_popular_route['total_rides']):,}</strong> passenger rides 
            (mean daily ridership: {most_popular_route['avg_daily_rides']:,.1f}).
        </div>
        <div class="insight-card">
            <strong>Commuter Dependency:</strong> Weekday service accounts for <strong>{weekday_share:.1f}%</strong> 
            of total ridership volume, demonstrating strong recurring commuter reliance. Saturday and Sunday/Holiday 
            services generate the remaining <strong>{weekend_share:.1f}%</strong>.
        </div>
        <div class="insight-card">
            <strong>Extreme Observation Event:</strong> The maximum observed single-day volume occurred on 
            <strong>{peak_record['service_date']}</strong> on route <strong>{peak_record['route_name']}</strong> 
            with <strong>{int(peak_record['rides']):,}</strong> recorded passenger rides.
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
