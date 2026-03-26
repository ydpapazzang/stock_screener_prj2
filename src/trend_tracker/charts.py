from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def _create_price_ma_chart(
    frame: pd.DataFrame,
    name: str,
    price_label: str,
    ma_columns: list[tuple[str, str, str]],
    title: str,
    tail_count: int,
) -> go.Figure:
    chart_df = frame.tail(tail_count).copy()
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["close"],
            mode="lines+markers",
            name=price_label,
            line=dict(color="#0f172a", width=2),
        )
    )

    for column, label, color in ma_columns:
        if column not in chart_df.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=chart_df.index,
                y=chart_df[column],
                mode="lines",
                name=label,
                line=dict(color=color, width=2),
            )
        )

    fig.update_layout(
        title=f"{name} {title}",
        height=420,
        margin=dict(l=20, r=20, t=70, b=70),
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
    )
    fig.update_yaxes(title="가격")
    return fig


def create_monthly_chart(monthly_df: pd.DataFrame, name: str) -> go.Figure:
    return _create_price_ma_chart(
        frame=monthly_df,
        name=name,
        price_label="월봉 종가",
        ma_columns=[("ma10", "10개월선", "#dc2626")],
        title="월봉 종가 vs 10개월선",
        tail_count=60,
    )


def create_weekly_chart(weekly_df: pd.DataFrame, name: str) -> go.Figure:
    return _create_price_ma_chart(
        frame=weekly_df,
        name=name,
        price_label="주봉 종가",
        ma_columns=[
            ("ma10", "10주선", "#dc2626"),
            ("ma20", "20주선", "#2563eb"),
            ("ma40", "40주선", "#16a34a"),
        ],
        title="주봉 종가 vs 10·20·40주선",
        tail_count=80,
    )
