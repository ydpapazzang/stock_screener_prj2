from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def create_monthly_chart(monthly_df: pd.DataFrame, name: str) -> go.Figure:
    chart_df = monthly_df.tail(60)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["close"],
            mode="lines+markers",
            name="월봉 종가",
            line=dict(color="#0f172a", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df.index,
            y=chart_df["ma10"],
            mode="lines",
            name="10개월선",
            line=dict(color="#dc2626", width=2),
        )
    )
    fig.update_layout(
        title=f"{name} 월봉 종가 vs 10개월선",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    fig.update_yaxes(title="가격")
    return fig
