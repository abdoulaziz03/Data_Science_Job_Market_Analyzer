"""
dashboard/dashboard.py
Dashboard interactif avec Plotly Dash pour analyser le marché Data Science
Lancement : python dashboard.py  →  http://localhost:8050
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output

# ─── Chargement des données ────────────────────────────────────────────────────
DATA_PATH = "../data/processed/jobs_clean.csv"

def load_data() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        import numpy as np
        np.random.seed(42)
        n = 200
        return pd.DataFrame({
            "source":     np.random.choice(["Adzuna", "AI-Jobs.net"], n),
            "title":      np.random.choice(["Data Scientist", "Data Engineer", "ML Engineer", "Data Analyst"], n),
            "company":    [f"Entreprise {i}" for i in range(n)],
            "location":   np.random.choice(["Paris", "Lyon", "Bordeaux", "Lille", "Nantes", "Remote"], n),
            "salary_avg": np.random.choice([None, *np.random.randint(35000, 90000, 100).tolist()], n),
            "contract":   np.random.choice(["CDI", "CDD", "Freelance", "Non précisé"], n, p=[0.6, 0.15, 0.15, 0.1]),
            "category":   np.random.choice(["Data Science", "IT", "Engineering"], n),
        })
    return pd.read_csv(DATA_PATH, encoding="utf-8-sig")

df = load_data()

# ─── App Dash ──────────────────────────────────────────────────────────────────
app = Dash(__name__, title="Dashboard - Marché Data Science 🇫🇷")

COLORS = {
    "bg":      "#0f172a",
    "card":    "#1e293b",
    "accent":  "#6366f1",
    "text":    "#e2e8f0",
    "subtext": "#94a3b8",
}

CARD_STYLE = {
    "backgroundColor": COLORS["card"],
    "borderRadius": "12px",
    "padding": "20px",
    "marginBottom": "20px",
}

# ─── Layout ───────────────────────────────────────────────────────────────────
app.layout = html.Div(style={"backgroundColor": COLORS["bg"], "minHeight": "100vh", "padding": "30px",
                              "fontFamily": "Inter, sans-serif", "color": COLORS["text"]}, children=[

    html.Div([
        html.H1("📊 Marché de l'Emploi Data Science", style={"margin": 0, "fontSize": "28px"}),
        html.P(f"{len(df)} offres analysées", style={"color": COLORS["subtext"], "margin": "5px 0 0 0"}),
    ], style={**CARD_STYLE, "marginBottom": "30px"}),

    html.Div([
        html.Div([
            html.Label("Source", style={"color": COLORS["subtext"], "fontSize": "13px"}),
            dcc.Dropdown(
                id="filter-source",
                options=[{"label": s, "value": s} for s in sorted(df["source"].dropna().unique())],
                multi=True, placeholder="Toutes les sources",
                style={"backgroundColor": COLORS["bg"], "color": COLORS["text"]},
            ),
        ], style={"flex": 1, "marginRight": "20px"}),

        html.Div([
            html.Label("Contrat", style={"color": COLORS["subtext"], "fontSize": "13px"}),
            dcc.Dropdown(
                id="filter-contract",
                options=[{"label": c, "value": c} for c in sorted(df["contract"].dropna().unique())],
                multi=True, placeholder="Tous les contrats",
                style={"backgroundColor": COLORS["bg"], "color": COLORS["text"]},
            ),
        ], style={"flex": 1}),
    ], style={**CARD_STYLE, "display": "flex"}),

    html.Div(id="kpis", style={"display": "flex", "gap": "20px", "marginBottom": "20px"}),

    html.Div([
        html.Div(dcc.Graph(id="chart-locations"), style={"flex": 1, **CARD_STYLE, "marginRight": "20px"}),
        html.Div(dcc.Graph(id="chart-contracts"), style={"flex": 1, **CARD_STYLE}),
    ], style={"display": "flex"}),

    html.Div([
        html.Div(dcc.Graph(id="chart-titles"),   style={"flex": 1, **CARD_STYLE, "marginRight": "20px"}),
        html.Div(dcc.Graph(id="chart-salaries"), style={"flex": 1, **CARD_STYLE}),
    ], style={"display": "flex"}),
])


# ─── Callbacks ────────────────────────────────────────────────────────────────
def filter_df(sources, contracts):
    filtered = df.copy()
    if sources:
        filtered = filtered[filtered["source"].isin(sources)]
    if contracts:
        filtered = filtered[filtered["contract"].isin(contracts)]
    return filtered


PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="#e2e8f0",
    margin=dict(t=40, b=20, l=20, r=20),
)


@app.callback(
    Output("kpis",            "children"),
    Output("chart-locations", "figure"),
    Output("chart-contracts", "figure"),
    Output("chart-titles",    "figure"),
    Output("chart-salaries",  "figure"),
    Input("filter-source",    "value"),
    Input("filter-contract",  "value"),
)
def update_dashboard(sources, contracts):
    fdf = filter_df(sources, contracts)

    def empty_fig(message="Aucune donnée"):
        fig = go.Figure()
        fig.add_annotation(text=message, showarrow=False,
                           font=dict(color=COLORS["subtext"], size=16),
                           xref="paper", yref="paper", x=0.5, y=0.5)
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # KPIs
    salary_data = fdf["salary_avg"].dropna() if not fdf.empty else pd.Series([], dtype=float)
    kpi_values = [
        ("💼 Offres",      f"{len(fdf):,}"),
        ("💰 Salaire moy", f"{salary_data.mean():,.0f} €" if len(salary_data) else "N/A"),
        ("📍 Villes",      f"{fdf['location'].nunique()}" if not fdf.empty else "0"),
        ("🏢 Entreprises", f"{fdf['company'].nunique()}"  if not fdf.empty else "0"),
    ]
    kpi_cards = [
        html.Div([
            html.P(label, style={"margin": 0, "color": COLORS["subtext"], "fontSize": "13px"}),
            html.H2(value, style={"margin": "5px 0 0 0", "fontSize": "24px", "color": COLORS["accent"]}),
        ], style={**CARD_STYLE, "flex": 1, "textAlign": "center", "marginBottom": 0})
        for label, value in kpi_values
    ]

    if fdf.empty:
        return kpi_cards, empty_fig("Aucune offre"), empty_fig(), empty_fig(), empty_fig()

    # Localisations
    top_loc = fdf["location"].value_counts().head(10).reset_index()
    top_loc.columns = ["location", "count"]
    fig_loc = px.bar(
        top_loc, x="count", y="location", orientation="h",
        title="Top 10 Villes", color="count",
        color_continuous_scale="purples"
    )
    fig_loc.update_layout(**PLOT_LAYOUT, coloraxis_showscale=False)

    # Types de contrats
    contract_counts = fdf["contract"].value_counts().reset_index()
    contract_counts.columns = ["contract", "count"]
    fig_contracts = px.pie(
        contract_counts, names="contract", values="count",
        title="Répartition des contrats",
        color_discrete_sequence=px.colors.sequential.Plasma_r
    )
    fig_contracts.update_layout(**PLOT_LAYOUT)

    # Titres de postes
    top_titles = fdf["title"].value_counts().head(10).reset_index()
    top_titles.columns = ["title", "count"]
    fig_titles = px.bar(
        top_titles, x="count", y="title", orientation="h",
        title="Top 10 Titres de postes", color="count",
        color_continuous_scale="teal"
    )
    fig_titles.update_layout(**PLOT_LAYOUT, coloraxis_showscale=False)

    # Distribution des salaires
    if salary_data.empty:
        fig_sal = empty_fig("Pas de données salariales")
    else:
        fig_sal = px.histogram(
            fdf.dropna(subset=["salary_avg"]),
            x="salary_avg", nbins=20,
            title="Distribution des salaires (€/an)",
            color_discrete_sequence=[COLORS["accent"]]
        )
        fig_sal.update_layout(**PLOT_LAYOUT)

    return kpi_cards, fig_loc, fig_contracts, fig_titles, fig_sal


# ─── Lancement ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
