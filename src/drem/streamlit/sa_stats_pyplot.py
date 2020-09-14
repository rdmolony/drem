import geopandas as gpd
import streamlit as st

from matplotlib import pyplot

from drem.filepaths import PROCESSED_DIR


st.title("Residential Energy Demand")

st.write("Here's our first attempt at using data to create a table:")
sa_statistics = gpd.read_parquet(PROCESSED_DIR / "sa_statistics.parquet")

st.write(sa_statistics[["small_area", "total_heat_demand_per_sa"]])

fig, ax = pyplot.subplots(ncols=1, sharex=True, sharey=True)
sa_statistics.plot(
    ax=ax,
    column="total_heat_demand_per_sa",
    legend=True,
    legend_kwds={"label": "Residential Energy Demand [GWh/year]"},
    figsize=(50, 50),
)
st.pyplot(fig=fig)
