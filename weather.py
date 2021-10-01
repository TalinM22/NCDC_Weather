import urllib.request as url
from gzip import GzipFile
import pandas as pd
import numpy as np
import datetime
import json
import requests
import time


##Retry logic to download file incase of failure
try:
    for i in range(0, 5):
        try:
            req = url.Request(
                "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2021.csv.gz"
            )
            z_f = url.urlopen(req)
            f = GzipFile(fileobj=z_f, mode="r")
            data = pd.read_csv(f)
            data.to_csv("File_21.csv", index = False)
        except:
            continue
        break
except:
    print("Data Download Failed")


data = data.drop(data.columns[[4, 5, 6, 7]], axis=1)

data.columns = ["GHCN_Code", "Date", "Type", "Value"]

data["Date"] = pd.to_datetime(data["Date"].astype(str), format="%Y-%m-%d")
data = data.loc[data["Type"].isin(["PRCP", "TAVG", "TMIN", "TMAX"])]


data.loc[data["Type"] == "TMAX", "Value"] = data["Value"].div(10)
data.loc[data["Type"] == "TMIN", "Value"] = data["Value"].div(10)
data.loc[data["Type"] == "TAVG", "Value"] = data["Value"].div(10)
data.loc[data["Type"] == "PRCP", "Value"] = data["Value"].div(10)


data = (
    data.pivot_table("Value", ["GHCN_Code", "Date"], "Type")
    .rename_axis(None, axis=1)
    .reset_index()
)


data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
df_list = []

for (columns, group) in data.groupby(["GHCN_Code"]):
    idx = pd.MultiIndex.from_product(
        [
            group["GHCN_Code"].unique(),
            pd.date_range(
                group["Date"].min().replace(day=1), end=group["Date"].max(), freq="D"
            ),
        ],
        names=["GHCN_Code", "Date"],
    )
    group = group.set_index(["GHCN_Code", "Date"]).reindex(idx).reset_index()
    group["GHCN_Code"] = group["GHCN_Code"].fillna(method="bfill")
    df_list.append(group)

data = pd.concat(df_list, ignore_index=True)


if [(data["TMIN"].isnull()) & (data["TAVG"].notnull()) & (data["TMAX"].notnull())]:
    data["TMIN"] = (2 * data["TAVG"]) - data["TMAX"]

if [(data["TMAX"].isnull()) & (data["TMIN"].notnull()) & (data["TAVG"].notnull())]:
    data["TMAX"] = (2 * data["TAVG"]) - data["TMIN"]

if [(data["TAVG"].isnull()) & (data["TMIN"].notnull()) & (data["TMAX"].notnull())]:
    data["TAVG"] = (data["TMIN"] + data["TMAX"]) / 2


data["z_score_tmax"] = (
    data.groupby("GHCN_Code")["TMAX"].apply(
        lambda x: (x - x.mean()) / x.std()).abs()
)
data["z_score_tmin"] = (
    data.groupby("GHCN_Code")["TMIN"].apply(
        lambda x: (x - x.mean()) / x.std()).abs()
)
data["z_score_tavg"] = (
    data.groupby("GHCN_Code")["TAVG"].apply(
        lambda x: (x - x.mean()) / x.std()).abs()
)


data["TMAX"] = np.where(data.z_score_tmax >= 2.6, np.nan, data.TMAX)
data["TMIN"] = np.where(data.z_score_tmin >= 2.6, np.nan, data.TMIN)
data["TAVG"] = np.where(data.z_score_tavg >= 2.6, np.nan, data.TAVG)


data["Date"] = pd.to_datetime(data["Date"].astype(str), format="%Y-%m-%d")

data = data[["GHCN_Code","Date","PRCP","TAVG","TMIN","TMAX"]]

print(data)
