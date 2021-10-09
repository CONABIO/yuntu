"""Basic functions for ecological analysis"""
import datetime
import numpy as np
import pytz
from scipy.special import comb
import itertools
import matplotlib.pyplot as plt
import matplotlib.cm as cm

def pair_counts(row, a, b):
    nm = int(row[a]>0 and row[b]>0)
    return pd.Series({"label_a": a, "label_b": b, "a_b": nm})

def coocurrence(activities, labels=None):
    if labels is None:
        labels = [col for col in activities if col not in ["abs_start_time", "abs_end_time"]]

    cocounts = []
    for pair in itertools.combinations(labels, 2):
        pcounts = activities.apply(pair_counts, a=pair[0], b=pair[1], axis=1)
        cocounts.append(pcounts)

    cooc = pd.concat(cocounts)

    return cooc.groupby(by=["label_a", "label_b"]).sum().reset_index()

def diversity(row, keys, div_type="Shannon"):
    """Compute diversity for each row"""
    total = np.maximum(np.sum(row),1)
    p = counts / total
    rest_p = p[p>0].astype(float)
    div = -np.sum(rest_p*np.log(rest_p))
    if div_type == "Hill":
        div = np.exp(div)
    abs_start_time, abs_end_time = row[["abs_start_time", "abs_end_time"]].values
    return pd.Series({"abs_start_time": abs_start_time,
                      "abs_end_time": abs_end_time,
                      "diversity": div})

def richness(row, labels):
    """Compute richness for each row"""
    rich = np.sum(np.where(row[labels] > 0, 1, 0))
    abs_start_time, abs_end_time = row[["abs_start_time", "abs_end_time"]].values
    return pd.Series({"abs_start_time": abs_start_time,
                      "abs_start_time": abs_end_time,
                      "richness": rich})

def rarefaction(row, size, labels):
    """Compute rarefaction for each row."""
    x = row[labels].values.astype('float')
    notabs = ~np.isnan(x)
    t = x[notabs]
    N = np.sum(t)
    diff = N - t
    rare_calc = np.sum(1 - comb(diff, size)/comb(N, size))

    return pd.Series({"abs_start_time": row["abs_start_time"],
                      "abs_end_time": row["abs_end_time"],
                      "rarefaction" : rare_calc})

def rarefy(i, Sn, n, x, exact=False):
    """Simulate values for rarefaction curve."""
    if not exact:
        sBar = Sn -  np.sum(comb(n-x, i))/comb(n, i)
    else:
        sBar = Sn - np.sum(np.array([comb(n-val, i, exact=True) for val in x]))/comb(n, i, exact=True)
    return sBar

def rarefaction_curve(row, view_time_zone="America/Mexico_city",
                      cmap = cm.get_cmap('Spectral'), color=None,
                      labels=None, plot_label=None, exact=False):
    """Compute rarefaction curve for each row and plot"""
    x = row[labels]
    ix = x['index']
    z = x.drop('index')
    notabs = ~np.isnan(z)
    y = z[notabs]
    n = np.sum(y)
    Sn = len(z)

    if "color" in row:
        color = cmap(row["color"])

    if not exact:
        y = y.astype("float")
        iPred = np.linspace(0, n, 1000)
        yhat = [rarefy(i, Sn, n, y) for i in iPred]

        if plot_label is None:
            plot_label = (row["abs_start_time"]
                          .astimezone(view_time_zone)
                          .strftime(format="%H:%M"))
    else:
        y = y.astype("int")
        iPred = np.arange(1, n, int(np.floor(n/1000)))
        yhat = [rarefy(i, Sn, n, y, exact=True) for i in iPred]

    plt.plot(iPred, yhat, color=color)
    plt.text(iPred[-1], yhat[-1], plot_label, ha='left', va='center')

    return ax
