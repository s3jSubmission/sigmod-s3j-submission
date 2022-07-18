import itertools
import json
import os
import random
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from config import parseDrawArguments

percent_keys = ["50%", "90%", "95%", "99%", "99.9%"]
machines = []

filenames = [
    "final-comps-per-machine",
    "final-comps-per-group",
    "size-per-group",
    "latency-percentiles-per-machine"]


def draw_computations_all_machines(df: pd.DataFrame, filename: str):
    pl = df.plot("Windows", df.columns[:-1], ylabel="Number of computations", marker="o",
                 title="Final Computations")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+"comps_per_machine.png", bbox_inches="tight")
    plt.close()


def draw_all_percentiles_per_machine(df: pd.DataFrame, machine: str, filename: str):
    new_df = df.loc[:, [machine, "Windows"]]
    new_df.loc[:, (machine, df.columns.get_level_values(1).drop_duplicates()[:-1])] =\
        new_df.loc[:, (machine, df.columns.get_level_values(1).drop_duplicates()[:-1])].div(1000)
    new_df.columns = new_df.columns.rename('Machine', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)", marker="o")
    plt.legend(bbox_to_anchor=(1.01, 1.0))
    plt.savefig(filename+'machine_%s-all_percentiles.png' % machine, bbox_inches="tight")
    plt.close()


def draw_percentiles_line_chart(df: pd.DataFrame, perc: str, filename: str):
    new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)] =\
        new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)].div(1000)
    new_df.columns = new_df.columns.rename('MachineID', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    print(new_df.columns)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)", figsize=(8, 2.3))

    avail_markers = ["o", "X", "s", "*", "D"]
    for i, line in enumerate(pl.get_lines()):
        line.set_marker(avail_markers[i])
    # for i, p in enumerate(new_df["0"][perc]):
    #     plt.text(i, p+20, "%.2f" %p, ha="center", color="magenta")
    new_df_title = "(MachineID, Percentile)"
    # plt.legend(title=new_df_title, loc='upper center',
    #              bbox_to_anchor=(0.5, -0.1), ncol=5)
    plt.savefig(filename+'%s-percentile.png' % perc, bbox_inches="tight")
    plt.close()


def draw_single(df: pd.DataFrame, perc: str, legend: bool, ax=None):
    new_df = df.loc[:, (df.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)] = \
        new_df.loc[:, (df.columns.get_level_values(0).drop_duplicates()[:-1], perc)].div(1000)
    new_df.columns = new_df.columns.rename('MachineID', level=0)
    new_df.columns = new_df.columns.rename('Percentile', level=1)
    print(new_df.columns)
    pl = new_df.plot("Windows", new_df.columns.get_level_values(0).drop_duplicates()[:-1],
                     ylabel="Latency(s)", ax=ax, legend=False, xticks=[0,10,20,30,40,50], yticks=[0,20,40,60,80])

    avail_markers = ["o", "X", "s", "*", "D", "o", "X", "s", "*", "D", "o", "X", "s", "*", "D", "o", "X", "s", "*", "D"]
    for i, line in enumerate(pl.get_lines()):
        line.set_marker(avail_markers[i])
    # for i, p in enumerate(new_df["0"][perc]):
    #     plt.text(i, p+20, "%.2f" %p, ha="center", color="magenta")
    new_df_title = "(MachineID, Percentile)"
    if legend:
        plt.legend(title=new_df_title, bbox_to_anchor=(1.01, 1.0))
    return pl


def draw_percentiles_line_chart_3in1(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, perc: str, filename: str):
    # new_df = df1.loc[:, (df1.columns.get_level_values(0).drop_duplicates(), ["", perc])]
    fig, axes = plt.subplots(nrows=2, ncols=1)
    fig.set_size_inches(7, 5)
    plt.subplots_adjust(hspace=0.6)
    pl = draw_single(df1, perc, False, axes[0])
    pl = draw_single(df2, perc, False, axes[1])
    # pl = draw_single(df3, perc, False, axes[2])
    # axes[1].legend(title="(MachineID, Percentile)",loc='upper center',
    #              bbox_to_anchor=(0.5, -0.2), fancybox=False, shadow=False, ncol=8)
    axes[0].title.set_text("Without load rebalancing")
    axes[1].title.set_text("With load rebalancing")
    # axes[2].title.set_text("parallelism = 15")

    fig.savefig(filename+'%s-percentile.png' % perc, bbox_inches="tight")
    plt.close()


def fill_nones(js: dict, percentiles: list):
    for t in js.keys():
        for m in js[t].keys():
            if js[t][m] is None:
                js[t][m] = dict.fromkeys(percentiles, 0)


def percentiles_dataframe(js: dict):
    global percent_keys, machines
    timestamps = []
    percentiles = []
    fill_nones(js, percent_keys)
    for _ in range(len(percent_keys)*len(machines)):
        percentiles.append([])
    for timestamp in js.keys():
        timestamps.append(timestamp)
    for t in timestamps:
        for m_i, m in enumerate(machines):
            for p_i, p in enumerate(percent_keys):
                percentiles[len(percent_keys)*m_i + p_i].append(js[t][m][p])
    np_percentiles = np.array(percentiles)
    df = pd.DataFrame(data=np_percentiles.T, columns=pd.MultiIndex.from_tuples(itertools.product(machines,percent_keys)))
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
    return df


def machine_computations_dataframe(js: dict):
    global machines
    timestamps = []
    computations = []
    for _ in range(len(machines)):
        computations.append([])
    for timestamp in js.keys():
        timestamps.append(timestamp)
    for t in timestamps:
        for m_i, m in enumerate(machines):
            computations[m_i].append(js[t][m])
    np_computations = np.array(computations)
    df = pd.DataFrame(data=np_computations.T, columns=machines)
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
    return df


def group_size_dataframe(js: dict):
    global machines
    timestamps = []
    groups = []
    sizes = []
    for t in js.keys():
        timestamps.append(t)
        for m in machines:
            for g in js[t][m].keys():
                groups.append((m,g))
    for m, g in groups:
        tmp = []
        for t in timestamps:
            if g in js[t][m].keys():
                tmp.append(js[t][m][g])
            else:
                tmp.append(None)
        sizes.append(tmp)
    np_sizes = np.array(sizes)
    df = pd.DataFrame(data=np_sizes.T, columns=pd.MultiIndex.from_tuples(groups))
    windows = [datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%H:%M:%S") for timestamp in timestamps]
    windows = [idx for idx, _ in enumerate(timestamps)]
    df["Windows"] = np.array(windows).T
    return df


def get_machine_ids(name_prefix):
    global machines
    with open(name_prefix + "machine_ids.txt", "r") as fh:
        lines = fh.readlines()
        lines = [line.rstrip() for line in lines]
        machines = lines
    print(machines)

if __name__ == '__main__':
    args = parseDrawArguments()
    experiment_names = args.name.split(",")
    print(len(experiment_names))
    if len(experiment_names) == 1:
        experiment_name = experiment_names[0]
        input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", experiment_name)
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", experiment_name)
        name_prefix = input_dir+"/stats_files/" + args.name + "__"
        png_prefix = output_dir +"/"+ args.name + "__"
        get_machine_ids(name_prefix)
        with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
            js = json.load(fh)
        percentiles_frame = percentiles_dataframe(js)
        new_df = percentiles_frame.loc[:, (percentiles_frame.columns.get_level_values(0).drop_duplicates(), ["", "90%"])]
        # print(new_df)
        draw_percentiles_line_chart(percentiles_frame, "99%", png_prefix)
        for machine in machines:
            draw_all_percentiles_per_machine(percentiles_frame, machine, png_prefix)
        with open(name_prefix + "final-comps-per-machine.txt", "r") as fh:
            m_comp_js = json.load(fh)
        computations_frame = machine_computations_dataframe(m_comp_js)
        # print(computations_frame)
        draw_computations_all_machines(computations_frame, png_prefix)

    else:
        input_dir_arr = []
        for exp in experiment_names:
            input_dir_arr.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments", exp))
        if not os.path.isdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)):
            os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type))
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_figures", "combined_"+args.type)
        dataframes = []
        for idx, exp in enumerate(experiment_names):
            name_prefix = input_dir_arr[idx]+"/stats_files/" + exp + "__"
            png_prefix = output_dir +"/"+ args.type + "__"
            get_machine_ids(name_prefix)
            with open(name_prefix + "latency-percentiles-per-machine.txt", "r") as fh:
                js = json.load(fh)
            percentiles_frame = percentiles_dataframe(js)
            dataframes.append(percentiles_frame)
        draw_percentiles_line_chart_3in1(dataframes[0], dataframes[1], dataframes[1], "99%", png_prefix)
