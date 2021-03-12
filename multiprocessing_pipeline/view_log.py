import argparse
import os
from os.path import join
import matplotlib.pyplot as plt
import datetime
from collections import defaultdict
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log_dirpath', required=True)
    parser.add_argument('--fps', action='store_true')
    args = parser.parse_args()
    return args


def main():
    data_s = 'QueueData.index='
    msg_msg2c = dict({
        'queue_wait': 'y',
        'tmp': 'k',
        'input_queue.get': 'r',
        'output_queue.put': 'g'
    })
    msg_msg2c_order = ['y', 'k', 'r', 'g']
    c2ddy = {
        'y': -0.375, 
        'r': -0.125, 
        'g': 0.125, 
        'k': 0.375
    }
    assert set(msg_msg2c_order) <= set(msg_msg2c.values())
    subblock_type2c = dict({
        'assembler': 'r',
        'processor': 'b',
        'dissembler': 'g'
    })

    blocks = defaultdict(list)

    args = parse_args()
    logs_dirpath = args.log_dirpath
    fy = 2
    dy = 0.2
    ddy = 0.3

    min_time = None
    max_time = None
    max_block_i = 0
    for fn in os.listdir(logs_dirpath):
        with open(join(logs_dirpath, fn), 'r') as f:
            data = f.readlines()
        fn = os.path.splitext(fn)[0]
        ij, block_name, subblock_type = fn.split(' ')
        block_i, block_j = list(map(int, ij.split('_')))
        y_name = f''
        max_block_i = max(max_block_i, block_i)
        blocks[block_name].append(subblock_type)
        for line in data:
            line = line.strip()
            if len(line) == 0:
                continue
            line = line.split(' -- ')
            try:
                date, log_level, msg = line
            except:
                continue
            if log_level != 'INFO':
                continue
            date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
            if min_time is None or date < min_time:
                min_time = date
            if max_time is None or date > max_time:
                max_time = date

    if args.fps:
        from prettytable import PrettyTable
        pt = PrettyTable(['name', 'mean ms', 'mean fps', 'median ms', 'median fps'])
        for fn in sorted(os.listdir(logs_dirpath)):
            with open(join(logs_dirpath, fn), 'r') as f:
                data = f.readlines()
            fn = os.path.splitext(fn)[0]
            ij, block_name, subblock_type = fn.split(' ')
            block_i, block_j = list(map(int, ij.split('_')))
            y = fy * (block_i + dy * block_j)
            if subblock_type == 'processor':
                times = []
                times_prev = []
                prev = None
                start = None
                for line in data:
                    line = line.strip()
                    line = line.split(' -- ')
                    try:
                        date, log_level, msg = line
                    except:
                        continue
                    if log_level != 'INFO':
                        continue
                    date, log_level, msg = line
                    date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
                    date = (date - min_time).total_seconds()

                    try:
                        msg_msg, msg_value = msg.split(' ')
                    except:
                        continue

                    if msg_msg in msg_msg2c:
                        c = msg_msg2c[msg_msg]
                        if c == 'r':
                            start = date
                        if c == 'g':
                            if prev is not None:
                                times_prev.append(date - prev)
                            prev = date
                            if start is not None:
                                times.append(date - start)
                                start = None

                for cur_times, timing_name in zip((times, times_prev), ['start2output', 'output2output']):
                    cur_times = cur_times[-200:]
                    if len(cur_times) > 10:
                        cur_times = cur_times[10:]
                        pt.add_row([
                            f'{block_i} {block_name} {subblock_type} {timing_name}',
                            f'{np.mean(cur_times) * 1000:.2f}',
                            f'{1 / np.mean(cur_times):.2f}',
                            f'{np.median(cur_times) * 1000:.2f}',
                            f'{1 / np.median(cur_times):.2f}'
                        ])
        print(pt)
        return

    duration = (max_time - min_time).total_seconds()
    fig, ax = plt.subplots(1, 1, figsize=(duration, fy * max_block_i))
    yticks = []
    ytick_names = []
    for fn in os.listdir(logs_dirpath):
        with open(join(logs_dirpath, fn), 'r') as f:
            data = f.readlines()
        fn = os.path.splitext(fn)[0]
        ij, block_name, subblock_type = fn.split(' ')
        block_i, block_j = list(map(int, ij.split('_')))
        y = fy * (block_i + dy * block_j)
        yticks.append(y)
        ytick_names.append(f'{block_name} {subblock_type}')
        cur_min_time = None
        xs = []
        cs = []
        ys = []
        for line in data:
            line = line.strip()
            line = line.split(' -- ')
            try:
                date, log_level, msg = line
            except:
                continue
            if log_level != 'INFO':
                continue
            date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
            date = (date - min_time).total_seconds()
            if cur_min_time is None:
                cur_min_time = date
            cur_max_time = date
            try:
                msg_msg, msg_value = msg.split(' ')
            except:
                continue
            if msg_value.startswith(data_s) or msg_value == 'None':
                if msg_msg in msg_msg2c:
                    xs.append(date)
                    cs.append(msg_msg2c[msg_msg])
                    y_text = None
                    ys.append(y + fy * dy * ddy * c2ddy[msg_msg2c[msg_msg]])
                    if msg_msg2c[msg_msg] == 'g':
                        if (subblock_type == 'assembler') or \
                                (subblock_type == 'processor' and 'assembler' not in blocks[block_name]):
                            y_text = y - 0.5 * fy * dy
                        if (subblock_type == 'processor' and 'dissembler' not in blocks[block_name]) or \
                                (subblock_type == 'dissembler'):
                            y_text = y + 0.5 * fy * dy
                    if y_text is not None:
                        msg_index = int(msg_value[len(data_s):])
                        # print(msg_index, block_name, subblock_type)
                        ax.text(
                            date, y_text, str(msg_index), 
                            fontsize=8, 
                            verticalalignment='center', 
                            horizontalalignment='center',
                            rotation=90
                        )
        ax.plot([cur_min_time, cur_max_time], [y, y], f'{subblock_type2c[subblock_type]}--')
        for c in msg_msg2c_order:
            sub_xs = []
            sub_ys = []
            for i in range(len(cs)):
                if cs[i] == c:
                    sub_xs.append(xs[i])
                    sub_ys.append(ys[i])
            ax.scatter(sub_xs, sub_ys, c=c, s=20)
    # return
    ax.yaxis.set_ticks(yticks)
    ax.set_yticklabels(ytick_names)
    plt.show()


if __name__ == "__main__":
    main()
