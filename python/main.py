"""
MapReduce 统一入口
用法:
    # 单机版
    python main.py standalone <input> <output> <mapper_pkl> <reducer_pkl>

    # 分布式 - Master
    python main.py master [--port 5000]

    # 分布式 - Worker
    python main.py worker --master localhost:5000 --port 5001

    # 分布式 - Client
    python main.py client --master localhost:5000 --input ../input.txt --output ../output.txt
"""

import argparse
import os
import sys


def _cmd_standalone(args):
    """单机版 MapReduce（原 main.py 逻辑）"""
    import pickle
    from typing import List, Tuple, Any, Dict

    from mapper import Mapper
    from reducer import Reducer

    input_path = args.input
    output_path = args.output
    mapper_path = args.mapper_pkl
    reducer_path = args.reducer_pkl

    mapper: Mapper
    with open(mapper_path, 'rb') as f:
        mapper = pickle.load(f)()
    reducer: Reducer
    with open(reducer_path, 'rb') as f:
        reducer = pickle.load(f)()

    # input
    lines = []
    with open(input_path, 'r') as f:
        for line in f:
            lines.append(line[:-len(os.linesep)])

    # map
    pairs: List[Tuple[Any, Any]] = []
    for line in lines:
        pairs.extend(mapper.map(line))

    # shuffle
    intermediate: Dict[Any, list] = {}
    for pair in pairs:
        values = intermediate.get(pair[0])
        if values is None:
            intermediate[pair[0]] = [pair[1]]
        else:
            values.append(pair[1])
    intermediate_list: List[Tuple[Any, list]] = list(intermediate.items())
    intermediate_list.sort(key=lambda x: str(x[0]))

    # reduce
    result: List[Tuple[Any, Any]] = []
    for pair in intermediate_list:
        result.append(reducer.reduce(pair[0], pair[1]))

    # output
    with open(output_path, 'w') as f:
        for pair in result:
            f.write(str(pair[0]))
            f.write('\t')
            f.write(str(pair[1]))
            f.write(os.linesep)

    print(f"[Standalone] 完成，结果写入 {output_path}")


def _cmd_master(args):
    """启动 Master 节点"""
    from distributed.master import run_master
    port = args.port or 5000
    run_master(port=port)


def _cmd_worker(args):
    """启动 Worker 节点"""
    from distributed.worker import run_worker
    master = args.master
    if ':' in master:
        host, port_str = master.split(':', 1)
        master_host = host
        master_port = int(port_str)
    else:
        master_host = master
        master_port = 5000

    worker_port = args.port or 5001
    run_worker(master_host=master_host, master_port=master_port, port=worker_port)


def _cmd_client(args):
    """启动 Client，提交作业"""
    from distributed.client import run_client
    master = args.master
    if ':' in master:
        host, port_str = master.split(':', 1)
        master_host = host
        master_port = int(port_str)
    else:
        master_host = master
        master_port = 5000

    run_client(
        master_host=master_host,
        master_port=master_port,
        input_path=args.input,
        output_path=args.output,
    )


def main():
    parser = argparse.ArgumentParser(
        description="MapReduce 框架 - 支持单机版和分布式版"
    )
    subparsers = parser.add_subparsers(dest="mode", help="运行模式")

    # ---- standalone ----
    p_standalone = subparsers.add_parser("standalone", help="单机版 MapReduce")
    p_standalone.add_argument("input", help="输入文件路径")
    p_standalone.add_argument("output", help="输出文件路径")
    p_standalone.add_argument("mapper_pkl", help="mapper pickle 文件路径")
    p_standalone.add_argument("reducer_pkl", help="reducer pickle 文件路径")
    p_standalone.set_defaults(func=_cmd_standalone)

    # ---- master ----
    p_master = subparsers.add_parser("master", help="启动 Master 节点 (JobTracker)")
    p_master.add_argument("--port", type=int, default=5000, help="Master 监听端口（默认 5000）")
    p_master.set_defaults(func=_cmd_master)

    # ---- worker ----
    p_worker = subparsers.add_parser("worker", help="启动 Worker 节点 (TaskTracker)")
    p_worker.add_argument("--master", required=True, help="Master 地址，如 localhost:5000")
    p_worker.add_argument("--port", type=int, default=5001, help="Worker 监听端口（默认 5001）")
    p_worker.set_defaults(func=_cmd_worker)

    # ---- client ----
    p_client = subparsers.add_parser("client", help="提交 MapReduce 作业")
    p_client.add_argument("--master", required=True, help="Master 地址，如 localhost:5000")
    p_client.add_argument("--input", required=True, help="输入文件路径")
    p_client.add_argument("--output", required=True, help="输出文件路径")
    p_client.set_defaults(func=_cmd_client)

    args = parser.parse_args()
    if not args.mode:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()