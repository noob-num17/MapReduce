"""
Master 节点 (JobTracker)
负责：接收 Worker 注册、接受作业提交、调度 Map/Reduce 任务、Shuffle、汇总结果
"""

import os
import uuid
import pickle
import base64
import threading
import time
from typing import List, Dict, Any, Tuple, Optional

from flask import Flask, request, jsonify

from .protocol import (
    MASTER_REGISTER, MASTER_SUBMIT_JOB, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    MASTER_JOB_STATUS,
    WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_INPUT_PATH, FIELD_OUTPUT_PATH, FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_SHARD, FIELD_REDUCE_TASK, FIELD_REDUCE_RESULT,
    FIELD_STATUS, FIELD_ERROR,
    STATUS_PENDING, STATUS_MAP_RUNNING, STATUS_SHUFFLING,
    STATUS_REDUCE_RUNNING, STATUS_COMPLETED, STATUS_FAILED,
    OUTPUT_DELIMITER, OUTPUT_LINE_END,
)
from .network import post_json, make_url


class Master:
    """Master 节点，提供 HTTP 服务，负责调度整个 MapReduce 作业"""

    def __init__(self, port: int = 5000):
        self.port = port
        self.workers: List[Dict[str, Any]] = []  # 已注册的 Worker 列表
        self.jobs: Dict[str, Dict[str, Any]] = {}  # job_id -> 作业状态
        self._lock = threading.Lock()

    # ================================================================
    # 公开方法：启动服务
    # ================================================================

    def run(self):
        """启动 Master HTTP 服务"""
        app = self._create_app()
        print(f"[Master] 启动于 http://0.0.0.0:{self.port}")
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    # ================================================================
    # Flask 路由注册
    # ================================================================

    def _create_app(self):
        from flask import Flask
        app = Flask(__name__)

        @app.route(MASTER_REGISTER, methods=["POST"])
        def register():
            return self._handle_register(request)

        @app.route(MASTER_SUBMIT_JOB, methods=["POST"])
        def submit_job():
            return self._handle_submit_job(request)

        @app.route(MASTER_MAP_DONE, methods=["POST"])
        def map_done():
            return self._handle_map_done(request)

        @app.route(MASTER_REDUCE_DONE, methods=["POST"])
        def reduce_done():
            return self._handle_reduce_done(request)

        @app.route(MASTER_JOB_STATUS + "/<job_id>", methods=["GET"])
        def job_status(job_id):
            return self._handle_job_status(job_id)

        return app

    # ================================================================
    # 请求处理
    # ================================================================

    def _handle_register(self, req):
        """Worker 注册"""
        data = req.get_json()
        worker_port = data.get(FIELD_WORKER_PORT)

        if worker_port is None:
            return jsonify({FIELD_ERROR: "缺少 worker_port"}), 400

        worker_id = str(uuid.uuid4())[:8]
        worker_info = {
            FIELD_WORKER_ID: worker_id,
            # 使用请求来源 IP
            "host": req.remote_addr,
            "port": worker_port,
        }

        with self._lock:
            # 避免重复注册
            for w in self.workers:
                if w["host"] == worker_info["host"] and w["port"] == worker_info["port"]:
                    worker_id = w[FIELD_WORKER_ID]
                    break
            else:
                self.workers.append(worker_info)

        print(f"[Master] Worker 注册成功: {worker_info['host']}:{worker_port} (id={worker_id})")
        return jsonify({FIELD_WORKER_ID: worker_id, "workers_count": len(self.workers)})

    def _handle_submit_job(self, req):
        """Client 提交作业"""
        data = req.get_json()

        input_path = data.get(FIELD_INPUT_PATH)
        output_path = data.get(FIELD_OUTPUT_PATH)
        mapper_pkl_b64 = data.get(FIELD_MAPPER_PKL)
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL)

        if not all([input_path, output_path, mapper_pkl_b64, reducer_pkl_b64]):
            return jsonify({FIELD_ERROR: "缺少必要参数: input_path, output_path, mapper_pkl, reducer_pkl"}), 400

        job_id = str(uuid.uuid4())[:8]

        job = {
            FIELD_JOB_ID: job_id,
            FIELD_STATUS: STATUS_PENDING,
            FIELD_INPUT_PATH: input_path,
            FIELD_OUTPUT_PATH: output_path,
            FIELD_MAPPER_PKL: mapper_pkl_b64,
            FIELD_REDUCER_PKL: reducer_pkl_b64,
            "map_shards": [],         # 收集到的 map 分片结果
            "map_done_count": 0,      # 已完成 map 的 worker 数量
            "reduce_results": [],     # 收集到的 reduce 结果
            "reduce_done_count": 0,   # 已完成 reduce 的 worker 数量
            FIELD_ERROR: None,
        }

        with self._lock:
            self.jobs[job_id] = job

        # 在后台线程中执行作业
        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()

        print(f"[Master] 作业已提交: {job_id}，输入={input_path}，输出={output_path}")
        return jsonify({FIELD_JOB_ID: job_id, FIELD_STATUS: STATUS_PENDING})

    def _handle_map_done(self, req):
        """Worker 回传 map 结果"""
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        shard = data.get(FIELD_SHARD)
        worker_id = data.get(FIELD_WORKER_ID, "")

        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404

            job["map_shards"].extend(shard)
            job["map_done_count"] += 1

        print(f"[Master] 收到 map 结果: job={job_id}, worker={worker_id}, "
              f"pairs={len(shard)}, done={job['map_done_count']}/{len(self.workers)}")
        return jsonify({"ok": True})

    def _handle_reduce_done(self, req):
        """Worker 回传 reduce 结果"""
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        result = data.get(FIELD_REDUCE_RESULT)
        worker_id = data.get(FIELD_WORKER_ID, "")

        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404

            job["reduce_results"].extend(result)
            job["reduce_done_count"] += 1

        print(f"[Master] 收到 reduce 结果: job={job_id}, worker={worker_id}, "
              f"pairs={len(result)}, done={job['reduce_done_count']}/{len(self.workers)}")
        return jsonify({"ok": True})

    def _handle_job_status(self, job_id: str):
        """查询作业状态"""
        with self._lock:
            job = self.jobs.get(job_id)

        if job is None:
            return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404

        return jsonify({
            FIELD_JOB_ID: job[FIELD_JOB_ID],
            FIELD_STATUS: job[FIELD_STATUS],
            FIELD_ERROR: job.get(FIELD_ERROR),
        })

    # ================================================================
    # 作业执行流程（后台线程）
    # ================================================================

    def _run_job(self, job_id: str):
        """在后台线程中运行完整的 map -> shuffle -> reduce 流程"""
        try:
            with self._lock:
                job = self.jobs[job_id]
                workers_snapshot = list(self.workers)

            if not workers_snapshot:
                self._fail_job(job_id, "没有可用的 Worker")
                return

            # ---- 1. 读取输入文件 ----
            input_path = job[FIELD_INPUT_PATH]
            mapper_pkl_b64 = job[FIELD_MAPPER_PKL]
            reducer_pkl_b64 = job[FIELD_REDUCER_PKL]
            output_path = job[FIELD_OUTPUT_PATH]

            print(f"[Master] 作业 {job_id}: 开始读取输入文件 {input_path}")
            lines = self._read_input(input_path)
            if lines is None:
                self._fail_job(job_id, f"无法读取输入文件: {input_path}")
                return

            print(f"[Master] 作业 {job_id}: 共 {len(lines)} 行，分配给 {len(workers_snapshot)} 个 Worker")

            # ---- 2. Map 阶段 ----
            self._set_status(job_id, STATUS_MAP_RUNNING)
            map_shards = self._dispatch_map(lines, workers_snapshot,
                                            mapper_pkl_b64, job_id)
            if map_shards is None:
                return  # 失败已在 _dispatch_map 中处理

            print(f"[Master] 作业 {job_id}: Map 阶段完成，共 {len(map_shards)} 个 key-value 对")

            # ---- 3. Shuffle 阶段 ----
            self._set_status(job_id, STATUS_SHUFFLING)
            reduce_tasks = self._shuffle(map_shards, len(workers_snapshot))
            print(f"[Master] 作业 {job_id}: Shuffle 阶段完成，分成 {len(reduce_tasks)} 个 reduce 任务")

            # ---- 4. Reduce 阶段 ----
            self._set_status(job_id, STATUS_REDUCE_RUNNING)
            reduce_results = self._dispatch_reduce(reduce_tasks, workers_snapshot,
                                                   reducer_pkl_b64, job_id)
            if reduce_results is None:
                return  # 失败已在 _dispatch_reduce 中处理

            print(f"[Master] 作业 {job_id}: Reduce 阶段完成，共 {len(reduce_results)} 个结果")

            # ---- 5. 写入输出 ----
            self._write_output(output_path, reduce_results)
            print(f"[Master] 作业 {job_id}: 结果已写入 {output_path}")

            self._set_status(job_id, STATUS_COMPLETED)
            print(f"[Master] 作业 {job_id}: 完成")

        except Exception as e:
            self._fail_job(job_id, f"作业执行异常: {str(e)}")

    # ================================================================
    # 辅助方法
    # ================================================================

    def _read_input(self, path: str) -> Optional[List[str]]:
        """读取输入文件"""
        try:
            with open(path, "r") as f:
                lines = [line.rstrip("\n").rstrip("\r") for line in f]
            return lines
        except Exception as e:
            print(f"[Master] 读取输入文件失败: {e}")
            return None

    def _dispatch_map(self, lines: List[str], workers: List[Dict],
                      mapper_pkl_b64: str, job_id: str) -> Optional[List[List]]:
        """
        分配 map 任务给各 Worker，等待所有 Worker 返回结果
        返回所有 map 结果的合并列表 [[key, value], ...]，失败返回 None
        """
        n = len(workers)
        # 将输入行均分为 n 份
        chunks = self._split_list(lines, n)

        # 重置 map 收集状态
        with self._lock:
            job = self.jobs[job_id]
            job["map_shards"] = []
            job["map_done_count"] = 0

        # 向每个 Worker 发送 map 任务，使用线程并行发送
        threads = []
        send_errors = []

        def send_map(worker, chunk, worker_idx):
            try:
                url = make_url(worker["host"], worker["port"], WORKER_EXECUTE_MAP)
                resp = post_json(url, {
                    FIELD_JOB_ID: job_id,
                    FIELD_WORKER_ID: worker.get(FIELD_WORKER_ID, ""),
                    FIELD_MAPPER_PKL: mapper_pkl_b64,
                    FIELD_LINES: chunk,
                })
                if not resp.get("ok"):
                    send_errors.append(
                        f"Worker {worker['host']}:{worker['port']} map 失败: {resp.get(FIELD_ERROR, '未知错误')}")
            except Exception as e:
                send_errors.append(f"Worker {worker['host']}:{worker['port']} 通信失败: {e}")

        for i, worker in enumerate(workers):
            chunk = chunks[i] if i < len(chunks) else []
            if not chunk:
                # 空分片，直接标记完成（Worker 侧会回传空）
                continue
            t = threading.Thread(target=send_map, args=(worker, chunk, i))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if send_errors:
            error_msg = "; ".join(send_errors)
            self._fail_job(job_id, f"Map 阶段失败: {error_msg}")
            return None

        # 等待所有 Worker 完成 map（通过轮询 done_count）
        expected = sum(1 for i, chunk in enumerate(chunks) if i < len(chunks) and chunk)
        self._wait_for_count(job_id, "map_done_count", expected, timeout=120)

        with self._lock:
            job = self.jobs[job_id]
            if job[FIELD_STATUS] == STATUS_FAILED:
                return None
            if job["map_done_count"] < expected:
                self._fail_job(job_id, f"Map 阶段超时: 仅收到 {job['map_done_count']}/{expected} 个 Worker 的结果")
                return None
            return list(job["map_shards"])

    def _shuffle(self, shards: List[List], num_workers: int) -> List[List]:
        """
        Shuffle: 按 key 分组并排序
        输入: [[key, value], [key, value], ...]
        输出: [[[key, [v1, v2, ...]], [key, [v1, ...]], ...], ...]  分成 num_workers 个任务
        """
        # 按 key 分组
        grouped: Dict[Any, List] = {}
        for pair in shards:
            key, value = pair[0], pair[1]
            grouped.setdefault(key, []).append(value)

        # 按 key 排序
        sorted_keys = sorted(grouped.keys(), key=lambda k: str(k))
        sorted_pairs = [[k, grouped[k]] for k in sorted_keys]

        # 均分为 num_workers 份
        return self._split_list(sorted_pairs, num_workers)

    def _dispatch_reduce(self, reduce_tasks: List[List], workers: List[Dict],
                         reducer_pkl_b64: str, job_id: str) -> Optional[List[List]]:
        """
        分配 reduce 任务给各 Worker，等待所有 Worker 返回结果
        返回所有 reduce 结果的合并列表 [[key, value], ...]，失败返回 None
        """
        n = len(workers)
        # reduce_tasks 已经按 n 均分
        chunks = reduce_tasks  # 已分好，每组是一个 list of [key, [values...]]

        # 重置 reduce 收集状态
        with self._lock:
            job = self.jobs[job_id]
            job["reduce_results"] = []
            job["reduce_done_count"] = 0

        threads = []
        send_errors = []

        def send_reduce(worker, chunk, worker_idx):
            try:
                url = make_url(worker["host"], worker["port"], WORKER_EXECUTE_REDUCE)
                resp = post_json(url, {
                    FIELD_JOB_ID: job_id,
                    FIELD_WORKER_ID: worker.get(FIELD_WORKER_ID, ""),
                    FIELD_REDUCER_PKL: reducer_pkl_b64,
                    FIELD_REDUCE_TASK: chunk,
                })
                if not resp.get("ok"):
                    send_errors.append(
                        f"Worker {worker['host']}:{worker['port']} reduce 失败: {resp.get(FIELD_ERROR, '未知错误')}")
            except Exception as e:
                send_errors.append(f"Worker {worker['host']}:{worker['port']} 通信失败: {e}")

        for i, worker in enumerate(workers):
            chunk = chunks[i] if i < len(chunks) else []
            if not chunk:
                continue
            t = threading.Thread(target=send_reduce, args=(worker, chunk, i))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if send_errors:
            error_msg = "; ".join(send_errors)
            self._fail_job(job_id, f"Reduce 阶段失败: {error_msg}")
            return None

        # 等待所有 Worker 完成 reduce
        expected = sum(1 for c in chunks if c)
        self._wait_for_count(job_id, "reduce_done_count", expected, timeout=120)

        with self._lock:
            job = self.jobs[job_id]
            if job[FIELD_STATUS] == STATUS_FAILED:
                return None
            if job["reduce_done_count"] < expected:
                self._fail_job(job_id, f"Reduce 阶段超时: 仅收到 {job['reduce_done_count']}/{expected} 个 Worker 的结果")
                return None
            return list(job["reduce_results"])

    def _write_output(self, path: str, results: List[List]):
        """将最终结果写入输出文件"""
        with open(path, "w") as f:
            for pair in results:
                f.write(str(pair[0]))
                f.write(OUTPUT_DELIMITER)
                f.write(str(pair[1]))
                f.write(OUTPUT_LINE_END)

    def _wait_for_count(self, job_id: str, field: str, expected: int, timeout: float):
        """轮询等待 job[field] >= expected"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                job = self.jobs.get(job_id)
                if job is None or job[FIELD_STATUS] == STATUS_FAILED:
                    return
                if job.get(field, 0) >= expected:
                    return
            time.sleep(0.5)

    def _set_status(self, job_id: str, status: str):
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id][FIELD_STATUS] = status

    def _fail_job(self, job_id: str, error: str):
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id][FIELD_STATUS] = STATUS_FAILED
                self.jobs[job_id][FIELD_ERROR] = error
        print(f"[Master] 作业 {job_id}: 失败 - {error}")

    @staticmethod
    def _split_list(lst: List, n: int) -> List[List]:
        """将列表均分为 n 份"""
        if n <= 0:
            return [lst]
        length = len(lst)
        chunk_size = max(1, length // n)
        remainder = length % n
        result = []
        idx = 0
        for i in range(n):
            extra = 1 if i < remainder else 0
            end = idx + chunk_size + extra
            result.append(lst[idx:min(end, length)])
            idx = end
        return [r for r in result if r]  # 过滤空列表


# ================================================================
# 入口
# ================================================================

def run_master(port: int = 5000):
    master = Master(port=port)
    master.run()