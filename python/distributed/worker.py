"""
Worker 节点 (TaskTracker)
负责：注册到 Master、接收并执行 Map/Reduce 子任务、回传结果
"""

import base64
import pickle
import threading
import time
import sys
from typing import List, Any

from flask import Flask, request, jsonify

from .protocol import (
    WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE, WORKER_PING,
    MASTER_REGISTER, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_SHARD, FIELD_REDUCE_TASK, FIELD_REDUCE_RESULT,
    FIELD_ERROR,
)
from .network import post_json, make_url


class Worker:
    """Worker 节点，提供 HTTP 服务，执行 Map/Reduce 子任务"""

    def __init__(self, master_host: str, master_port: int, port: int = 5001):
        self.master_host = master_host
        self.master_port = master_port
        self.port = port
        self.worker_id: str = ""
        self._registered = False

    # ================================================================
    # 公开方法：启动服务
    # ================================================================

    def run(self):
        """启动 Worker，注册到 Master，然后开始监听任务"""
        app = self._create_app()

        # 在启动 Flask 之前先尝试注册
        # 由于 Flask 是阻塞的，我们在后台线程中注册
        def register_loop():
            time.sleep(0.5)  # 等待 Flask 完全启动
            self._register()

        threading.Thread(target=register_loop, daemon=True).start()

        print(f"[Worker] 启动于 http://0.0.0.0:{self.port}，Master: {self.master_host}:{self.master_port}")
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    # ================================================================
    # Flask 路由注册
    # ================================================================

    def _create_app(self):
        from flask import Flask
        app = Flask(__name__)

        @app.route(WORKER_EXECUTE_MAP, methods=["POST"])
        def execute_map():
            return self._handle_execute_map(request)

        @app.route(WORKER_EXECUTE_REDUCE, methods=["POST"])
        def execute_reduce():
            return self._handle_execute_reduce(request)

        @app.route(WORKER_PING, methods=["GET"])
        def ping():
            return jsonify({"status": "ok", FIELD_WORKER_ID: self.worker_id})

        return app

    # ================================================================
    # 注册到 Master
    # ================================================================

    def _register(self):
        """向 Master 注册自身"""
        max_retries = 30
        for i in range(max_retries):
            try:
                url = make_url(self.master_host, self.master_port, MASTER_REGISTER)
                resp = post_json(url, {FIELD_WORKER_PORT: self.port})
                self.worker_id = resp.get(FIELD_WORKER_ID, "")
                self._registered = True
                print(f"[Worker] 注册成功，worker_id={self.worker_id}")
                return
            except Exception as e:
                print(f"[Worker] 注册失败 ({i+1}/{max_retries}): {e}")
                time.sleep(1)

        print("[Worker] 无法注册到 Master，请确认 Master 已启动")
        sys.exit(1)

    # ================================================================
    # 任务处理
    # ================================================================

    def _handle_execute_map(self, req):
        """执行 map 子任务"""
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        mapper_pkl_b64 = data.get(FIELD_MAPPER_PKL, "")
        lines = data.get(FIELD_LINES, [])

        print(f"[Worker {self.worker_id}] 收到 map 任务: job={job_id}, lines={len(lines)}")

        try:
            # 反序列化 mapper UDF
            mapper_cls = pickle.loads(base64.b64decode(mapper_pkl_b64))
            mapper = mapper_cls()

            # 执行 map
            shard: List[List[Any]] = []
            for line in lines:
                pairs = mapper.map(line)
                for key, value in pairs:
                    shard.append([key, value])

            print(f"[Worker {self.worker_id}] map 完成: {len(shard)} 个 key-value 对")

            # 将结果回传给 Master
            self._send_map_done(job_id, shard)

            return jsonify({"ok": True, "pair_count": len(shard)})

        except Exception as e:
            error_msg = f"map 任务执行失败: {str(e)}"
            print(f"[Worker {self.worker_id}] {error_msg}")
            return jsonify({FIELD_ERROR: error_msg, "ok": False}), 500

    def _handle_execute_reduce(self, req):
        """执行 reduce 子任务"""
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL, "")
        reduce_task = data.get(FIELD_REDUCE_TASK, [])

        print(f"[Worker {self.worker_id}] 收到 reduce 任务: job={job_id}, keys={len(reduce_task)}")

        try:
            # 反序列化 reducer UDF
            reducer_cls = pickle.loads(base64.b64decode(reducer_pkl_b64))
            reducer = reducer_cls()

            # 执行 reduce
            result: List[List[Any]] = []
            for pair in reduce_task:
                key = pair[0]
                values = pair[1]
                r_key, r_value = reducer.reduce(key, values)
                result.append([r_key, r_value])

            print(f"[Worker {self.worker_id}] reduce 完成: {len(result)} 个结果")

            # 将结果回传给 Master
            self._send_reduce_done(job_id, result)

            return jsonify({"ok": True, "pair_count": len(result)})

        except Exception as e:
            error_msg = f"reduce 任务执行失败: {str(e)}"
            print(f"[Worker {self.worker_id}] {error_msg}")
            return jsonify({FIELD_ERROR: error_msg, "ok": False}), 500

    # ================================================================
    # 结果回传
    # ================================================================

    def _send_map_done(self, job_id: str, shard: List[List]):
        """将 map 中间结果回传给 Master"""
        url = make_url(self.master_host, self.master_port, MASTER_MAP_DONE)
        try:
            resp = post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                FIELD_SHARD: shard,
            })
            print(f"[Worker {self.worker_id}] map 结果已回传: {resp}")
        except Exception as e:
            print(f"[Worker {self.worker_id}] map 结果回传失败: {e}")

    def _send_reduce_done(self, job_id: str, result: List[List]):
        """将 reduce 结果回传给 Master"""
        url = make_url(self.master_host, self.master_port, MASTER_REDUCE_DONE)
        try:
            resp = post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                FIELD_REDUCE_RESULT: result,
            })
            print(f"[Worker {self.worker_id}] reduce 结果已回传: {resp}")
        except Exception as e:
            print(f"[Worker {self.worker_id}] reduce 结果回传失败: {e}")


# ================================================================
# 入口
# ================================================================

def run_worker(master_host: str, master_port: int, port: int):
    worker = Worker(master_host=master_host, master_port=master_port, port=port)
    worker.run()