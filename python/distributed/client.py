"""
Client 节点
负责：向 Master 提交 MapReduce 作业、等待作业完成
"""

import base64
import os
import pickle
import sys
import time
from typing import Optional

from .protocol import (
    MASTER_SUBMIT_JOB, MASTER_JOB_STATUS,
    FIELD_INPUT_PATH, FIELD_OUTPUT_PATH,
    FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_JOB_ID, FIELD_STATUS, FIELD_ERROR,
    STATUS_COMPLETED, STATUS_FAILED,
)
from .network import post_json, get_json, make_url


class Client:
    """MapReduce 作业提交客户端"""

    def __init__(self, master_host: str, master_port: int):
        self.master_host = master_host
        self.master_port = master_port

    def submit(self, input_path: str, output_path: str,
               mapper_path: str, reducer_path: str,
               wait: bool = True) -> Optional[str]:
        """
        提交一个 MapReduce 作业
        返回 job_id，如果 wait=True 则阻塞等待作业完成
        """
        # 将 UDF pickle 文件读取并编码为 base64
        print(f"[Client] 读取 mapper: {mapper_path}")
        with open(mapper_path, "rb") as f:
            mapper_pkl_b64 = base64.b64encode(f.read()).decode("utf-8")

        print(f"[Client] 读取 reducer: {reducer_path}")
        with open(reducer_path, "rb") as f:
            reducer_pkl_b64 = base64.b64encode(f.read()).decode("utf-8")

        # 提交作业
        url = make_url(self.master_host, self.master_port, MASTER_SUBMIT_JOB)
        print(f"[Client] 提交作业到 {self.master_host}:{self.master_port}")
        print(f"[Client] 输入: {input_path}，输出: {output_path}")

        resp = post_json(url, {
            FIELD_INPUT_PATH: input_path,
            FIELD_OUTPUT_PATH: output_path,
            FIELD_MAPPER_PKL: mapper_pkl_b64,
            FIELD_REDUCER_PKL: reducer_pkl_b64,
        })

        job_id = resp.get(FIELD_JOB_ID)
        if not job_id:
            print(f"[Client] 提交失败: {resp}")
            return None

        print(f"[Client] 作业已提交，job_id={job_id}")

        if wait:
            self._wait_for_completion(job_id)

        return job_id

    def check_status(self, job_id: str) -> dict:
        """查询作业状态"""
        url = make_url(self.master_host, self.master_port, MASTER_JOB_STATUS + "/" + job_id)
        return get_json(url)

    def _wait_for_completion(self, job_id: str):
        """轮询等待作业完成"""
        print(f"[Client] 等待作业 {job_id} 完成...")
        dots = 0
        while True:
            try:
                status = self.check_status(job_id)
                s = status.get(FIELD_STATUS, "unknown")

                if s == STATUS_COMPLETED:
                    print(f"\n[Client] 作业 {job_id} 完成！")
                    return
                elif s == STATUS_FAILED:
                    print(f"\n[Client] 作业 {job_id} 失败: {status.get(FIELD_ERROR, '未知错误')}")
                    return
                else:
                    # 进度指示
                    dots = (dots + 1) % 4
                    print(f"\r[Client] 作业状态: {s}{'.' * dots}   ", end="", flush=True)

            except Exception as e:
                print(f"\n[Client] 查询状态失败: {e}")

            time.sleep(1)


# ================================================================
# 入口
# ================================================================

def run_client(master_host: str, master_port: int,
               input_path: str, output_path: str):
    """以客户端模式运行：先 pickle UDF，然后提交作业"""
    # 使用 save.py 的逻辑：序列化 wordcount 的 UDF
    from wordcount_mapper import WordCountMapper
    from wordcount_reducer import WordCountReducer

    mapper_pkl_path = "./mapper.pkl"
    reducer_pkl_path = "./reducer.pkl"

    print("[Client] 序列化 UDF...")
    with open(mapper_pkl_path, "wb") as f:
        pickle.dump(WordCountMapper, file=f)
    with open(reducer_pkl_path, "wb") as f:
        pickle.dump(WordCountReducer, file=f)

    client = Client(master_host=master_host, master_port=master_port)
    job_id = client.submit(
        input_path=input_path,
        output_path=output_path,
        mapper_path=mapper_pkl_path,
        reducer_path=reducer_pkl_path,
        wait=True,
    )

    if job_id:
        print(f"[Client] 结果已写入 {output_path}")
        # 显示输出内容
        try:
            with open(output_path, "r") as f:
                print("[Client] 输出内容:")
                for line in f:
                    print(f"  {line.rstrip()}")
        except Exception as e:
            print(f"[Client] 无法读取输出: {e}")
    else:
        print("[Client] 作业提交失败")
        sys.exit(1)