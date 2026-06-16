"""
网络通信工具模块
封装 HTTP 请求/响应处理、重试逻辑
"""

import requests
import json
from typing import Optional, Dict, Any
from .protocol import DEFAULT_TIMEOUT


def post_json(url: str, data: Optional[Dict[str, Any]] = None,
              timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    发送 POST 请求，body 为 JSON 格式
    返回解析后的 JSON 响应字典
    """
    try:
        resp = requests.post(url, json=data, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise NetworkError(f"无法连接到 {url}，请确认服务已启动")
    except requests.exceptions.Timeout:
        raise NetworkError(f"请求 {url} 超时（{timeout}秒）")
    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = {}
        raise NetworkError(
            f"请求 {url} 失败: HTTP {e.response.status_code}"
            + (f", {detail.get('error', '')}" if detail.get('error') else "")
        )
    except requests.exceptions.RequestException as e:
        raise NetworkError(f"请求 {url} 异常: {e}")


def get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    发送 GET 请求，返回解析后的 JSON 响应字典
    """
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise NetworkError(f"无法连接到 {url}，请确认服务已启动")
    except requests.exceptions.Timeout:
        raise NetworkError(f"请求 {url} 超时（{timeout}秒）")
    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = {}
        raise NetworkError(
            f"请求 {url} 失败: HTTP {e.response.status_code}"
            + (f", {detail.get('error', '')}" if detail.get('error') else "")
        )
    except requests.exceptions.RequestException as e:
        raise NetworkError(f"请求 {url} 异常: {e}")


def make_url(host: str, port: int, path: str) -> str:
    """构造完整 URL"""
    # 确保 path 以 / 开头
    if not path.startswith("/"):
        path = "/" + path
    return f"http://{host}:{port}{path}"


class NetworkError(Exception):
    """网络通信错误"""
    pass