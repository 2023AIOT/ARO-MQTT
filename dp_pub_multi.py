import argparse
import json
import time
import uuid
import threading
from pathlib import Path
from typing import Optional, Set

import paho.mqtt.client as mqtt


def load_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def make_client(client_id: str) -> mqtt.Client:
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
    except Exception:
        return mqtt.Client(client_id=client_id)


class MultiPairPublisher:
    """
    Publisher 根据 path_id 连接到对应 broker 的本地 MQTT，将冗余副本发往不同 broker。
    Adapter 负责在 overlay 网络中按树转发，最终把两份副本都传到 subscriber 节点。

    数据流（primary=E1, secondary=R1, sub=E1）：
      msg1 (path=E1): Publisher → E1:1888 本地 MQTT
                       → E1 adapter 本地订阅收到
                       → E1 是 sub，直接 publish_to_local_broker → subscriber 收到
      msg1 (path=R1): Publisher → R1:1885 本地 MQTT（走 overlay 路径）
                       → R1 adapter 本地订阅收到
                       → R1 overlay 发往 root 方向
                       → 某 DESIGNATED 端口收到，层层转发 → E1 adapter
                       → E1 是 sub，投本地 MQTT → subscriber 收到
    """

    def __init__(self, run_id: str, brokers_json: str, pairs_json: str, pair_id: str, control_host: str, control_port: int):
        self.run_id = run_id
        self.brokers = load_json(brokers_json)
        self.broker_by_id = {b['id']: b for b in self.brokers}
        all_pairs = load_json(pairs_json)
        self.pair = next(p for p in all_pairs if p['pair_id'] == pair_id)
        self.pair_id = pair_id
        self.topic_business = self.pair['topic']
        self.publisher_pool = list(self.pair['publisher_pool'])
        self.subscriber_pool = list(self.pair.get('subscriber_pool', []))
        self.control_host = control_host
        self.control_port = int(control_port)
        self.pub_id = str(uuid.uuid4())[:8]
        self.seq_no = 0
        self.schedules = {}
        self._lock = threading.Lock()
        self._last_wait_log = 0.0

        # 【修复】用 RLock（可重入锁）代替 Lock，避免同一线程在 _on_ctrl() 中嵌套调用
        # _ensure_client_for_path() 时造成死锁。RLock 允许同一线程多次获取。
        self._clients: dict[str, list] = {}  # path_id → [client, ready]
        self._client_lock = threading.RLock()
        # 记录每个 path_id 最后一次 publish 是否成功（用于日志）
        self._last_pub_ok: dict[str, bool] = {}

        # 控制平面连接
        self.ctrl = make_client(f'PUBCTRL_{run_id}_{pair_id}_{self.pub_id}')
        self.ctrl.on_connect = self._on_connect
        self.ctrl.on_message = self._on_ctrl
        self.ctrl.connect(self.control_host, self.control_port, 30)
        self.ctrl.loop_start()
        print(f'[PUB] started pair={self.pair_id} topic={self.topic_business} '
              f'pool={self.publisher_pool} sub={self.subscriber_pool[0]}')

    def topic_sel(self):
        return f'asr/{self.run_id}/{self.pair_id}/control/path_selection'

    def _ensure_client_for_path(self, path_id: str):
        """为指定 path_id 确保有连接到对应 broker MQTT 的客户端"""
        with self._client_lock:
            entry = self._clients.get(path_id)
            if entry is not None:
                return entry[1]  # entry[0]=client, entry[1]=ready
        # 不在锁内创建，以免阻塞其他线程
        broker = self.broker_by_id.get(path_id)
        if not broker:
            return False
        host = broker['host']
        port = int(broker['port'])
        client = make_client(f'PUB_{self.run_id}_{self.pair_id}_{self.pub_id}_{path_id}')
        # 用 list 而不是 tuple，这样 on_conn 回调能真正修改 ready 状态
        state = [client, False]  # state[0]=client, state[1]=ready

        def on_conn(c, ud, fl, rc):
            state[1] = (rc == 0)
            if rc == 0:
                print(f'[PUB] on_conn OK path={path_id}')
            else:
                print(f'[PUB] on_conn FAIL path={path_id} rc={rc}')
        def on_disconn(c, ud, rc):
            state[1] = False
        client.on_connect = on_conn
        client.on_disconnect = on_disconn
        client.connect(host, port, 30)
        client.loop_start()
        with self._client_lock:
            self._clients[path_id] = state
        print(f'[PUB] connecting to broker {path_id} at {host}:{port}')
        return True

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self.topic_sel(), qos=1)
            print(f'[PUB] subscribed path_selection pair={self.pair_id}')

    def _on_ctrl(self, client, userdata, msg):
        if not msg.payload:
            return
        s = json.loads(msg.payload.decode('utf-8'))
        ws = int(s['window_start'])
        primary = str(s['primary_id'])
        sec = list(s['secondary_schedule'])

        # 更新本地 publisher_pool（与 CP 下发一致，避免日志和实际错位）
        publisher_pool = list(s.get('publisher_pool', []))
        if publisher_pool:
            with self._client_lock:
                self.publisher_pool = publisher_pool

        # ---- 新增：自适应 m 字段 ----
        path_schedule = s.get('path_schedule')
        m_current = int(s.get('m_current', 0))
        priority_class = str(s.get('priority_class', 'normal'))
        target_reliability = float(s.get('target_reliability', 0.99))
        estimated_reliability = float(s.get('estimated_reliability', 0.0))

        print(
            f'[PUB-ADAPT-M] pair={self.pair_id} ws={ws} '
            f'priority={priority_class} m={m_current} '
            f'target_R={target_reliability} estimated_R={estimated_reliability} '
            f'first_slot_paths={path_schedule[0] if path_schedule else "legacy"}',
            flush=True
        )

        # 计算当前窗口需要的所有 path_id
        w = int(s['window_size'])
        if path_schedule:
            # 从 path_schedule 展开所有路径（每个 slot 的路径去重合并）
            window_paths: Set[str] = set()
            for slot_paths in path_schedule:
                for p in slot_paths:
                    window_paths.add(str(p))
        else:
            # 兼容旧算法：使用 primary + secondary_schedule
            window_paths = set([primary])
            for i in range(w):
                sec_path = str(sec[i]) if i < len(sec) else primary
                if sec_path != primary:
                    window_paths.add(sec_path)

        # 只在新 path 集合变化时才重建连接，否则复用
        with self._client_lock:
            existing_paths = set(self._clients.keys())
            new_paths = window_paths - existing_paths
            if new_paths:
                for p in sorted(new_paths):
                    self._ensure_client_for_path(p)
                print(f'[PUB] schedule pair={self.pair_id} ws={ws} primary={primary} '
                      f'new_paths={sorted(new_paths)} total_conns={len(self._clients)}')
            else:
                print(f'[PUB] schedule pair={self.pair_id} ws={ws} primary={primary} '
                      f'(reusing {len(self._clients)} connections)')

        with self._lock:
            self.schedules[ws] = {
                'ws': ws,
                'w': w,
                'primary': primary,
                'sec': sec,
                'path_schedule': path_schedule,
                'm_current': m_current,
                'priority_class': priority_class,
                'target_reliability': target_reliability,
                'estimated_reliability': estimated_reliability,
            }

    @staticmethod
    def _ws_for_seq(seq: int, w: int) -> int:
        return ((seq - 1) // w) * w + 1

    def _get_schedule(self, seq: int):
        with self._lock:
            if not self.schedules:
                return None
            latest_ws = max(self.schedules.keys())
            w = int(self.schedules[latest_ws]['w'])
            ws = self._ws_for_seq(seq, w)
            return self.schedules.get(ws)

    def send_one(self) -> bool:
        """发送一条消息。只在至少一个副本 publish 成功时才推进 seq_no。"""
        next_seq = self.seq_no + 1
        sched = self._get_schedule(next_seq)
        if not sched:
            now = time.time()
            if now - self._last_wait_log > 1.0:
                print(f'[PUB-WAIT] pair={self.pair_id} next_seq={next_seq} no schedule')
                self._last_wait_log = now
            return False
        ws, w = int(sched['ws']), int(sched['w'])
        idx = next_seq - ws
        if idx < 0 or idx >= w:
            return False
        # ---- 优先使用 path_schedule ----
        path_schedule = sched.get('path_schedule')
        if path_schedule and idx < len(path_schedule):
            paths_to_send = list(dict.fromkeys(str(x) for x in path_schedule[idx]))
        else:
            # 兼容旧算法：使用 primary + secondary_schedule
            primary = str(sched['primary'])
            secondary = str(sched['sec'][idx]) if idx < len(sched['sec']) else primary
            paths_to_send = [primary, secondary] if primary != secondary else [primary]

        m_current = int(sched.get('m_current', len(paths_to_send)))
        priority_class = str(sched.get('priority_class', 'normal'))

        if next_seq == 1 or next_seq % 100 == 0:
            print(
                f'[PUB-M-SEND] pair={self.pair_id} seq={next_seq} '
                f'm_actual={len(paths_to_send)} paths={paths_to_send} '
                f'priority={priority_class}',
                flush=True
            )

        payload = {
            'ts_send': time.time(),
            'run_id': self.run_id,
            'pair_id': self.pair_id,
            'topic': self.topic_business,
            'pub_id': self.pub_id,
            'seq_no': next_seq,
            'path_id': None,
            'primary_id': str(sched['primary']),
            'secondary_id': str(sched['sec'][idx]) if idx < len(sched['sec']) else str(sched['primary']),
            'm_current': m_current,
            'path_list': paths_to_send,
            'priority_class': priority_class,
            'data': {'value': time.time() % 1000}
        }

        # 逐 path 发送
        published_any = False
        for path_id in paths_to_send:
            # 确保连接存在
            self._ensure_client_for_path(path_id)
            with self._client_lock:
                entry = self._clients.get(path_id)
                if entry is None:
                    print(f'[PUB-SKIP] seq={next_seq} path={path_id} no_entry')
                    self._last_pub_ok[path_id] = False
                    continue
                client, ready = entry[0], entry[1]
            if not ready:
                print(f'[PUB-SKIP] seq={next_seq} path={path_id} ready=False')
                self._last_pub_ok[path_id] = False
                continue
            p = dict(payload)
            p['path_id'] = path_id
            topic = f'{self.topic_business}/rmqtt/{self.pub_id}/{next_seq}/{path_id}'
            try:
                info = client.publish(topic, json.dumps(p, ensure_ascii=False), qos=1)
                info.wait_for_publish(timeout=2.0)
                if info.is_published():
                    print(f'[PUB-OK] seq={next_seq} path={path_id} mid={info.mid}')
                    self._last_pub_ok[path_id] = True
                    published_any = True
                else:
                    print(f'[PUB-FAIL] seq={next_seq} path={path_id} not_published')
                    self._last_pub_ok[path_id] = False
            except Exception as e:
                print(f'[PUB-ERR] seq={next_seq} path={path_id} {e}')
                self._last_pub_ok[path_id] = False

        if published_any:
            self.seq_no = next_seq
            return True
        else:
            # 所有 path 都失败，等待重试
            return False

    def run(self, total: int, rate_hz: float):
        period = 1.0 / rate_hz
        next_t = time.monotonic()
        sent = 0
        retries = 0
        max_retries_per_msg = 500  # 500 * 0.05s = 25s，足够等待调度到达
        while sent < total:
            now = time.monotonic()
            if now < next_t:
                time.sleep(min(0.01, next_t - now))
                continue
            ok = self.send_one()
            if ok:
                sent += 1
                retries = 0
                next_t += period
            else:
                retries += 1
                if retries > max_retries_per_msg:
                    print(f'[PUB-ABORT] pair={self.pair_id} seq={self.seq_no + 1} '
                          f'failed after {max_retries_per_msg} retries')
                    break
                time.sleep(0.05)
        print(f'[PUB-FIN] pair={self.pair_id} sent={sent}/{total}')
        time.sleep(1.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run_id', default='demo3')
    ap.add_argument('--pair_id', default=None)
    ap.add_argument('--brokers_json', default='brokers_20.json')
    ap.add_argument('--pairs_json', default='pairs_5.json')
    ap.add_argument('--control_host', default='127.0.0.1')
    ap.add_argument('--control_port', type=int, default=1885)
    ap.add_argument('--count', type=int, default=1000)
    ap.add_argument('--rate_hz', type=float, default=5.0)
    args = ap.parse_args()
    pairs_json_path = Path(args.pairs_json)
    if not pairs_json_path.is_absolute():
        pairs_json_path = Path(__file__).parent / args.pairs_json
    all_pairs = json.loads(pairs_json_path.read_text(encoding='utf-8'))
    if args.pair_id is None:
        args.pair_id = all_pairs[0]['pair_id']
        print(f'auto pair_id={args.pair_id}')
    pub = MultiPairPublisher(args.run_id, args.brokers_json, args.pairs_json, args.pair_id,
                            args.control_host, args.control_port)
    pub.run(args.count, args.rate_hz)


if __name__ == '__main__':
    main()
