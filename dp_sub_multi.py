import argparse
import json
import math
import statistics
import threading
import time
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt


def load_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def make_client(client_id: str) -> mqtt.Client:
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
    except Exception:
        return mqtt.Client(client_id=client_id)


def percentile_from_sorted(sorted_values, p: float):
    if not sorted_values:
        return None
    idx = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return sorted_values[idx]


class MultiPairSubscriber:
    def __init__(self, run_id: str, brokers_json: str, pairs_json: str, pair_id: str, control_host: str, control_port: int,
                 window_size: int, d_l_sec: float, target_messages: int):
        self.run_id = run_id
        self.brokers = load_json(brokers_json)
        self.broker_by_id = {b['id']: b for b in self.brokers}
        all_pairs = load_json(pairs_json)
        self.pair = next(p for p in all_pairs if p['pair_id'] == pair_id)
        self.pair_id = pair_id
        self.topic_business = self.pair['topic']
        self.subscriber_pool = list(self.pair.get('subscriber_pool', []))
        self.control_host = control_host
        self.control_port = int(control_port)
        self.window_size_default = int(window_size)
        self.d_l_sec = float(d_l_sec)
        self.target_messages = int(target_messages)
        self.window_timeout_sec = 8.0
        self.empty_window_streak = 0
        self.max_empty_window_streak = 10
        self.arrivals_sec = {}
        self.winners = {}
        self.schedules = {}
        self.window_start = None
        self.window_end = None
        self.window_w = self.window_size_default
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._finalizing = False
        self._finalize_timer = None
        self._deadline_timer = None
        self._first_schedule_received = False
        self.received_delays_sec = []
        self.received_mask = [False] * (self.target_messages + 1)
        # ---- MAB 反馈增强指标：每路径时延和 first arrival ----
        self._path_delays: dict = {}
        self._first_arrival_delays: dict = {}
        # ---- 自适应 m 跟踪：每个窗口的 m 值（用于 stats JSON）----
        self._m_history: list = []  # [(ws, m_current, estimated_R, target_R, priority_class), ...]
        self._m_lock = threading.Lock()
        # 控制平面：连接到当前树根节点的 MQTT（动态）
        self.ctrl = make_client(f'SUBCTRL_{run_id}_{pair_id}')
        self.ctrl.on_connect = self._on_ctrl_connect
        self.ctrl.on_message = self._on_ctrl_msg
        self.ctrl.reconnect_delay_set(min_delay=1, max_delay=5)
        self.ctrl.connect(self.control_host, self.control_port, 30)
        self.ctrl.loop_start()
        self.sub_clients = []
        self._sub_ready = {bid: False for bid in self.subscriber_pool}
        # 数据平面：每个 subscriber 连接自己的本地 MQTT（通过 MQTT-ST 树接收）
        for bid in self.subscriber_pool:
            b = self.broker_by_id[bid]

            c = make_client(f'SUB_{run_id}_{pair_id}_{bid}')
            c.user_data_set({'bid': bid})

            c.on_connect = self._on_sub_connect
            c.on_disconnect = self._on_sub_disconnect
            c.on_message = self._on_data_msg

            # 关键：先登记，再 connect / loop_start
            self.sub_clients.append((bid, c))

            c.connect(b['host'], int(b['port']), 30)
            c.loop_start()

            print(
                f'[SUB] connecting to broker {bid} at {b["host"]}:{b["port"]} '
                f'for data topic={self.topic_business}'
            )
        print(f'Subscriber started pair={self.pair_id} topic={self.topic_business} subscriber_pool={self.subscriber_pool} data_via=MQTT-ST-overlay')

    def topic_sel(self):
        return f'asr/{self.run_id}/{self.pair_id}/control/path_selection'

    def topic_metrics(self):
        return f'asr/{self.run_id}/{self.pair_id}/control/metrics'

    def _cancel(self, t):
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        return None

    def _start_deadline(self):
        self._deadline_timer = self._cancel(self._deadline_timer)
        t = threading.Timer(self.window_timeout_sec, self._finalize_current_window_safe, kwargs={'reason':'deadline'})
        t.daemon = True
        self._deadline_timer = t
        t.start()

    def _on_sub_connect(self, client, userdata, flags, rc):
        """数据平面 MQTT 订阅回调：连接成功后在对应 broker 上订阅业务 topic"""
        bid = None
        if isinstance(userdata, dict):
            bid = userdata.get('bid')

        if bid is None:
            print('[SUB] on_conn WARN: bid missing, cannot subscribe')
            return

        if rc == 0:
            result, mid = client.subscribe(f'{self.topic_business}/rmqtt/#', qos=1)
            if result != 0:  # 0 = MQTT_ERR_SUCCESS
                print(
                    f'[SUB] subscribe FAIL broker={bid} topic={self.topic_business}/rmqtt/# '
                    f'result={result} mid={mid}',
                    flush=True
                )
                return
            with self._lock:
                self._sub_ready[bid] = True
            print(
                f'[SUB] on_conn OK broker={bid} '
                f'subscribed topic={self.topic_business}/rmqtt/# result={result} mid={mid}',
                flush=True
            )
        else:
            print(f'[SUB] on_conn FAIL broker={bid} rc={rc}', flush=True)

    def _on_sub_disconnect(self, client, userdata, *args):
        """数据平面 MQTT 断开回调：自动重连"""
        rc = args[-1] if args else None

        bid = None
        if isinstance(userdata, dict):
            bid = userdata.get('bid')

        if bid is None:
            for b, c in self.sub_clients:
                if c is client:
                    bid = b
                    break

        if bid is None:
            print(f'[SUB] on_disconn WARN: unknown client rc={rc}', flush=True)
            return

        with self._lock:
            self._sub_ready[bid] = False
        print(f'[SUB] on_disconn broker={bid} rc={rc}, reconnecting...', flush=True)
        try:
            client.reconnect()
        except Exception as e:
            print(f'[SUB] reconnect failed broker={bid}: {e}', flush=True)

    def _schedule_finalize(self):
        if self._finalize_timer is not None:
            return
        self._deadline_timer = self._cancel(self._deadline_timer)
        t = threading.Timer(0.2, self._finalize_current_window_safe, kwargs={'reason':'window_end'})
        t.daemon = True
        self._finalize_timer = t
        t.start()

    def _init_window(self, ws: int, w: int):
        self.window_start = int(ws)
        self.window_w = int(w)
        self.window_end = self.window_start + self.window_w - 1
        self.arrivals_sec.clear()
        self.winners.clear()
        self._path_delays.clear()
        self._first_arrival_delays.clear()
        self._finalizing = False
        self._finalize_timer = self._cancel(self._finalize_timer)
        self._start_deadline()

    def _on_ctrl_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self.topic_sel(), qos=1)
            print(f'[SUB] subscribed path_selection pair={self.pair_id}')
        else:
            print(f'[SUB] control connect failed rc={rc}')

    def _on_ctrl_msg(self, client, userdata, msg):
        if not msg.payload:
            return
        s = json.loads(msg.payload.decode('utf-8'))
        ws = int(s['window_start'])
        w = int(s['window_size'])
        sec = list(s['secondary_schedule'])
        if len(sec) != w:
            return
        publisher_pool = list(s.get('publisher_pool', []))
        subscriber_pool = list(s.get('subscriber_pool', []))
        # ---- 新增：自适应 m 字段 ----
        path_schedule = s.get('path_schedule')
        m_current = int(s.get('m_current', 0))
        priority_class = str(s.get('priority_class', 'normal'))
        target_reliability = float(s.get('target_reliability', 0.99))
        estimated_reliability = float(s.get('estimated_reliability', 0.0))

        print(
            f'[SUB-ADAPT-M] pair={self.pair_id} ws={ws} '
            f'priority={priority_class} m={m_current} '
            f'target_R={target_reliability} estimated_R={estimated_reliability} '
            f'first_slot_paths={path_schedule[0] if path_schedule else "legacy"}',
            flush=True
        )

        # ---- 记录 m_current 历史（用于最终 stats）----
        with self._m_lock:
            self._m_history.append({
                'ws': ws,
                'm_current': m_current,
                'estimated_R': estimated_reliability,
                'target_R': target_reliability,
                'priority_class': priority_class,
            })

        with self._lock:
            self.schedules[ws] = {
                'ws': ws,
                'w': w,
                'primary': str(s['primary_id']),
                'sec': sec,
                'publisher_pool': publisher_pool,
                'subscriber_pool': subscriber_pool,
                'path_schedule': path_schedule,
                'm_current': m_current,
                'priority_class': priority_class,
                'target_reliability': target_reliability,
                'estimated_reliability': estimated_reliability,
            }
            if not self._first_schedule_received:
                self._first_schedule_received = True
                print(f'[ASR] first schedule received pair={self.pair_id} ws={ws} primary={s["primary_id"]}, activate data receiving')
            if self.window_start is None:
                # 收到第一个 schedule 立即初始化窗口并启动 deadline
                # 不等待第一条数据，否则当 adapter/转发出错时会导致死锁（窗口永远不 finalized）
                self._init_window(ws, w)
                print(f'[ASR] init window pair={self.pair_id} ws={ws}-{ws+w-1} from first schedule', flush=True)
            elif ws < self.window_start:
                # 【Fix 4】旧 retained schedule 只缓存，禁止回滚当前已推进的窗口
                print(f'[ASR] ignore old schedule ws={ws}, current={self.window_start}-{self.window_end}', flush=True)
            else:
                # 未来窗口或正好 ws == window_start：正常处理
                pass

    def _parse_topic(self, topic: str):
        # topic 格式: {business_topic}/rmqtt/{pub_id}/{seq_no}/{path_id}
        # 示例: factory/agv/state/rmqtt/abcd1234/1/E1
        # parts: [factory, agv, state, rmqtt, abcd1234, 1, E1]
        #   idx=3, idx+1=pub_id, idx+2=seq, idx+3=path_id
        # path_id（E1/E2/...）才是用于路径质量统计的 broker
        parts = topic.split('/')
        try:
            idx = parts.index('rmqtt')
            seq = int(parts[idx + 2])
            path_id = str(parts[idx + 3])
            return seq, path_id
        except Exception as e:
            print(f'[SUB-PARSE-ERR] topic={topic} err={e}', flush=True)
            return None

    def _on_data_msg(self, client, userdata, msg):
        print(f'[SUB-RX] topic={msg.topic}')
        parsed = self._parse_topic(msg.topic)
        if not parsed:
            return
        seq, path_id = parsed
        payload = json.loads(msg.payload.decode('utf-8'))

        # 【修复统计隔离 Bug】必须过滤非本轮、非本 pair 的残留消息
        if payload.get('run_id') != self.run_id:
            return
        if payload.get('pair_id') != self.pair_id:
            return

        ts_send = float(payload.get('ts_send', 0.0))
        delay_sec = time.time() - ts_send if ts_send > 0 else 0.0
        with self._lock:
            if not self._first_schedule_received:
                return
            # 消息所属窗口预初始化（只用于未来窗口，当前窗口由 _on_ctrl_msg 初始化）
            ws_of_msg = ((seq - 1) // self.window_size_default) * self.window_size_default + 1
            if seq > self.window_end and ws_of_msg > self.window_end:
                next_ws = self.window_end + 1
                next_sched = self.schedules.get(next_ws)
                next_w = int(next_sched['w']) if next_sched else self.window_w
                self._init_window(next_ws, next_w)
            if not (self.window_start <= seq <= self.window_end):
                return
            self.arrivals_sec.setdefault(seq, {})[path_id] = delay_sec
            self._path_delays.setdefault(path_id, []).append(delay_sec)
            if seq not in self.winners:
                self.winners[seq] = (path_id, delay_sec)
                self._first_arrival_delays[seq] = delay_sec
                if 1 <= seq <= self.target_messages and not self.received_mask[seq]:
                    self.received_mask[seq] = True
                    self.received_delays_sec.append(delay_sec)
                    if sum(1 for i in range(1, self.target_messages + 1) if self.received_mask[i]) >= self.target_messages:
                        self._stop_event.set()
            if seq == self.window_end:
                self._schedule_finalize()

    def _expected_paths_for_window(self, ws: int, we: int, w: int):
        """
        从当前窗口的 path_schedule 或 primary + secondary_schedule 动态推导 expected paths。
        优先使用 path_schedule（adaptive_m），否则回退到 primary+secondary_schedule。
        """
        sched = self.schedules.get(ws)
        if not sched:
            return {}
        # ---- 优先使用 path_schedule ----
        path_schedule = sched.get('path_schedule')
        if path_schedule:
            expected = {}
            for i in range(w):
                seq = ws + i
                slot_paths = path_schedule[i] if i < len(path_schedule) else []
                for p in slot_paths:
                    expected.setdefault(str(p), []).append(seq)
            return expected
        # ---- 兼容旧算法：使用 primary + secondary_schedule ----
        primary = str(sched.get('primary', ''))
        sec = list(sched.get('sec', []))
        expected = {}
        for i in range(w):
            seq = ws + i
            expected.setdefault(primary, []).append(seq)
            s2 = str(sec[i]) if i < len(sec) else primary
            if s2 != primary:
                expected.setdefault(s2, []).append(seq)
        return expected

    def _finalize_current_window_safe(self, reason='unknown'):
        with self._lock:
            if self._finalizing:
                return
            self._finalizing = True
        ws, we, w = self.window_start, self.window_end, self.window_w
        if ws is None:
            return
        sched = self.schedules.get(ws, {})
        # expected 从 primary+sec 动态推导，不再依赖静态 pool
        expected = self._expected_paths_for_window(ws, we, w)
        path_metrics = {}
        for bid in expected.keys():
            exp = expected.get(bid, [])
            n_t = len(exp)
            delays = []
            n_r = 0
            for seq in exp:
                got = self.arrivals_sec.get(seq, {}).get(bid)
                if got is not None:
                    n_r += 1
                    delays.append(float(got))
            n_l = n_t - n_r
            phi = n_l / n_t if n_t > 0 else 1.0
            avg_delay_sec = statistics.mean(delays) if delays else 0.0
            q_sec = phi * self.d_l_sec + (1.0 - phi) * avg_delay_sec

            # ---- MAB 反馈增强：p95/p99/jitter ----
            sorted_delays = sorted(delays)
            p95_delay_sec = percentile_from_sorted(sorted_delays, 0.95) if sorted_delays else 0.0
            p99_delay_sec = percentile_from_sorted(sorted_delays, 0.99) if sorted_delays else 0.0

            jitter_sec = 0.0
            if len(delays) >= 2:
                gaps = [abs(delays[i] - delays[i - 1]) for i in range(1, len(delays))]
                jitter_sec = statistics.mean(gaps)

            throughput_msgs = n_r

            winner_count = 0
            deadline_miss_count = 0

            for exp_seq in exp:
                if exp_seq in self.winners and self.winners[exp_seq][0] == bid:
                    winner_count += 1

                if exp_seq not in self.arrivals_sec or bid not in self.arrivals_sec.get(exp_seq, {}):
                    deadline_miss_count += 1

            winner_ratio = winner_count / n_t if n_t > 0 else 0.0
            deadline_miss_ratio = deadline_miss_count / n_t if n_t > 0 else 1.0

            path_metrics[bid] = {
                'n_T': n_t,
                'n_R': n_r,
                'n_L': n_l,
                'phi': phi,

                'avg_delay_sec': avg_delay_sec,
                'avg_delay_ms': avg_delay_sec * 1000.0,

                'p95_delay_sec': p95_delay_sec,
                'p95_delay_ms': p95_delay_sec * 1000.0,

                'p99_delay_sec': p99_delay_sec,
                'p99_delay_ms': p99_delay_sec * 1000.0,

                'jitter_sec': jitter_sec,
                'jitter_ms': jitter_sec * 1000.0,

                'q_sec': q_sec,
                'q_ms': q_sec * 1000.0,

                'throughput_msgs': throughput_msgs,
                'winner_ratio': winner_ratio,
                'deadline_miss_ratio': deadline_miss_ratio,
            }

        # ---- MAB 反馈增强：first arrival 指标（所有路径合并）----
        first_arrivals = [
            self._first_arrival_delays[seq]
            for seq in range(ws, we + 1)
            if seq in self._first_arrival_delays
        ]

        if first_arrivals:
            sorted_fa = sorted(first_arrivals)
            combined_mean = statistics.mean(sorted_fa)
            combined_p99 = percentile_from_sorted(sorted_fa, 0.99) if sorted_fa else 0.0
        else:
            combined_mean = 0.0
            combined_p99 = 0.0

        for bid in path_metrics:
            path_metrics[bid]['first_arrival_mean_sec'] = combined_mean
            path_metrics[bid]['first_arrival_mean_ms'] = combined_mean * 1000.0
            path_metrics[bid]['first_arrival_p99_sec'] = combined_p99
            path_metrics[bid]['first_arrival_p99_ms'] = combined_p99 * 1000.0

        report = {
            'run_id': self.run_id,
            'pair_id': self.pair_id,
            'window_start': ws,
            'window_end': we,
            'window_size': w,
            'path_metrics': path_metrics,
            'finalize_reason': reason,
            'timestamp': time.time()
        }
        self.ctrl.publish(self.topic_metrics(), json.dumps(report), qos=1)
        print(f'finalize pair={self.pair_id} window={ws}-{we} reason={reason}')
        for bid, pm in path_metrics.items():
            print(f'  [{bid}] n_T={pm["n_T"]} n_R={pm["n_R"]} phi={pm["phi"]:.4f} avg={pm["avg_delay_ms"]:.2f}ms q={pm["q_ms"]:.2f}ms')
        total_rx_this_window = sum(pm["n_R"] for pm in path_metrics.values())
        if total_rx_this_window == 0:
            self.empty_window_streak += 1
        else:
            self.empty_window_streak = 0
        if self.empty_window_streak >= self.max_empty_window_streak:
            print(f'[SUB-ABORT] pair={self.pair_id} no data for {self.empty_window_streak} windows, stop', flush=True)
            self._stop_event.set()
            return
        if we >= self.target_messages:
            threading.Timer(2.0, self._stop_event.set).start()
            return
        next_ws = we + 1
        next_sched = self.schedules.get(next_ws)
        next_w = int(next_sched['w']) if next_sched else self.window_w
        with self._lock:
            self._init_window(next_ws, next_w)

    def run_forever(self):
        try:
            while not self._stop_event.is_set():
                time.sleep(0.2)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop_and_report()

    def stop_and_report(self):
        self._finalize_timer = self._cancel(self._finalize_timer)
        self._deadline_timer = self._cancel(self._deadline_timer)

        n = len(self.received_delays_sec)

        Path('out').mkdir(parents=True, exist_ok=True)
        out_path = Path('out') / f'stats_{self.run_id}_{self.pair_id}.json'

        # ---- 汇总自适应 m 统计（取所有窗口的平均值）----
        with self._m_lock:
            m_history = list(self._m_history)
        if m_history:
            avg_m = sum(h['m_current'] for h in m_history) / len(m_history)
            final_m = m_history[-1]['m_current']
            final_estimated_R = m_history[-1]['estimated_R']
            final_target_R = m_history[-1]['target_R']
            final_priority = m_history[-1]['priority_class']
            m_summary = {
                'avg_m_current': round(avg_m, 2),
                'final_m_current': final_m,
                'final_estimated_R': round(final_estimated_R, 6),
                'final_target_R': round(final_target_R, 6),
                'final_priority_class': final_priority,
                'windows_recorded': len(m_history),
                'detail': [
                    {
                        'ws': h['ws'],
                        'm_current': h['m_current'],
                        'estimated_R': round(h['estimated_R'], 6),
                        'target_R': round(h['target_R'], 6),
                        'priority_class': h['priority_class'],
                    }
                    for h in m_history
                ],
            }
        else:
            m_summary = {
                'avg_m_current': 0.0,
                'final_m_current': 0,
                'final_estimated_R': 0.0,
                'final_target_R': 0.0,
                'final_priority_class': 'unknown',
                'windows_recorded': 0,
                'detail': [],
            }

        # 关键修复：
        # 即使一条数据都没有收到，也必须写 stats 文件。
        # 否则 run_benchmark.py 会误判为"统计文件未找到"，无法区分：
        # 1. subscriber 崩溃
        # 2. subscriber 正常退出但 0 接收
        # 3. 数据面完全没通
        if n == 0:
            stats = {
                'run_id': self.run_id,
                'pair_id': self.pair_id,
                'received_count': 0,
                'expected_count': self.target_messages,
                'success_ratio': 0.0,
                'min_ms': 0.0,
                'avg_ms': 0.0,
                'p95_ms': 0.0,
                'p99_ms': 0.0,
                'max_ms': 0.0,
                'std_ms': 0.0,
                'loss_rate': 1.0,
                'empty_window_streak': self.empty_window_streak,
                'max_empty_window_streak': self.max_empty_window_streak,
                'first_schedule_received': self._first_schedule_received,
                'last_window_start': self.window_start,
                'last_window_end': self.window_end,
                'known_schedules': sorted(list(self.schedules.keys())),
                'sub_ready': dict(self._sub_ready),
                'reason': 'no_data',
                'timestamp': time.time(),
                'adaptive_m': m_summary,
            }
            out_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding='utf-8')
            print(
                f'RESULT pair={self.pair_id} success=0.000% '
                f'received=0/{self.target_messages} reason=no_data '
                f'empty_window_streak={self.empty_window_streak} saved={out_path}',
                flush=True
            )
            return

        delays = sorted(self.received_delays_sec)
        avg = sum(delays) / n
        p95 = percentile_from_sorted(delays, 0.95)
        p99 = percentile_from_sorted(delays, 0.99)

        stats = {
            'run_id': self.run_id,
            'pair_id': self.pair_id,
            'received_count': n,
            'expected_count': self.target_messages,
            'success_ratio': n / self.target_messages if self.target_messages else 0.0,
            'min_ms': min(delays) * 1000.0,
            'avg_ms': avg * 1000.0,
            'p95_ms': p95 * 1000.0,
            'p99_ms': p99 * 1000.0,
            'max_ms': max(delays) * 1000.0,
            'std_ms': (statistics.pstdev(delays) * 1000.0) if n > 1 else 0.0,
            'loss_rate': 1.0 - (n / self.target_messages if self.target_messages else 0.0),
            'empty_window_streak': self.empty_window_streak,
            'max_empty_window_streak': self.max_empty_window_streak,
            'first_schedule_received': self._first_schedule_received,
            'last_window_start': self.window_start,
            'last_window_end': self.window_end,
            'known_schedules': sorted(list(self.schedules.keys())),
            'sub_ready': dict(self._sub_ready),
            'reason': 'ok',
            'timestamp': time.time(),
            'adaptive_m': m_summary,
        }

        out_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding='utf-8')
        print(
            f'RESULT pair={self.pair_id} success={stats["success_ratio"]*100:.3f}% '
            f'avg={stats["avg_ms"]:.3f}ms p95={stats["p95_ms"]:.3f}ms '
            f'p99={stats["p99_ms"]:.3f}ms saved={out_path}',
            flush=True
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run_id', default='demo3')
    ap.add_argument('--pair_id', default=None)
    ap.add_argument('--brokers_json', default='brokers_20.json')
    ap.add_argument('--pairs_json', default='pairs_5.json')
    ap.add_argument('--control_host', default='127.0.0.1')
    ap.add_argument('--control_port', type=int, default=1885)
    ap.add_argument('--window_size', type=int, default=24)
    ap.add_argument('--D_L_sec', type=float, default=1.0)
    ap.add_argument('--count', type=int, default=1000)
    args = ap.parse_args()
    pairs_json_path = Path(args.pairs_json)
    if not pairs_json_path.is_absolute():
        pairs_json_path = Path(__file__).parent / pairs_json_path
    all_pairs = json.loads(pairs_json_path.read_text(encoding='utf-8'))
    if args.pair_id is None:
        args.pair_id = all_pairs[0]['pair_id']
        print(f'auto pair_id={args.pair_id}')
    sub = MultiPairSubscriber(args.run_id, args.brokers_json, args.pairs_json, args.pair_id, args.control_host,
                              args.control_port, args.window_size, args.D_L_sec, args.count)
    sub.run_forever()


if __name__ == '__main__':
    main()
