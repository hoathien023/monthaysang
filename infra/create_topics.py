#!/usr/bin/env python3
"""
infra/create_topics.py
----------------------
TV2 – Tạo Kafka topic cho đồ án Nhập môn Dữ liệu lớn.

Idempotent  : chạy lại nhiều lần không gây lỗi (topic đã tồn tại → bỏ qua).
--recreate  : xóa rồi tạo lại topic bench.* (web.* KHÔNG bị ảnh hưởng).

Cách dùng:
    python infra/create_topics.py
    python infra/create_topics.py --bootstrap-servers localhost:29092
    python infra/create_topics.py --recreate
    python infra/create_topics.py --recreate --show-reasons --verbose
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python infra/create_topics.py

Yêu cầu:
    pip install confluent-kafka
    Kafka đang chạy và healthy (docker compose up -d)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Final

try:
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError, KafkaException
except ImportError:
    print(
        "Lỗi: confluent-kafka chưa được cài.\n"
        "Chạy: pip install confluent-kafka",
        file=sys.stderr,
    )
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BOOTSTRAP: Final[str] = "localhost:29092"
DEFAULT_RF: Final[int] = 1          # replication factor; với 1 broker bắt buộc = 1
DELETE_WAIT_S: Final[int] = 25      # giây tối đa chờ Kafka propagate xóa topic


# ─────────────────────────────────────────────────────────────────────────────
# Topic definitions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TopicSpec:
    """Định nghĩa 1 Kafka topic."""
    name: str
    partitions: int
    reason: str   # giải thích số partition (dùng cho vấn đáp và --show-reasons)


TOPIC_SPECS: Final[list[TopicSpec]] = [
    # ── Các topic sản xuất ───────────────────────────────────────────────────
    TopicSpec(
        name="web.clickstream",
        partitions=6,
        reason=(
            "Topic chính với lưu lượng cao nhất (page_view, search, click, add_to_cart). "
            "6 = 2×3 cho phép chạy song song 1/2/3/6 consumer mà không lãng phí partition. "
            "Cũng là cấu hình cao nhất trong thí nghiệm E1 benchmark so sánh partition."
        ),
    ),
    TopicSpec(
        name="web.transactions",
        partitions=3,
        reason=(
            "Sự kiện giao dịch chiếm ~10-20% tổng lưu lượng (ít hơn clickstream). "
            "3 partition đủ để demo multi-consumer và giữ thứ tự per-user (key=user_id). "
            "Khớp với bench.p3 để so sánh chéo trong báo cáo."
        ),
    ),
    TopicSpec(
        name="web.events.dlq",
        partitions=1,
        reason=(
            "Dead-Letter Queue chỉ nhận sự kiện lỗi (~5% tổng theo thí nghiệm E6). "
            "1 partition đảm bảo thứ tự thời gian toàn cục, dễ debug và đối chiếu số lượng. "
            "Throughput thấp nên không cần song song hóa."
        ),
    ),
    # ── Các topic benchmark (chỉ dùng cho thí nghiệm E1) ────────────────────
    TopicSpec(
        name="bench.p1",
        partitions=1,
        reason="E1 baseline: 1 partition = ingestion hoàn toàn tuần tự, tối đa 1 consumer.",
    ),
    TopicSpec(
        name="bench.p3",
        partitions=3,
        reason="E1 mid-point: 3 partition, đo xem throughput có tăng tuyến tính không.",
    ),
    TopicSpec(
        name="bench.p6",
        partitions=6,
        reason="E1 high-end: 6 partition, kiểm tra đường cong throughput đạt cực đại ở đâu.",
    ),
]

# Tên topic benchmark (dùng cho --recreate)
BENCH_NAMES: Final[set[str]] = {s.name for s in TOPIC_SPECS if s.name.startswith("bench.")}


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def configure_logging(verbose: bool) -> logging.Logger:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    return logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# AdminClient helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_admin_client(bootstrap_servers: str) -> AdminClient:
    """Tạo AdminClient với timeout hợp lý."""
    return AdminClient({
        "bootstrap.servers": bootstrap_servers,
        "request.timeout.ms": 30_000,
        "socket.timeout.ms": 30_000,
        "log_level": 0,   # tắt log nội bộ confluent-kafka
    })


def get_broker_count(admin: AdminClient) -> int:
    """Đếm số broker đang hoạt động trong cluster."""
    md = admin.list_topics(timeout=30)
    return len(md.brokers)


def list_existing_names(admin: AdminClient) -> set[str]:
    """Trả về tên topic đang tồn tại (bỏ qua topic nội bộ bắt đầu bằng '__')."""
    md = admin.list_topics(timeout=30)
    return {name for name in md.topics if not name.startswith("__")}


def get_actual_partition_counts(admin: AdminClient) -> dict[str, int]:
    """Lấy số partition thực tế của từng topic từ metadata Kafka."""
    md = admin.list_topics(timeout=30)
    return {
        name: len(meta.partitions)
        for name, meta in md.topics.items()
        if not name.startswith("__")
    }


# ─────────────────────────────────────────────────────────────────────────────
# Delete & Create
# ─────────────────────────────────────────────────────────────────────────────

def delete_topics(
    admin: AdminClient,
    names: list[str],
    log: logging.Logger,
) -> dict[str, str]:
    """
    Xóa danh sách topic rồi chờ metadata propagate.

    Returns:
        {topic_name: "DELETED" | "NOT_FOUND" | "ERROR:<msg>"}
    """
    if not names:
        return {}

    log.info("Đang xóa %d topic: %s", len(names), names)
    futures = admin.delete_topics(names, operation_timeout=30)
    results: dict[str, str] = {}

    for topic, future in futures.items():
        try:
            future.result()
            results[topic] = "DELETED"
            log.debug("  ✓ Xóa thành công: %s", topic)
        except KafkaException as exc:
            err: KafkaError = exc.args[0]
            if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                # Topic không tồn tại → không phải lỗi
                results[topic] = "NOT_FOUND"
                log.debug("  – Không tồn tại (bỏ qua): %s", topic)
            else:
                results[topic] = f"ERROR: {err.str()}"
                log.error("  ✗ Lỗi xóa %s: %s", topic, err)

    # Chờ Kafka xóa xong khỏi metadata trước khi tạo lại
    deleted = {n for n, s in results.items() if s == "DELETED"}
    _wait_until_absent(admin, deleted, log)
    return results


def _wait_until_absent(
    admin: AdminClient,
    names: set[str],
    log: logging.Logger,
) -> None:
    """Poll cho đến khi các topic biến mất khỏi metadata Kafka (tối đa DELETE_WAIT_S giây)."""
    if not names:
        return
    deadline = time.monotonic() + DELETE_WAIT_S
    while time.monotonic() < deadline:
        still_there = names & list_existing_names(admin)
        if not still_there:
            log.info("Tất cả topic đã xóa khỏi metadata.")
            return
        log.debug("Vẫn còn trong metadata: %s — đợi 2 giây...", still_there)
        time.sleep(2)
    still_there = names & list_existing_names(admin)
    if still_there:
        log.warning(
            "Hết %ds chờ; %s vẫn còn trong metadata. "
            "Có thể gặp lỗi khi tạo lại.",
            DELETE_WAIT_S, still_there,
        )


def create_topics(
    admin: AdminClient,
    specs: list[TopicSpec],
    replication_factor: int,
    log: logging.Logger,
) -> dict[str, str]:
    """
    Tạo topic. Idempotent: TOPIC_ALREADY_EXISTS không gây lỗi.

    Returns:
        {topic_name: "CREATED" | "EXISTS" | "ERROR:<msg>"}
    """
    new_topics = [
        NewTopic(
            s.name,
            num_partitions=s.partitions,
            replication_factor=replication_factor,
        )
        for s in specs
    ]
    futures = admin.create_topics(new_topics, operation_timeout=30)
    results: dict[str, str] = {}

    for topic, future in futures.items():
        try:
            future.result()
            results[topic] = "CREATED"
            log.debug("  ✓ Đã tạo: %s", topic)
        except KafkaException as exc:
            err: KafkaError = exc.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                results[topic] = "EXISTS"
                log.debug("  – Đã tồn tại (bỏ qua): %s", topic)
            else:
                results[topic] = f"ERROR: {err.str()}"
                log.error("  ✗ Lỗi tạo %s: %s", topic, err)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_DISPLAY: Final[dict[str, tuple[str, str]]] = {
    "CREATED":   ("✓", "CREATED"),
    "EXISTS":    ("-", "EXISTS (skipped)"),
    "RECREATED": ("R", "RECREATED"),
}


def print_results_table(
    specs: list[TopicSpec],
    statuses: dict[str, str],
    actual_partitions: dict[str, int],
    replication_factor: int,
) -> None:
    """In bảng kết quả dạng ASCII ra stdout."""
    W_TOPIC, W_P, W_RF, W_STATUS = 22, 9, 4, 20
    sep = "-" * (W_TOPIC + W_P * 2 + W_RF + W_STATUS + 16)

    header = (
        f"{'Topic':<{W_TOPIC}}   "
        f"{'Expect P':>{W_P}}   "
        f"{'Actual P':>{W_P}}   "
        f"{'RF':>{W_RF}}   "
        f"{'Status':<{W_STATUS}}"
    )

    print()
    print(sep)
    print(header)
    print(sep)

    ok_count = 0
    for spec in specs:
        raw = statuses.get(spec.name, "UNKNOWN")
        icon, label = _STATUS_DISPLAY.get(raw, ("!", raw))
        actual = actual_partitions.get(spec.name)
        actual_str = str(actual) if actual is not None else "?"
        partition_ok = (actual == spec.partitions)
        is_ok = raw in _STATUS_DISPLAY and partition_ok
        if is_ok:
            ok_count += 1
        mismatch_flag = "" if partition_ok else " !"
        print(
            f"{spec.name:<{W_TOPIC}}   "
            f"{spec.partitions:>{W_P}}   "
            f"{actual_str + mismatch_flag:>{W_P}}   "
            f"{replication_factor:>{W_RF}}   "
            f"[{icon}] {label}"
        )

    print(sep)
    result_mark = "OK" if ok_count == len(specs) else "FAIL"
    print(f"[{result_mark}] {ok_count}/{len(specs)} topic san sang\n")


def print_partition_reasons(specs: list[TopicSpec]) -> None:
    """In giải thích lý do chọn số partition."""
    print("=" * 70)
    print("Ly do chon so partition")
    print("=" * 70)
    for spec in specs:
        print(f"\n  {spec.name}  ({spec.partitions} partition)")
        # Wrap text tại ~65 ký tự
        words = spec.reason.split()
        line = "    "
        for word in words:
            if len(line) + len(word) + 1 > 68:
                print(line)
                line = "    " + word
            else:
                line += (" " if line.strip() else "") + word
        if line.strip():
            print(line)
    print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="create_topics.py",
        description="TV2 – Tao Kafka topic (idempotent).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Bien moi truong:
  KAFKA_BOOTSTRAP_SERVERS    Dia chi Kafka (mac dinh: localhost:29092)
  KAFKA_REPLICATION_FACTOR   Replication factor (mac dinh: 1)

Vi du:
  python infra/create_topics.py
  python infra/create_topics.py --recreate --verbose
  python infra/create_topics.py --show-reasons
        """,
    )
    parser.add_argument(
        "--bootstrap-servers", "-b",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
        metavar="HOST:PORT",
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP})",
    )
    parser.add_argument(
        "--replication-factor", "-r",
        type=int,
        default=int(os.environ.get("KAFKA_REPLICATION_FACTOR", str(DEFAULT_RF))),
        metavar="N",
        help="Replication factor (default: 1; voi 1 broker phai la 1)",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Xoa va tao lai topic bench.* truoc khi chay thi nghiem. Topic web.* KHONG bi anh huong.",
    )
    parser.add_argument(
        "--show-reasons",
        action="store_true",
        help="In ly do chon so partition sau bang ket qua.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="In log DEBUG chi tiet.",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    log = configure_logging(args.verbose)

    log.info("=== create_topics.py ===")
    log.info("Bootstrap : %s", args.bootstrap_servers)
    log.info("RF        : %d", args.replication_factor)
    log.info("--recreate: %s", args.recreate)

    # ── 1. Kết nối ───────────────────────────────────────────────────────────
    try:
        admin = build_admin_client(args.bootstrap_servers)
        broker_count = get_broker_count(admin)
        log.info("Ket noi OK — %d broker dang hoat dong.", broker_count)
    except KafkaException as exc:
        log.error("Khong ket noi duoc Kafka: %s", exc)
        log.error(
            "Goi y: Chay 'docker compose ps' — kafka phai o trang thai (healthy). "
            "Neu chua healthy, doi them ~30 giay sau 'docker compose up -d'."
        )
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        log.error("Loi khong xac dinh khi ket noi: %s", exc)
        sys.exit(1)

    # Kiểm tra RF vs broker count trước khi làm gì
    if args.replication_factor > broker_count:
        log.error(
            "Replication factor %d > so broker %d → tao topic se that bai. "
            "Vui long dung --replication-factor %d.",
            args.replication_factor, broker_count, broker_count,
        )
        sys.exit(1)

    final_statuses: dict[str, str] = {}
    recreated_set: set[str] = set()

    # ── 2. Xóa bench.* nếu --recreate ────────────────────────────────────────
    if args.recreate:
        log.info("--recreate: xoa topic bench.* roi tao lai...")
        del_results = delete_topics(admin, sorted(BENCH_NAMES), log)
        # Các topic bị xóa thành công sẽ được đánh dấu RECREATED (không phải CREATED)
        recreated_set = {name for name, st in del_results.items() if st == "DELETED"}

    # ── 3. Tạo tất cả topic ───────────────────────────────────────────────────
    log.info("Dang tao %d topic...", len(TOPIC_SPECS))
    create_results = create_topics(admin, TOPIC_SPECS, args.replication_factor, log)

    for name, status in create_results.items():
        if name in recreated_set and status == "CREATED":
            final_statuses[name] = "RECREATED"
        else:
            final_statuses[name] = status

    # ── 4. Lấy partition count thực tế để verify ──────────────────────────────
    actual = get_actual_partition_counts(admin)

    # ── 5. In bảng kết quả ───────────────────────────────────────────────────
    print_results_table(TOPIC_SPECS, final_statuses, actual, args.replication_factor)

    if args.show_reasons:
        print_partition_reasons(TOPIC_SPECS)

    # ── 6. Exit code ─────────────────────────────────────────────────────────
    errors = [n for n, s in final_statuses.items() if s.startswith("ERROR")]
    partition_mismatches = [
        s.name for s in TOPIC_SPECS
        if actual.get(s.name) != s.partitions
    ]

    if errors:
        log.error("Tao that bai: %s", errors)
        sys.exit(1)

    if partition_mismatches:
        log.error(
            "So partition thuc te khong khop ky vong: %s. "
            "Neu la bench.*, chay lai voi --recreate.",
            partition_mismatches,
        )
        sys.exit(1)

    log.info("Tat ca topic san sang. Co the chay producer/consumer.")


if __name__ == "__main__":
    main()
