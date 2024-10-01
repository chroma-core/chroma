import threading
import time
import concurrent.futures
import queue

from typing import Optional, cast


class Ticket:
    def __init__(self, issued_by: Optional["AdmissionController"]) -> None:
        self.acquisition_elapsed = 0.0
        self.issued_by = issued_by

    def release(self) -> None:
        if self.issued_by:
            self.issued_by.release()
            self.issued_by = None


class Waiter:
    def __init__(self) -> None:
        self.index = 0
        self.woke_up = False

    def notify(self) -> None:
        self.woke_up = True


class AdmissionController:
    def admit_one(self) -> Optional[Ticket]:
        raise NotImplementedError

    def release(self) -> None:
        raise NotImplementedError

    def admitted(self) -> int:
        raise NotImplementedError

    def capacity(self) -> int:
        raise NotImplementedError

    def resize(self) -> int:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError


class AdmissionControllerImpl(AdmissionController):
    FIFO_MODE = 0
    LIFO_MODE = 1
    DEFAULT_M = 0.005
    DEFAULT_N = 0.1

    def __init__(self, parallelism: int, M: float = DEFAULT_M, N: float = DEFAULT_N):
        self.mtx = threading.Lock()
        self.stopped = False
        self.M = M
        self.N = N
        self.allowed = parallelism
        self.admitted_count = 0
        self.last_empty = time.time()
        self.queue_mode = self.FIFO_MODE
        self.waiters: list[Optional[Waiter]] = [None] * 64
        self.head = 0
        self.tail = 0
        self.waiter_condition = threading.Condition(self.mtx)

    def admit_one(self) -> Optional[Ticket]:
        admit = False
        stopped = self.stopped

        with self.mtx:
            if self.head == self.tail and self.admitted_count < self.allowed:
                self.admitted_count += 1
                admit = True

        if stopped:
            return None

        tk = Ticket(issued_by=self)
        if admit:
            return tk

        return self.admit_one_slow_path(tk=tk)

    def admit_one_slow_path(self, tk: Ticket) -> Optional[Ticket]:
        t0 = time.time()
        waiter = Waiter()
        timeout, enqueued = self.enqueue_waiter(waiter=waiter)

        if not enqueued:
            tk.acquisition_elapsed = time.time() - t0
            return tk

        proceed = False

        with self.waiter_condition:
            self.waiter_condition.wait_for(lambda: waiter.woke_up, timeout)
            proceed = waiter.woke_up

        self.remove_waiter(waiter)

        if not proceed:
            proceed = waiter.woke_up

        if proceed:
            tk.acquisition_elapsed = time.time() - t0
            return tk

        return None

    def enqueue_waiter(self, waiter: Waiter) -> tuple[float, bool]:
        now = time.time()

        with self.mtx:
            self.check_invariants()
            if self.head == self.tail and self.admitted_count < self.allowed:
                self.admitted_count += 1
                return 0, False

            if (self.tail + 1) % len(self.waiters) == self.head:
                self.resize_waiters()

            self.adjust_queue_mode(now)
            waiter.index = self.tail
            self.waiters[self.tail] = waiter
            self.tail = (self.tail + 1) % len(self.waiters)
            self.check_invariants()
            if self.queue_mode == self.LIFO_MODE:
                return self.M, True
            return self.N, True

    def remove_waiter(self, waiter: Waiter) -> None:
        with self.mtx:
            self.remove_waiter_no_mtx(waiter=waiter)

    def remove_waiter_no_mtx(self, waiter: Waiter) -> None:
        self.check_invariants()
        if waiter.index > len(self.waiters):
            raise RuntimeError(
                "Admission controller invariants violated: waiter index out of bounds"
            )
        if self.waiters[waiter.index] == waiter:
            self.waiters[waiter.index] = None
            if self.head == waiter.index:
                self.strip_head_nils()
            if self.tail == waiter.index + 1 % len(self.waiters):
                self.strip_tail_nils()
            if self.head == self.tail:
                self.adjust_queue_mode(time.time())
        self.check_invariants()

    def release(self) -> None:
        with self.mtx:
            self.check_invariants()
            if self.admitted_count == 0:
                raise RuntimeError(
                    "Admission controller invariants violated: double release"
                )

            self.admitted_count -= 1
            if self.admitted_count >= self.allowed:
                raise RuntimeError(
                    "Admission controller invariants violated: too many outstanding tickets"
                )

            self.possibly_release_one_from_queue()
            self.check_invariants()

    def admitted(self) -> int:
        with self.mtx:
            return self.admitted_count

    def capacity(self) -> int:
        with self.mtx:
            return self.allowed

    def resize(self) -> int:
        with self.mtx:
            return self.allowed

    def stop(self) -> None:
        with self.mtx:
            self.stopped = True

    def possibly_release_one_from_queue(self) -> None:
        self.check_invariants()
        if self.head == self.tail:
            return

        waiter: Waiter
        if self.queue_mode == self.FIFO_MODE:
            if self.waiters[self.head] is None:
                raise RuntimeError(
                    "Admission controller invariants violated: nil at head"
                )
            waiter = cast(Waiter, self.waiters[self.head])
        elif self.queue_mode == self.LIFO_MODE:
            idx = (self.tail - 1) % len(self.waiters)
            if self.waiters[idx] is None:
                raise RuntimeError(
                    "Admission controller invariants violated: nil at tail"
                )
            waiter = cast(Waiter, self.waiters[idx])

        if waiter is None:
            raise RuntimeError(
                "Admission controller invariants violated: unhandled queue mode"
            )

        self.remove_waiter_no_mtx(waiter=waiter)
        self.admitted_count += 1
        waiter.notify()
        self.check_invariants()

    def resize_waiters(self) -> None:
        self.check_invariants()
        if (self.tail + 1) % len(self.waiters) != self.head:
            raise RuntimeError(
                "Admission controller invariants violated: resize when not full"
            )

        new_size = len(self.waiters) * 2
        new_waiters: list[Optional[Waiter]] = [None] * new_size
        new_idx = 0
        for i in range(len(self.waiters)):
            idx = (self.head + i) % len(self.waiters)
            if idx == self.tail:
                break
            if self.waiters[idx] is not None:
                w: Waiter = cast(Waiter, self.waiters[idx])
                new_waiters[new_idx] = w
                self.waiters[idx] = None
                w.index = new_idx
                new_idx += 1

        self.waiters = new_waiters
        self.head = 0
        self.tail = new_idx
        self.check_invariants()

    def adjust_queue_mode(self, now: float) -> None:
        if self.head == self.tail:
            self.last_empty = now
            self.queue_mode = self.FIFO_MODE
        elif self.queue_mode == self.FIFO_MODE and self.last_empty + self.N < now:
            self.queue_mode = self.LIFO_MODE

    def strip_head_nils(self) -> None:
        while self.head != self.tail and self.waiters[self.head] is None:
            self.head = (self.head + 1) % len(self.waiters)

    def strip_tail_nils(self) -> None:
        while self.head != self.tail:
            idx = (self.tail - 1) % len(self.waiters)
            if self.waiters[idx] is not None:
                break
            self.tail = idx

    def check_invariants(self) -> None:
        for idx, w in enumerate(self.waiters):
            if w is None:
                continue
            if w.index != idx:
                raise RuntimeError(
                    "Admission controller invariants violated: waiter at wrong index"
                )

        if self.head != self.tail:
            if self.waiters[self.head] is None:
                raise RuntimeError(
                    "Admission controller invariants violated: head is nil"
                )
            size = len(self.waiters)
            tail_idx = (self.tail + size - 1) % size
            if self.waiters[tail_idx] is None:
                raise RuntimeError(
                    "Admission controller invariants violated: tail is nil"
                )


global_admitted = 0
global_denied = 0
lock = threading.Lock()
ac = AdmissionControllerImpl(parallelism=10)

NUM_REQUESTS = 100


def increment_admitted() -> None:
    global global_admitted
    with lock:
        global_admitted += 1


def increment_denied() -> None:
    global global_denied
    with lock:
        global_denied += 1


def stream_requests(n: int) -> tuple[Optional[Ticket], int]:
    delay = 0.001
    time.sleep(delay)
    ticket = ac.admit_one()
    if ticket:
        print(f"Admitted request {n}.")
        return ticket, n
    else:
        print(f"Request denied for {n}.")
        return None, n


def process_responses(
    response_queue: queue.Queue[tuple[Optional[Ticket], int]]
) -> None:
    while True:
        response = response_queue.get()
        ticket, n = response

        if ticket:
            time.sleep(0.05)
            ticket.release()
            increment_admitted()
        else:
            increment_denied()

        if n == NUM_REQUESTS - 1:
            break


def main() -> None:
    response_queue: queue.Queue[tuple[Optional[Ticket], int]] = queue.Queue()
    processing_thread = concurrent.futures.ThreadPoolExecutor(max_workers=10).submit(
        process_responses, response_queue
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for i in range(NUM_REQUESTS):
            future = executor.submit(stream_requests, i)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            response = future.result()
            response_queue.put(response)

    processing_thread.result()

    print(
        f"Admitted {global_admitted} requests. Denied {global_denied} requests. Total admitted percentage: {global_admitted / NUM_REQUESTS}"
    )


if __name__ == "__main__":
    main()
