#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Queue, Empty

CHUNK_SIZE = 1024 * 1024  # 1 MiB
STOP_EVENT = threading.Event()
ACTIVE_PROCESSES = set()
PROCESS_LOCK = threading.Lock()


def request_stop():
    STOP_EVENT.set()
    with PROCESS_LOCK:
        processes = list(ACTIVE_PROCESSES)
    for proc in processes:
        try:
            proc.terminate()
        except Exception:
            pass


def classify_error(exc):
    win = getattr(exc, "winerror", None)
    eno = getattr(exc, "errno", None)
    code = f"WINERROR_{win}" if win is not None else (f"ERRNO_{eno}" if eno is not None else type(exc).__name__)
    mapping = {
        5: ("PERMISSION_DENIED", "Access denied; check permissions or file locks."),
        21: ("DEVICE_NOT_READY", "Drive is not ready; check card, reader, and connection."),
        23: ("CRC_ERROR", "Cyclic redundancy check error; possible media or data-path fault."),
        32: ("FILE_IN_USE", "Another process is using or locking the file."),
        1117: ("DEVICE_IO_ERROR", "Windows I/O device error; possible card, reader, or connection fault."),
        112: ("DESTINATION_FULL", "Destination disk is full."),
        28: ("DESTINATION_FULL", "Destination filesystem has no space left."),
        2: ("FILE_NOT_FOUND", "File disappeared or path changed during operation."),
        3: ("PATH_NOT_FOUND", "Parent folder or drive path is unavailable."),
    }
    status, explanation = mapping.get(win, mapping.get(eno, (
        ("PERMISSION_DENIED", "Access denied by the operating system.") if isinstance(exc, PermissionError)
        else ("FILE_NOT_FOUND", "File or path no longer exists.") if isinstance(exc, FileNotFoundError)
        else ("OS_READ_ERROR", "Operating system returned an I/O error; inspect native details.") if isinstance(exc, OSError)
        else ("WORKER_ERROR", "Worker failed; inspect native details.")
    )))
    return status, code, explanation


def _hash_file(path: Path) -> dict:
    """Worker: read a file in chunks and return size + SHA-256."""
    started = time.monotonic()
    digest = hashlib.sha256()
    total = 0
    with path.open("rb", buffering=0) as handle:
        while True:
            block = handle.read(CHUNK_SIZE)
            if not block:
                break
            digest.update(block)
            total += len(block)
    return {"ok": True, "size": total, "sha256": digest.hexdigest(),
            "read_seconds": round(time.monotonic() - started, 4)}


def _copy_and_hash(src: Path, temp_dst: Path) -> dict:
    """Worker: copy source to a new temporary file while hashing source bytes."""
    started = time.monotonic()
    digest = hashlib.sha256()
    total = 0
    temp_dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb", buffering=0) as reader, temp_dst.open("xb", buffering=0) as writer:
        while True:
            block = reader.read(CHUNK_SIZE)
            if not block:
                break
            writer.write(block)
            digest.update(block)
            total += len(block)
        writer.flush()
        os.fsync(writer.fileno())
    return {"ok": True, "size": total, "sha256": digest.hexdigest(),
            "read_seconds": round(time.monotonic() - started, 4)}


def _worker_main(args: list[str]) -> int:
    try:
        if args[0] == "--_worker-read":
            result = _hash_file(Path(args[1]))
        elif args[0] == "--_worker-copy":
            result = _copy_and_hash(Path(args[1]), Path(args[2]))
        else:
            return 2
        print(json.dumps(result))
        return 0
    except Exception as exc:
        status, code, explanation = classify_error(exc)
        print(json.dumps({"ok": False, "status": status, "error_code": code,
                          "explanation": explanation, "error": f"{type(exc).__name__}: {exc}"}))
        return 1


def _run_worker(worker_args: list[str], timeout: float) -> dict:
    if STOP_EVENT.is_set():
        return {"ok": False, "status": "CANCELLED", "error": "Stopped by user"}
    command = [sys.executable, str(Path(__file__).resolve()), *worker_args]
    proc = None
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        with PROCESS_LOCK:
            ACTIVE_PROCESSES.add(proc)
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Never wait indefinitely for a timed-out storage operation to exit.
            try:
                proc.kill()
            except OSError:
                pass
            try:
                stdout, stderr = proc.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                # Close our pipe handles and return control to the supervisor.
                for pipe in (proc.stdout, proc.stderr):
                    try:
                        if pipe:
                            pipe.close()
                    except OSError:
                        pass
                stdout, stderr = "", "Worker did not exit within 2 seconds after termination."
            return {"ok": False, "status": "READ_TIMEOUT", "error_code": "OPERATION_TIMEOUT",
                    "explanation": f"Worker exceeded {timeout:g}s. It was terminated; possible causes include slow/busy media, reader retries, a stuck connection, or a large file.",
                    "error": (stderr or "").strip()[:1000]}
        if STOP_EVENT.is_set():
            return {"ok": False, "status": "CANCELLED", "error": "Force stop requested"}
        try:
            result = json.loads(stdout.strip().splitlines()[-1])
        except Exception:
            detail = (stderr or "").strip() or (stdout or "").strip() or f"Worker exited {proc.returncode}"
            return {"ok": False, "status": "WORKER_ERROR", "error_code": "INVALID_WORKER_RESULT",
                    "explanation": "Worker exited without a valid result.", "error": detail[:1000]}
        if not result.get("ok"):
            result.setdefault("status", "OS_READ_ERROR")
        return result
    except Exception as exc:
        status, code, explanation = classify_error(exc)
        return {"ok": False, "status": status, "error_code": code, "explanation": explanation,
                "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if proc is not None:
            with PROCESS_LOCK:
                ACTIVE_PROCESSES.discard(proc)


def validate_roots(source: Path, destination: Path | None = None) -> Path:
    source = source.expanduser().resolve()
    if not source.exists() or not source.is_dir():
        raise ValueError(f"Source folder/drive does not exist: {source}")
    if destination is not None:
        destination = destination.expanduser().resolve()
        if destination == source or source in destination.parents:
            raise ValueError("Destination cannot be the source itself or a folder inside the source.")
    return source


def collect_files(source: Path, log=print) -> tuple[list[Path], list[dict]]:
    files: list[Path] = []
    walk_errors: list[dict] = []

    def onerror(exc):
        item = {"source": getattr(exc, "filename", "") or str(source),
                "status": "FOLDER_ERROR", "details": str(exc)}
        walk_errors.append(item)
        log(f"[FOLDER ERROR] {item['source']}: {exc}")

    for root, dirs, filenames in os.walk(source, onerror=onerror, followlinks=False):
        # Do not descend into directory symlinks.
        dirs[:] = [d for d in dirs if not (Path(root) / d).is_symlink()]
        for filename in filenames:
            path = Path(root) / filename
            try:
                if path.is_symlink():
                    log(f"[SKIP] Symbolic link: {path}")
                    continue
                if path.is_file():
                    files.append(path)
            except OSError as exc:
                walk_errors.append({"source": str(path), "status": "STAT_ERROR", "details": str(exc)})
    return files, walk_errors


def write_report(report_path: Path, rows: list[dict]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["source", "destination", "status", "error_code", "size_bytes", "seconds", "read_speed_mbps", "details"]
    with report_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _scan_row(path, result, elapsed):
    status = "READABLE" if result.get("ok") else result.get("status", "OS_READ_ERROR")
    io_seconds = result.get("read_seconds", elapsed)
    # Avoid flagging tiny files or process-startup overhead as slow media.
    if result.get("ok") and result.get("size", 0) >= 1024 * 1024 and io_seconds > 0:
        speed_check = result["size"] / (1024 * 1024) / io_seconds
        if speed_check < 0.5:
            status = "SLOW_READ"
    details = result.get("explanation", "")
    if result.get("error"):
        details = (details + " " + result["error"]).strip()
    size = result.get("size", "")
    speed = round((size / (1024 * 1024)) / io_seconds, 3) if size and io_seconds > 0 else ""
    return {"source": str(path), "destination": "", "status": status,
            "error_code": result.get("error_code", ""), "size_bytes": size,
            "seconds": round(elapsed, 2), "read_speed_mbps": speed, "details": details}


def scan(source: Path, timeout: float, report: Path, log=print, progress=None, workers: int = 2) -> list[dict]:
    source = validate_roots(source)
    STOP_EVENT.clear()
    files, rows = collect_files(source, log)
    total = len(files)
    log(f"Found {total} files. Parallel scan using {workers} worker(s).")

    def test(path):
        begin = time.monotonic()
        result = _run_worker(["--_worker-read", str(path)], timeout)
        return path, result, time.monotonic() - begin

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(test, path): path for path in files}
        for future in as_completed(futures):
            if STOP_EVENT.is_set():
                for f in futures:
                    f.cancel()
                break
            path, result, elapsed = future.result()
            row = _scan_row(path, result, elapsed)
            rows.append(row)
            completed += 1
            log(f"[{completed}/{total}] {row['status']}: {path} ({elapsed:.1f}s)")
            if progress:
                progress(completed, total, row["status"], path)
    if STOP_EVENT.is_set():
        rows.append({"source": str(source), "status": "SCAN_STOPPED", "details": "Force stop requested; completed results only."})
    write_report(report, rows)
    log(f"Scan report saved: {report}")
    return rows


def move_files(source: Path, destination: Path, timeout: float, report: Path,
               log=print, progress=None, workers: int = 2) -> list[dict]:
    source = validate_roots(source, destination)
    destination = destination.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    STOP_EVENT.clear()
    files, rows = collect_files(source, log)
    total = len(files)
    log(f"Found {total} files. Parallel verified move using {workers} worker(s).")

    def move_one(src):
        begin = time.monotonic()
        relative = src.relative_to(source)
        target = destination / relative
        try:
            if STOP_EVENT.is_set():
                result = {"status": "CANCELLED", "details": "Not started; force stop requested."}
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    result = {"status": "DESTINATION_EXISTS", "details": "Not overwritten; source retained."}
                else:
                    temp = target.with_name(target.name + f".sdmove-{uuid.uuid4().hex}.tmp")
                    copied = _run_worker(["--_worker-copy", str(src), str(temp)], timeout)
                    if not copied.get("ok"):
                        result = {"status": copied.get("status", "COPY_ERROR"),
                                  "error_code": copied.get("error_code", ""),
                                  "details": copied.get("explanation", copied.get("error", "Copy failed; source retained."))}
                        try: temp.unlink(missing_ok=True)
                        except OSError: pass
                    elif STOP_EVENT.is_set():
                        result = {"status": "CANCELLED", "details": "Stopped after copy; source retained."}
                        try: temp.unlink(missing_ok=True)
                        except OSError: pass
                    else:
                        verified = _run_worker(["--_worker-read", str(temp)], timeout)
                        if not verified.get("ok") or verified.get("sha256") != copied.get("sha256"):
                            result = {"status": "VERIFY_FAILED", "error_code": verified.get("error_code", "SHA256_MISMATCH"),
                                      "details": verified.get("explanation", "Checksum mismatch; source retained.")}
                            try: temp.unlink(missing_ok=True)
                            except OSError: pass
                        elif STOP_EVENT.is_set():
                            result = {"status": "CANCELLED", "details": "Stopped after verification; source retained."}
                            try: temp.unlink(missing_ok=True)
                            except OSError: pass
                        else:
                            os.replace(temp, target)
                            try:
                                st = src.stat()
                                os.chmod(target, st.st_mode)
                                os.utime(target, (st.st_atime, st.st_mtime))
                            except OSError:
                                pass
                            try:
                                src.unlink()
                                result = {"status": "MOVED_VERIFIED", "details": "SHA-256 verified; source removed."}
                            except OSError as exc:
                                status, code, explanation = classify_error(exc)
                                result = {"status": "COPIED_VERIFIED_SOURCE_RETAINED", "error_code": code,
                                          "details": f"Source deletion failed ({status}): {exc}. {explanation}"}
        except Exception as exc:
            status, code, explanation = classify_error(exc)
            result = {"status": status, "error_code": code, "details": f"{exc}. {explanation}"}
        return src, result, time.monotonic() - begin

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(move_one, path): path for path in files}
        for future in as_completed(futures):
            if STOP_EVENT.is_set():
                for f in futures:
                    f.cancel()
                break
            src, result, elapsed = future.result()
            rows.append({"source": str(src), "destination": str(destination / src.relative_to(source)),
                         "status": result.get("status", "ERROR"), "error_code": result.get("error_code", ""),
                         "size_bytes": result.get("size_bytes", ""), "seconds": round(elapsed, 2),
                         "read_speed_mbps": result.get("read_speed_mbps", ""), "details": result.get("details", "")})
            completed += 1
            log(f"[{completed}/{total}] {result.get('status')}: {src} ({elapsed:.1f}s)")
            if progress:
                progress(completed, total, result.get("status", "ERROR"), src)
    if STOP_EVENT.is_set():
        rows.append({"source": str(source), "destination": str(destination), "status": "MOVE_STOPPED",
                     "details": "Force stop requested. Unverified source files are retained."})
    write_report(report, rows)
    log(f"Transfer report saved: {report}")
    return rows


def cli_main() -> int:
    parser = argparse.ArgumentParser(
        description="Safely diagnose an SD card and move readable files to another disk."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    scan_parser = sub.add_parser("scan", help="Read-test every file and create a CSV report.")
    scan_parser.add_argument("source", type=Path, help="SD card drive/folder, e.g. E:\\")
    scan_parser.add_argument("--timeout", type=float, default=20, help="Seconds allowed per file (default: 20).")
    scan_parser.add_argument("--report", type=Path, default=Path("sd_scan_report.csv"))
    scan_parser.add_argument("--workers", type=int, default=2)
    move_parser = sub.add_parser("move", help="Move readable files after SHA-256 verification.")
    move_parser.add_argument("source", type=Path, help="SD card drive/folder, e.g. E:\\")
    move_parser.add_argument("destination", type=Path, help="Destination drive/folder, e.g. D:\\SD_Backup")
    move_parser.add_argument("--timeout", type=float, default=30, help="Seconds allowed per file operation (default: 30).")
    move_parser.add_argument("--report", type=Path, default=Path("sd_move_report.csv"))
    move_parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()

    try:
        if args.command == "scan":
            rows = scan(args.source, args.timeout, args.report, workers=max(1, min(16, args.workers)))
        else:
            rows = move_files(args.source, args.destination, args.timeout, args.report, workers=max(1, min(16, args.workers)))
        counts = {}
        for row in rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        print("\nSummary:")
        for status, count in sorted(counts.items()):
            print(f"  {status}: {count}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


class App:
    def __init__(self, root):
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk

        self.tk = tk
        self.filedialog = filedialog
        self.messagebox = messagebox
        self.ttk = ttk
        self.root = root
        self.root.title("Smart Move Tool")
        self.root.geometry("820x590")
        self.events = Queue()
        self.busy = False

        self.source_var = tk.StringVar()
        self.dest_var = tk.StringVar()
        self.timeout_var = tk.StringVar(value="30")
        self.workers_var = tk.StringVar(value="2")
        self.status_var = tk.StringVar(value="Ready")

        frame = ttk.Frame(root, padding=12)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="SD card / source folder:").grid(row=0, column=0, sticky="w", pady=5)
        ttk.Entry(frame, textvariable=self.source_var).grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(frame, text="Browse", command=self.pick_source).grid(row=0, column=2)
        ttk.Label(frame, text="Destination disk / folder:").grid(row=1, column=0, sticky="w", pady=5)
        ttk.Entry(frame, textvariable=self.dest_var).grid(row=1, column=1, sticky="ew", padx=6)
        ttk.Button(frame, text="Browse", command=self.pick_dest).grid(row=1, column=2)
        ttk.Label(frame, text="Per-file timeout (seconds):").grid(row=2, column=0, sticky="w", pady=5)
        ttk.Entry(frame, textvariable=self.timeout_var, width=10).grid(row=2, column=1, sticky="w", padx=6)

        ttk.Label(frame, text="Parallel workers (1-16):").grid(row=3, column=0, sticky="w", pady=5)
        ttk.Entry(frame, textvariable=self.workers_var, width=10).grid(row=3, column=1, sticky="w", padx=6)
        buttons = ttk.Frame(frame)
        buttons.grid(row=4, column=0, columnspan=3, sticky="w", pady=10)
        self.scan_btn = ttk.Button(buttons, text="Scan SD Card", command=self.start_scan)
        self.scan_btn.pack(side="left", padx=(0, 8))
        self.move_btn = ttk.Button(buttons, text="Move Verified Files", command=self.start_move)
        self.move_btn.pack(side="left", padx=(0, 8))
        ttk.Button(buttons, text="Open Report Folder", command=self.open_report_folder).pack(side="left", padx=(0, 8))
        self.stop_btn = ttk.Button(buttons, text="FORCE STOP", command=self.force_stop, state="disabled")
        self.stop_btn.pack(side="left")
        self.progress = ttk.Progressbar(frame, mode="determinate")
        self.progress.grid(row=5, column=0, columnspan=3, sticky="ew", pady=6)
        ttk.Label(frame, textvariable=self.status_var).grid(row=6, column=0, columnspan=3, sticky="w")
        ttk.Label(frame, text="Activity log:").grid(row=7, column=0, columnspan=3, sticky="w", pady=(10, 3))
        self.log_box = tk.Text(frame, height=19, wrap="word", state="disabled")
        self.log_box.grid(row=8, column=0, columnspan=3, sticky="nsew")
        frame.rowconfigure(8, weight=1)
        ttk.Label(
            frame,
            text="Safety: files are deleted from the SD card only after the destination copy passes SHA-256 verification. Existing destination files are never overwritten.",
            wraplength=780,
        ).grid(row=9, column=0, columnspan=3, sticky="w", pady=(10, 0))
        self.root.after(100, self.poll_events)

    def log(self, message):
        self.events.put(("log", message))

    def pick_source(self):
        path = self.filedialog.askdirectory(title="Select SD card root")
        if path:
            self.source_var.set(path)

    def pick_dest(self):
        path = self.filedialog.askdirectory(title="Select destination disk/folder")
        if path:
            self.dest_var.set(path)

    def open_report_folder(self):
        path = Path(self.dest_var.get().strip() or self.source_var.get().strip() or ".").expanduser()
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            self.messagebox.showerror("Could not open folder", str(exc))

    def begin(self, mode):
        if self.busy:
            return
        source_text = self.source_var.get().strip()
        dest_text = self.dest_var.get().strip()
        try:
            timeout = float(self.timeout_var.get())
            workers = int(self.workers_var.get())
            if timeout <= 0 or workers < 1 or workers > 16:
                raise ValueError("Timeout must be positive and workers must be between 1 and 16.")
            if not source_text:
                raise ValueError("Select the SD card/source folder.")
            source = Path(source_text)
            validate_roots(source, Path(dest_text) if mode == "move" and dest_text else None)
            if mode == "move" and not dest_text:
                raise ValueError("Select a destination disk/folder.")
            if mode == "move":
                if not self.messagebox.askyesno(
                    "Confirm move",
                    "Readable files will be moved. Source files are deleted only after SHA-256 verification. Failed files stay on the SD card. Continue?"
                ):
                    return
        except ValueError as exc:
            self.messagebox.showerror("Check selections", str(exc) or "Timeout must be greater than zero.")
            return

        STOP_EVENT.clear()
        self.busy = True
        self.scan_btn.configure(state="disabled")
        self.move_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.progress.configure(value=0, maximum=100)
        # Keep scan reports off the SD card; transfer reports go to the destination disk.
        report = (
            Path(dest_text) / "sd_move_report.csv"
            if mode == "move"
            else Path.home() / "sd_scan_report.csv"
        )

        def progress(index, total, status, path):
            self.events.put(("progress", (index, total, status, str(path))))

        def task():
            try:
                if mode == "scan":
                    rows = scan(source, timeout, report, self.log, progress, workers)
                else:
                    rows = move_files(source, Path(dest_text), timeout, report, self.log, progress, workers)
                counts = {}
                for row in rows:
                    counts[row["status"]] = counts.get(row["status"], 0) + 1
                self.events.put(("done", (counts, str(report))))
            except Exception as exc:
                self.events.put(("fatal", str(exc)))

        threading.Thread(target=task, daemon=True).start()

    def start_scan(self):
        self.begin("scan")

    def start_move(self):
        self.begin("move")

    def force_stop(self):
        if self.busy:
            self.status_var.set("Force stop requested; terminating active workers...")
            self.log("FORCE STOP requested by user.")
            request_stop()
            self.stop_btn.configure(state="disabled")

    def poll_events(self):
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "log":
                    self.log_box.configure(state="normal")
                    self.log_box.insert("end", payload + "\n")
                    self.log_box.see("end")
                    self.log_box.configure(state="disabled")
                elif kind == "progress":
                    index, total, status, path = payload
                    self.progress.configure(maximum=max(total, 1), value=index)
                    self.status_var.set(f"{index}/{total} — {status}: {path}")
                elif kind == "done":
                    counts, report = payload
                    self.busy = False
                    self.scan_btn.configure(state="normal")
                    self.move_btn.configure(state="normal")
                    self.stop_btn.configure(state="disabled")
                    self.status_var.set(f"Finished. Report: {report}")
                    summary = "\n".join(f"{k}: {v}" for k, v in sorted(counts.items())) or "No files found."
                    self.messagebox.showinfo("Finished", f"{summary}\n\nReport: {report}")
                elif kind == "fatal":
                    self.busy = False
                    self.scan_btn.configure(state="normal")
                    self.move_btn.configure(state="normal")
                    self.stop_btn.configure(state="disabled")
                    self.status_var.set("Stopped with an error.")
                    self.messagebox.showerror("Operation failed", payload)
        except Empty:
            pass
        self.root.after(100, self.poll_events)


def main():
    if len(sys.argv) > 1 and sys.argv[1].startswith("--_worker-"):
        raise SystemExit(_worker_main(sys.argv[1:]))
    if len(sys.argv) > 1:
        raise SystemExit(cli_main())
    try:
        import tkinter as tk
    except ImportError:
        print("Tkinter is not available. Run with CLI arguments; see --help.", file=sys.stderr)
        raise SystemExit(2)
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
