"""暂停申请领域服务：重跑任务停工申请的状态机与跨模块协调。

核心行为：
- 发起窗口（重跑任务）申请暂停：待送达 → 送达装载班 → 待确认 → 已暂停 → 已恢复
- 送达后跨模块在装载窗口生成当班人待办（TODO-PR-xxx），值班员确认后从双方可回查的边界暂停
- 与已受理但尚未写完的窗口确认互斥：同一事实表同一时刻只有一方推进
- 两种暂停模式：收完再停（放行当前在写窗口收完）/ 暂扣确认（保护已落下步骤，暂扣后续确认）
- 恢复位置同一串值写入暂停申请、装载待办、重跑任务三处，双方看到同一恢复位置
- 换班接手不丢已完成步骤，恢复按暂停模式决定是否还要重确认在写窗口（如 W9）
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

TODO_PREFIX = "TODO-"
DEFAULT_SHIFT = "一班"
NEXT_SHIFT = "二班"
MODE_DRAIN = "收完再停"
MODE_WITHHOLD = "暂扣确认"
UNDECIDED = "待装载班决定"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class PauseRequestService(BaseModuleService):
    MODULE = "pause_request"
    ENTITY = "暂停申请"
    LIST_FIELDS = ["申请编号", "重跑编号", "影响事实表", "暂停模式", "暂停边界", "恢复位置", "当班人", "申请时间"]
    REQUIRED_FIELDS = ["申请编号", "重跑编号", "影响事实表"]
    STATUS_ORDER = ["待送达", "待确认", "已暂停", "已恢复"]
    # 暂停申请是双方回查视图，操作入口在重跑任务（发起）与装载窗口（待办）
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # ---- 基础查询 ----

    def create_entry(self, values: dict[str, Any]):
        return None, ["暂停申请需从重跑任务窗口「申请暂停」发起"]

    def run_action(self, entry_id: int, action: str):
        return None, "暂停申请为双方回查视图：请从「重跑任务」发起申请，或在「装载窗口」处理当班待办"

    def find_by_request_no(self, request_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("申请编号") == request_no:
                return row
        return None

    def active_for_task(self, task_no: str) -> dict[str, Any] | None:
        """任务维度进行中的申请：待送达/待确认/已暂停。"""
        for row in store.rows(self.MODULE):
            if row.get("重跑编号") == task_no and row.get("status") in ("待送达", "待确认", "已暂停"):
                return row
        return None

    def active_for_table(self, table: str) -> dict[str, Any] | None:
        """事实表维度已送达的申请：待确认/已暂停（待送达尚未交给装载班，不影响窗口）。"""
        for row in store.rows(self.MODULE):
            if row.get("影响事实表") == table and row.get("status") in ("待确认", "已暂停"):
                return row
        return None

    def find_task(self, task_no: str) -> dict[str, Any] | None:
        for row in store.rows("reprocess_task"):
            if row.get("重跑编号") == task_no:
                return row
        return None

    def find_todo(self, request_no: str) -> dict[str, Any] | None:
        for row in store.rows("load_window"):
            if row.get("窗口编号") == TODO_PREFIX + request_no:
                return row
        return None

    def find_window(self, window_no: str) -> dict[str, Any] | None:
        for row in store.rows("load_window"):
            if row.get("窗口编号") == window_no:
                return row
        return None

    def inflight_windows(self, table: str) -> list[dict[str, Any]]:
        """已受理但尚未写完的窗口（装载中），暂停申请要与之起冲突。"""
        return [
            row for row in store.rows("load_window")
            if row.get("所属事实表") == table
            and not str(row.get("窗口编号", "")).startswith(TODO_PREFIX)
            and row.get("status") == "装载中"
        ]

    # ---- 发起窗口（重跑任务）入口 ----

    def create_request(self, task: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        """申请暂停：仅重跑中任务可发起，同一任务同时只能有一个进行中的申请。"""
        if task.get("status") != "重跑中":
            return False, f"仅「重跑中」任务可申请暂停，当前状态：{task.get('status')}", None
        task_no = str(task.get("重跑编号", ""))
        if self.active_for_task(task_no):
            return False, f"重跑任务 {task_no} 已有进行中的暂停申请，不得重复发起", None
        rows = store.rows(self.MODULE)
        seq = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        request_no = f"PR-{seq:03d}"
        entry = {
            "id": seq,
            "申请编号": request_no,
            "重跑编号": task_no,
            "影响事实表": task.get("影响事实表", ""),
            "暂停模式": UNDECIDED,
            "暂停边界": "",
            "恢复位置": "",
            "当班人": "",
            "申请时间": _now(),
            "status": "待送达", "pending": True, "abnormal": False,
            # 回查字段：确认/恢复时间、放行与待重确认窗口、实做恢复的当班人
            "确认时间": "", "恢复时间": "",
            "放行窗口": "", "待重确认窗口": "", "恢复当班": "",
        }
        rows.append(entry)
        task["status"] = "暂停申请中"
        return True, f"暂停申请 {request_no} 已登记（待送达装载班）", entry

    def deliver(self, task: dict[str, Any]) -> tuple[bool, str]:
        """送达装载班：跨模块在装载窗口生成当班人待办，申请进入待确认。"""
        if task.get("status") != "暂停申请中":
            return False, f"任务当前状态 {task.get('status')}，没有待送达的暂停申请"
        req = self.active_for_task(str(task.get("重跑编号", "")))
        if req is None or req.get("status") != "待送达":
            return False, "没有待送达的暂停申请"
        req["status"] = "待确认"
        req["当班人"] = DEFAULT_SHIFT
        windows = store.rows("load_window")
        todo_no = TODO_PREFIX + str(req["申请编号"])
        todo = {
            "id": max((int(r.get("id", 0)) for r in windows), default=0) + 1,
            "窗口编号": todo_no,
            "所属事实表": req.get("影响事实表", ""),
            "窗口起点": "", "窗口终点": "",
            "预期行数": "", "实际行数": "",
            "装载水位": "", "装载耗时": "",
            "status": "暂停待办", "pending": True, "abnormal": False,
            "当班人": DEFAULT_SHIFT,
            "申请编号": req.get("申请编号", ""),
            "重跑编号": req.get("重跑编号", ""),
            "暂停模式": UNDECIDED,
        }
        windows.append(todo)
        inflight = self.inflight_windows(str(req.get("影响事实表", "")))
        if inflight:
            conflict = "、".join(str(w.get("窗口编号", "")) for w in inflight)
            return True, (
                f"暂停申请 {req['申请编号']} 已送达装载窗口，生成当班人（{DEFAULT_SHIFT}）待办 {todo_no}；"
                f"与在写窗口 {conflict} 的确认冲突，需装载班决定收完再停或暂扣确认"
            )
        return True, (
            f"暂停申请 {req['申请编号']} 已送达装载窗口，生成当班人（{DEFAULT_SHIFT}）待办 {todo_no}；"
            "表内无在写窗口，值班员确认后即停"
        )

    # ---- 装载窗口（当班待办）入口 ----

    def confirm_pause(self, todo: dict[str, Any], mode: str) -> tuple[bool, str]:
        """值班员确认暂停：收完再停放行当前窗口，暂扣确认保护已落下步骤。"""
        if not str(todo.get("窗口编号", "")).startswith(TODO_PREFIX):
            return False, f"{todo.get('窗口编号')} 不是暂停待办，确认暂停请在装载窗口的待办行上操作"
        req = self.find_by_request_no(str(todo.get("申请编号", "")))
        if req is None:
            return False, "待办缺少关联的暂停申请"
        if req.get("status") != "待确认" or todo.get("status") != "暂停待办":
            return False, f"暂停申请 {req.get('申请编号')} 当前状态 {req.get('status')}，不能重复决定"
        if req.get("放行窗口"):
            return False, f"已决定收完再停并放行 {req['放行窗口']}，等待其收完，不能改判"

        table = str(req.get("影响事实表", ""))
        req["暂停模式"] = mode
        req["当班人"] = todo.get("当班人", DEFAULT_SHIFT)
        req["确认时间"] = _now()
        todo["暂停模式"] = mode

        inflight = self.inflight_windows(table)
        if not inflight:
            boundary = self._watermark_boundary(table)
            self._settle(req, boundary)
            return True, f"表内无在写窗口，暂停直接生效，边界：{boundary}"

        window = inflight[0]
        wid = str(window.get("窗口编号", ""))
        if mode == MODE_DRAIN:
            req["放行窗口"] = wid
            note = f"{MODE_DRAIN}（等待{wid}收完）"
            req["暂停模式"] = note
            todo["暂停模式"] = note
            return True, (
                f"已决定收完再停：放行在写窗口 {wid} 的确认，{wid} 收完后自动停住；"
                "期间其他窗口确认与暂停并发时暂停方优先，一律暂扣"
            )
        # 暂扣确认：保护已落下步骤，暂扣后续确认
        window["status"] = "暂扣中"
        window["pending"] = True
        req["待重确认窗口"] = wid
        boundary = f"{wid}（需重确认）"
        self._settle(req, boundary)
        return True, (
            f"已暂扣 {wid} 的后续确认，已落下步骤受保护；暂停生效，"
            f"恢复时需先重确认 {wid}"
        )

    def handover(self, todo: dict[str, Any]) -> tuple[bool, str]:
        """换班接手：二班接管待办，恢复位置与已完成步骤保持不变。"""
        if not str(todo.get("窗口编号", "")).startswith(TODO_PREFIX):
            return False, f"{todo.get('窗口编号')} 不是暂停待办"
        req = self.find_by_request_no(str(todo.get("申请编号", "")))
        if req is None or req.get("status") != "已暂停" or todo.get("status") != "已暂停":
            return False, "仅已暂停的待办可换班接手"
        if todo.get("当班人") == NEXT_SHIFT:
            return False, f"当前已是{NEXT_SHIFT}当班，无需重复接手"
        todo["当班人"] = NEXT_SHIFT
        req["当班人"] = NEXT_SHIFT
        return True, f"{NEXT_SHIFT}已接手 {req['申请编号']}，恢复位置不变：{req.get('恢复位置')}"

    def resume(self, todo: dict[str, Any]) -> tuple[bool, str]:
        """确认恢复：按暂停模式校验是否还需重确认在写窗口，已完成步骤不丢不补做。"""
        if not str(todo.get("窗口编号", "")).startswith(TODO_PREFIX):
            return False, f"{todo.get('窗口编号')} 不是暂停待办"
        req = self.find_by_request_no(str(todo.get("申请编号", "")))
        if req is None or req.get("status") != "已暂停" or todo.get("status") != "已暂停":
            return False, "仅已暂停的申请可确认恢复"
        held = str(req.get("待重确认窗口", ""))
        if held:
            window = self.find_window(held)
            if window is not None and window.get("status") == "暂扣中":
                return False, f"恢复前需先重确认窗口 {held}：请对该窗口执行「确认装载」"
            req["待重确认窗口"] = ""
        req["status"] = "已恢复"
        req["pending"] = False
        req["恢复时间"] = _now()
        req["恢复当班"] = todo.get("当班人", "")
        todo["status"] = "已恢复"
        todo["pending"] = False
        task = self.find_task(str(req.get("重跑编号", "")))
        if task is not None:
            task["status"] = "重跑中"
        return True, (
            f"暂停申请 {req['申请编号']} 已恢复（{req['恢复当班']}实做），重跑任务回到重跑中，"
            f"恢复位置：{req.get('恢复位置')}；已完成步骤保持原样，不因换人补做"
        )

    # ---- 装载窗口（窗口动作）互斥守卫与联动 ----

    def guard_window_action(self, window: dict[str, Any], action: str) -> tuple[bool, str]:
        """窗口确认与暂停并发时只能有一方推进：暂停待决/已暂停期间暂扣窗口动作。"""
        req = self.active_for_table(str(window.get("所属事实表", "")))
        if req is None:
            return True, ""
        wid = str(window.get("窗口编号", ""))
        if action == "确认装载":
            if req.get("status") == "待确认":
                if req.get("放行窗口") and req.get("放行窗口") == wid:
                    return True, ""
                return False, (
                    f"暂停申请 {req['申请编号']} 待装载班决定，窗口确认与暂停并发时暂停方优先，"
                    f"{wid} 的确认被暂扣"
                )
            if window.get("status") == "暂扣中" and req.get("待重确认窗口") == wid:
                return True, ""
            return False, (
                f"任务已暂停（{req['申请编号']}），{wid} 的确认被暂扣，"
                f"恢复位置：{req.get('恢复位置')}"
            )
        if action == "启动装载":
            return False, f"暂停流程进行中（{req['申请编号']}，{req.get('status')}），不得启动新窗口"
        if action == "标记中断":
            if window.get("status") == "暂扣中":
                return False, f"{wid} 处于暂扣保护中，已完成步骤不得中断丢弃"
            return True, ""
        return True, ""

    def after_window_action(self, window: dict[str, Any], action: str) -> None:
        """窗口动作落地后的联动：放行窗口收完则暂停生效；待重确认窗口确认后解除暂扣。"""
        req = self.active_for_table(str(window.get("所属事实表", "")))
        if req is None:
            return
        wid = str(window.get("窗口编号", ""))
        if action == "确认装载":
            if not window.get("装载水位"):
                window["装载水位"] = window.get("窗口终点", "")
            if req.get("status") == "待确认" and req.get("放行窗口") == wid:
                end = window.get("窗口终点") or window.get("装载水位")
                self._settle(req, f"{wid}之后（水位{end}）")
            elif req.get("status") == "已暂停" and req.get("待重确认窗口") == wid:
                req["待重确认窗口"] = ""
        elif action == "标记中断":
            if req.get("status") == "待确认" and req.get("放行窗口") == wid:
                self._settle(req, f"{wid}（已中断，恢复时重新装载）")

    # ---- 内部 ----

    def _watermark_boundary(self, table: str) -> str:
        loaded = [
            row for row in store.rows("load_window")
            if row.get("所属事实表") == table
            and not str(row.get("窗口编号", "")).startswith(TODO_PREFIX)
            and row.get("status") == "已装载"
        ]
        if loaded:
            end = loaded[-1].get("窗口终点") or loaded[-1].get("装载水位")
            return f"水位{end}之后"
        return "表内暂无已装载窗口"

    def _settle(self, req: dict[str, Any], boundary: str) -> None:
        """暂停生效：同一恢复位置写入申请、待办、任务三处，双方可回查。"""
        mode = str(req.get("暂停模式", ""))
        if mode.startswith(MODE_DRAIN):
            mode = MODE_DRAIN
            req["暂停模式"] = mode
        req["status"] = "已暂停"
        req["暂停边界"] = boundary
        req["恢复位置"] = boundary
        todo = self.find_todo(str(req.get("申请编号", "")))
        if todo is not None:
            todo["status"] = "已暂停"
            todo["装载水位"] = boundary
            todo["暂停模式"] = mode
        task = self.find_task(str(req.get("重跑编号", "")))
        if task is not None:
            task["status"] = "已暂停"
            task["重跑水位"] = boundary
