import asyncio
import os
import time
from pathlib import Path

import httpx
from croniter import croniter
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

SESSDATA = os.getenv("SESSDATA", "")
BILI_JCT = os.getenv("BILI_JCT", "")
MIN_LEVEL = 21
SWITCH_INTERVAL = 5  # 切换间隔（秒）
CRON_EXPRESSION = "0 * * * *"  # 刷新表达式，默认每整点

API_MY_MEDALS = "https://api.live.bilibili.com/xlive/app-ucenter/v1/user/GetMyMedals"
WEAR_MEDAL_URL = "https://api.live.bilibili.com/xlive/app-ucenter/v1/fansMedal/wear"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


async def get_medals(client: httpx.AsyncClient) -> list[dict]:
    """分页获取所有粉丝勋章。"""
    all_items = []
    page = 1
    while True:
        for attempt in range(3):
            try:
                resp = await client.get(API_MY_MEDALS, params={"page": page, "page_size": 10})
                data = resp.json()
                break
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                else:
                    raise RuntimeError(f"获取勋章失败: {e}")
        if data.get("code") != 0:
            raise RuntimeError(f"获取勋章失败: {data}")
        d = data.get("data", {})
        items = d.get("items", [])
        all_items.extend(items)
        page_info = d.get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1
    return all_items


async def wear_medal(client: httpx.AsyncClient, medal_id: int) -> bool:
    """佩戴指定勋章。"""
    for attempt in range(3):
        try:
            resp = await client.post(WEAR_MEDAL_URL, data={"medal_id": medal_id, "csrf": BILI_JCT})
            data = resp.json()
            if data["code"] != 0:
                print(f"  佩戴勋章 {medal_id} 失败: {data.get('message', data)}")
                return False
            return True
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                print(f"  佩戴勋章 {medal_id} 异常: {e}")
                return False


async def main():
    if not SESSDATA or not BILI_JCT:
        raise ValueError("请先设置 SESSDATA 和 BILI_JCT")
    cookie = f"SESSDATA={SESSDATA}; bili_jct={BILI_JCT}"
    headers = {**HEADERS, "Cookie": cookie}

    async with httpx.AsyncClient(headers=headers, timeout=15) as client:
        medals = await get_medals(client)

        qualified = [m for m in medals if m.get("level", 0) >= MIN_LEVEL]
        if not qualified:
            print(f"未找到 >= {MIN_LEVEL} 级勋章（共 {len(medals)} 个）")
            return

        lit = [m for m in qualified if m.get("is_lighted", 0)]
        unlit = [m for m in qualified if not m.get("is_lighted", 0)]

        print(f"找到 {len(qualified)} 个 >= {MIN_LEVEL} 级勋章（{len(lit)} 点亮, {len(unlit)} 熄灭）:")
        for m in qualified:
            status = "点亮" if m.get("is_lighted", 0) else "熄灭"
            print(f"  [{m['medal_id']}] {m['target_name']} Lv.{m['level']} {status}")

        if not lit:
            print("无 >= 21 级点亮勋章")
            return

        cron = croniter(CRON_EXPRESSION)
        next_refresh = cron.get_next(float)
        refresh_task = None
        idx = 0
        print(f"\n每 {SWITCH_INTERVAL} 秒轮换 {len(lit)} 个点亮勋章, cron: {CRON_EXPRESSION}... (Ctrl+C 停止)\n")

        while True:
            # 定时触发后台刷新
            if time.time() >= next_refresh and refresh_task is None:
                ts = time.strftime("%H:%M:%S")
                print(f"[{ts}] 后台刷新勋章列表...")

                async def _refresh():
                    nonlocal lit, idx
                    medals = await get_medals(client)
                    new_lit = [m for m in medals if m.get("level", 0) >= MIN_LEVEL and m.get("is_lighted", 0)]
                    lit = new_lit
                    idx = 0
                    ts2 = time.strftime("%H:%M:%S")
                    print(f"[{ts2}] 刷新完成: {len(lit)} 个点亮勋章, 下次刷新 {time.strftime('%H:%M:%S', time.localtime(next_refresh))}")

                refresh_task = asyncio.create_task(_refresh())
                next_refresh = cron.get_next(float)

            # 后台任务完成则清理
            if refresh_task and refresh_task.done():
                refresh_task = None

            if not lit:
                await asyncio.sleep(SWITCH_INTERVAL)
                continue

            medal = lit[idx % len(lit)]
            idx += 1
            ok = await wear_medal(client, medal["medal_id"])
            ts = time.strftime("%H:%M:%S")
            status = "OK" if ok else "FAIL"
            print(f"[{ts}] 佩戴 [{medal['medal_id']}] {medal['target_name']} Lv.{medal['level']} -> {status}")
            await asyncio.sleep(SWITCH_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n已停止。")
