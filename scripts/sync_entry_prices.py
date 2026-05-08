"""
일회성 스크립트: 잔고조회 API에서 매입평균가를 가져와 active 포지션의 entry_price를 갱신.

Usage:
    python -m scripts.sync_entry_prices
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from app.config import KISConfig
from app.broker.client import KISClient
from app.models.database import init_db
from app.models import Position, get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = "sqlite+aiosqlite:///data/trading.db"


async def get_holdings(client: KISClient) -> dict[str, int]:
    """잔고조회 output1에서 종목별 매입평균가를 가져온다."""
    env = client.config.env
    tr_id = "VTTC8434R" if env == "paper" else "TTTC8434R"

    params = {
        "CANO": client.config.account_prefix,
        "ACNT_PRDT_CD": client.config.account_suffix,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }

    data = await client.request(
        "GET", "/uapi/domestic-stock/v1/trading/inquire-balance", tr_id, params=params
    )

    holdings = {}
    for item in data.get("output1", []):
        symbol = item.get("pdno", "")
        avg_price = int(float(item.get("pchs_avg_pric", "0")))
        if symbol and avg_price > 0:
            holdings[symbol] = avg_price
    return holdings


async def main():
    init_db(DATABASE_URL)

    config = KISConfig()
    client = KISClient(config)
    await client.refresh_token()

    logger.info("Fetching holdings from broker API...")
    holdings = await get_holdings(client)
    logger.info(f"Found {len(holdings)} holdings: {holdings}")

    if not holdings:
        logger.info("No holdings found. Nothing to update.")
        await client.close()
        return

    updated = 0
    async for session in get_session():
        stmt = select(Position).where(Position.status == "active")
        positions = (await session.execute(stmt)).scalars().all()

        for pos in positions:
            if pos.symbol in holdings:
                old_price = pos.entry_price
                new_price = holdings[pos.symbol]
                if old_price != new_price:
                    pos.entry_price = new_price
                    pos.peak_price = max(pos.peak_price or 0, new_price)
                    logger.info(
                        f"  {pos.symbol} {pos.name}: {old_price:,} -> {new_price:,}"
                    )
                    updated += 1
                else:
                    logger.info(f"  {pos.symbol} {pos.name}: already correct ({new_price:,})")
            else:
                logger.warning(f"  {pos.symbol} {pos.name}: not found in broker holdings")

        await session.commit()

    logger.info(f"Done. Updated {updated} positions.")
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
