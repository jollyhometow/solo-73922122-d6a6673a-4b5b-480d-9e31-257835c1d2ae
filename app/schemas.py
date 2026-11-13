"""请求模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class TimelineEntryIn(BaseModel):
    region: str = Field(..., min_length=1, description="地区名称")
    effective_from: str = Field(..., description="生效日 YYYY-MM-DD，区间左闭")
    effective_to: str | None = Field(None, description="失效日 YYYY-MM-DD，区间右开；末段可为 null")


class CorrectionIn(BaseModel):
    customer_id: str = Field(..., min_length=1)
    expected_version: int = Field(..., ge=0, description="所见修订（当前生效版本号），过期将被拒绝")
    entries: list[TimelineEntryIn] = Field(..., min_length=1)
    backfill_delay_ms: int = Field(0, ge=0, le=300000, description="可调回填延迟，演示回填期间并发")
    fail_simulation: bool = Field(False, description="模拟回填失败：验证失败保留旧版")
    note: str | None = None
