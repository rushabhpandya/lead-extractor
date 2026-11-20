"""
schemas/lead.py — Pydantic V2 LeadRecord model for the Global B2B Lead Extractor.
Multi-tenant, E.164 phone, Instagram, and global address fields.
"""

from __future__ import annotations
from typing import Optional, List, Any
from pydantic import BaseModel, Field, model_validator
import uuid
from datetime import datetime, timezone


class LeadRecord(BaseModel):
    """Canonical lead record — used for cache persistence and API responses."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    workspace_id: str = Field(default="default_agency")

    # Identity
    business_name: str = ""
    category: str = ""
    sub_locality: str = ""
    city: str = ""
    country_code: str = "IN"   # ISO 3166-1 alpha-2
    address: str = ""

    # Phone
    phone: str = ""             # Raw scraped phone string
    phone_e164: Optional[str] = None   # E.164 e.g. +442074491234
    is_whatsapp_ready: bool = False    # True if mobile / fixed_or_mobile type

    # Email
    email: str = ""
    emails_all: List[str] = Field(default_factory=list)

    # Social
    instagram_handle: Optional[str] = None   # e.g. "mayfairantiques"
    instagram_url: Optional[str] = None      # full URL

    # Web
    website: str = ""
    rating: Optional[float] = None

    # Outbound Marketing & Tech Intelligence Signals (Zero-Request)
    has_meta_pixel: bool = False
    has_google_ads: bool = False
    cms_platform: str = "Custom"
    whatsapp_url: str = ""
    linkedin_url: str = ""
    lead_score: int = 0
    email_mx_verified: bool = False

    # Meta
    source: str = "cache"       # "cache" | "osm" | "ddgs" | "live" | "sqlite"
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @model_validator(mode="before")
    @classmethod
    def populate_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "name" in data and not data.get("business_name"):
                data["business_name"] = data["name"]
            elif "business_name" in data and not data.get("name"):
                data["name"] = data["business_name"]
        return data

    # Legacy compatibility — app.js reads these fields
    @property
    def name(self) -> str:
        return self.business_name

    def to_legacy_dict(self) -> dict:
        """Return dict compatible with the app.js SSE consumer and export endpoints."""
        return {
            "name":              self.business_name,
            "business_name":     self.business_name,
            "address":           self.address,
            "phone":             self.phone_e164 or self.phone,
            "phone_e164":        self.phone_e164,
            "is_whatsapp_ready": self.is_whatsapp_ready,
            "email":             self.email,
            "website":           self.website,
            "rating":            self.rating,
            "instagram_handle":  self.instagram_handle,
            "instagram_url":     self.instagram_url,
            "country_code":      self.country_code,
            "city":              self.city,
            "category":          self.category,
            "source":            self.source,
            # Intelligence Signals
            "has_meta_pixel":    self.has_meta_pixel,
            "has_google_ads":    self.has_google_ads,
            "cms_platform":      self.cms_platform,
            "whatsapp_url":      self.whatsapp_url,
            "linkedin_url":      self.linkedin_url,
            "lead_score":        self.lead_score,
            "email_mx_verified": self.email_mx_verified,
        }
