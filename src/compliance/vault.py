"""The token vault: the only module in this repo that touches raw identifiers.

ASSIGNMENT.md 5e: "Tokenize direct identifiers into a segregated vault; the
analytic warehouse joins on surrogates. **Recall the 2725(3) ZIP5 carve-out
when you draw the boundary. Build the boundary into your schema, not into a
policy document.**"

So the boundary IS the schema, and this is where it is drawn.

  18 U.S.C. 2725(3) defines "personal information" as information that
  identifies an individual -- and then says it "does not include information
  on vehicular accidents, driving violations, and driver's status" and
  excludes the **5-digit zip code**.

That single exclusion is the entire lawful path to geographic aggregation for
a record sourced from a motor vehicle record, and it is drawn here as a
projection: `analytic_columns()` is the complete list of what may leave, and
anything not on it never enters a frame that a gold writer can see.

    CROSSES  party_token, phone_token, zip5, jurisdiction, and values DERIVED
             from the coordinate inside this module (census tract, block
             group, H3 cell, IANA zone, snap status, NPA).
    NEVER    full_name, street_address, city, phone_e164, party_latitude,
             party_longitude.

A note on the derived geography, because it is the one place this boundary is
softer than 2725(3) alone would draw it: a census block group is FINER than a
5-digit ZIP, so on a record genuinely sourced from an MVR the carve-out would
justify the ZIP and not the block group. They are emitted because
`contracts/lead_output.schema.json` -- the interface the downstream contact
centre consumes -- has `geo.census_tract`, `geo.census_bg` and `geo.h3_r8`
fields and this pipeline does not get to redefine its consumer's contract, and
because the fixture rows are not MVR-sourced. COMPLIANCE.md records the
tension and says what a production MVR-sourced feed would have to drop.


Tokens
------
HMAC-SHA256, keyed, over the identifier, rendered as a prefixed hex string.
Keyed rather than a bare digest because an unkeyed hash of a ten-digit NANP
number is reversible in milliseconds -- the domain has 10^10 members and a
laptop does 10^9 SHA-256/s -- so an unkeyed "token" is the phone number with
extra steps. Deterministic rather than a random UUID because the token is the
JOIN KEY between the vault and the warehouse and between two builds; a random
surrogate would need its own mapping table, which is a second vault.

The key lives in `config/settings.toml [keys].vault_hmac_key` (gitignored).
`config/compliance.toml` carries a DOCUMENTED DEV DEFAULT so a fresh clone
runs the tests; it is published in a public repo and is therefore not a
secret, which is exactly why a real deployment must set its own. With the key,
a token is reversible by brute force over the identifier space. Rotation is
discussed in COMPLIANCE.md and is deliberately not implemented (rotating a
join key means re-keying every derived table, which is a migration, not a
function).


The access log -- 18 U.S.C. 2721(c)
-----------------------------------
Every read of a vault table appends a row naming the reader, the table, the
purpose, the row count and the time. 2721(c) independently requires a
FIVE-YEAR redisclosure record identifying each recipient and the permitted
purpose, and "per-query access attribution on any table containing direct
identifiers" is 5e in as many words.

The log is append-only and content-addressed by the build sha, in
`access_log/<build_sha>.parquet`. That is what makes it both a real audit
record and byte-reproducible: a re-run with unchanged inputs has the same
build sha and rewrites the same partition with identical content, while a run
with changed inputs writes a NEW partition beside the old one and never
touches it. `read_at` is the run's frozen `as_of` at 00:00 UTC for the same
reason `evaluated_at` is -- wall clock in an output makes two identical builds
produce different bytes, and the real wall clock is in the manifest.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import config
from ..ingest.watermark import durable_replace

log = logging.getLogger("compliance.vault")

PARTIES = "parties"
CONSENT_PROVENANCE = "consent_provenance"
REVOCATIONS = "revocations"
INTERNAL_DNC = "internal_dnc"
EBR = "ebr"
ACCESS_LOG = "access_log"

TABLES = (PARTIES, CONSENT_PROVENANCE, REVOCATIONS, INTERNAL_DNC, EBR)

# The raw fixture columns. Named here ONCE so the PII test can import the list
# rather than re-deriving it, and so a new fixture column cannot quietly
# become an analytic column by being forgotten.
DIRECT_IDENTIFIERS = (
    "full_name", "street_address", "city", "phone_e164",
    "party_latitude", "party_longitude",
)

# What may cross the boundary. See the module docstring.
ANALYTIC_COLUMNS = (
    "party_id_label",     # the fixture's P0xx label. NOT a natural key of a
                          # person: it identifies a ROW OF A SYNTHETIC FILE,
                          # and it is what makes the golden test readable.
    "party_token",
    "phone_token",
    "zip5",               # 18 U.S.C. 2725(3) -- the carve-out, and the reason
                          # this column and not `street_address` is here.
    "jurisdiction",
    "npa",                # three digits, coarser than the ZIP5 that 2725(3)
                          # expressly excludes. Needed for the calling-window
                          # fallback and named in `basis`; never dialled.
    "line_type",
    "line_type_asof",
    "rnd_response",
    "on_national_dnc",
    "dnc_scrub_age_days",
    "consent_on_file",
    "consent_revoked",
    "incident_date",
    "report_filing_date",
    "fixture_note",
)


class VaultError(RuntimeError):
    pass


def vault_root(root: Path | str | None = None) -> Path:
    if root is not None:
        return Path(root)
    return config.DATA_DIR / str(config.compliance()["vault_subdir"])


def hmac_key() -> bytes:
    """The vault key: settings.toml if configured, else the documented dev key."""
    cfg = config.compliance()
    value = config.key(str(cfg["vault_key_name"]), default=str(cfg["vault_hmac_key_dev"]))
    if value == str(cfg["vault_hmac_key_dev"]):
        log.debug("vault using the documented development key from config/compliance.toml")
    return value.encode("utf-8")


def tokenise(value: Any, *, prefix: str, key: bytes) -> str | None:
    """HMAC-SHA256 over one identifier. None in, None out -- never a token
    for an absent value, because a token for nothing still joins."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value)
    if not text:
        return None
    digest = hmac.new(key, text.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{prefix}_{digest[:32]}"


@dataclass
class AccessRecord:
    """One row of the 2721(c) redisclosure record."""

    seq: int
    table: str
    actor: str
    purpose: str
    rows: int
    columns: str
    read_at: datetime
    build_sha: str


@dataclass
class Vault:
    """Segregated storage for direct identifiers, with attribution on read.

    Every constructor argument that appears in the access log is required.
    There is no default `purpose`: a read whose purpose nobody had to type is
    a read nobody will be able to justify in five years, and 2721(c) asks for
    exactly that justification.
    """

    root: Path
    as_of: date
    build_sha: str = "unset"
    key: bytes = field(default_factory=hmac_key, repr=False)
    _access: list[AccessRecord] = field(default_factory=list, repr=False)

    # -- tokens ---------------------------------------------------------
    def party_token(self, party_id: Any) -> str | None:
        return tokenise(party_id, prefix=str(config.compliance()["token_prefix_party"]),
                        key=self.key)

    def phone_token(self, e164: Any) -> str | None:
        return tokenise(e164, prefix=str(config.compliance()["token_prefix_phone"]),
                        key=self.key)

    # -- paths ----------------------------------------------------------
    def path(self, table: str) -> Path:
        return self.root / f"{table}.parquet"

    @property
    def access_log_path(self) -> Path:
        return self.root / ACCESS_LOG / f"{self.build_sha}.parquet"

    # -- reads, which are logged ----------------------------------------
    @property
    def read_at(self) -> datetime:
        """The frozen clock. See the module docstring."""
        return datetime.combine(self.as_of, datetime.min.time(), tzinfo=timezone.utc)

    def _log(self, table: str, actor: str, purpose: str, rows: int,
             columns: Sequence[str]) -> None:
        self._access.append(
            AccessRecord(
                seq=len(self._access) + 1,
                table=table,
                actor=actor,
                purpose=purpose,
                rows=int(rows),
                columns=",".join(sorted(columns)),
                read_at=self.read_at,
                build_sha=self.build_sha,
            )
        )

    def read(self, table: str, *, actor: str, purpose: str,
             columns: Sequence[str] | None = None) -> pd.DataFrame:
        """Read a vault table and append an access-log row. Always both."""
        path = self.path(table)
        if not path.exists():
            frame = pd.DataFrame(columns=list(columns or []))
        else:
            frame = pd.read_parquet(path, columns=list(columns) if columns else None)
        self._log(table, actor, purpose, len(frame), frame.columns)
        return frame

    def analytic_view(self, *, actor: str, purpose: str) -> pd.DataFrame:
        """The ONLY projection of `parties` that may leave the vault.

        A ZIP5-only view in the sense 2725(3) means it: the finest DIRECT
        location that crosses is the 5-digit ZIP, and nothing on
        DIRECT_IDENTIFIERS is in the result by construction -- the projection
        is a whitelist, so a new fixture column has to be added here on
        purpose before it can reach an analytic frame.
        """
        frame = self.read(PARTIES, actor=actor, purpose=purpose)
        leaked = [c for c in DIRECT_IDENTIFIERS if c in frame.columns]
        keep = [c for c in ANALYTIC_COLUMNS if c in frame.columns]
        out = frame[keep].copy()
        still = [c for c in out.columns if c in leaked]
        if still:  # pragma: no cover - defensive; the whitelist makes it unreachable
            raise VaultError(f"analytic view would carry direct identifiers: {still}")
        return out

    # -- writes ---------------------------------------------------------
    def write(self, table: str, frame: pd.DataFrame, *, order_by: Sequence[str]) -> Path:
        """Replace a vault table deterministically.

        Sorted by a key the caller declares total, fsync-then-rename via
        Phase 1's `durable_replace`, so a crash mid-write cannot leave a
        half-written vault where a complete one should be.
        """
        dest = self.path(table)
        dest.parent.mkdir(parents=True, exist_ok=True)
        cols = list(order_by)
        missing = [c for c in cols if c not in frame.columns]
        if missing:
            raise VaultError(f"{table}: sort key {missing} not in the frame")
        ordered = frame.sort_values(cols, kind="stable").reset_index(drop=True)
        if ordered.duplicated(subset=cols).any():
            raise VaultError(
                f"{table}: ORDER BY {cols} is not total; the parquet bytes would "
                "depend on thread scheduling"
            )
        tmp = dest.with_name(dest.name + ".part")
        pq.write_table(pa.Table.from_pandas(ordered, preserve_index=False), tmp,
                       compression="zstd")
        durable_replace(tmp, dest)
        return dest

    def append(self, table: str, rows: Sequence[dict[str, Any]], *,
               order_by: Sequence[str]) -> Path:
        """Append rows to an append-only table, de-duplicated on the sort key.

        Append-only means append-only: an existing row is never rewritten. A
        row whose key is already present is dropped rather than replacing the
        stored one, so a re-run is idempotent and a changed payload under an
        existing key is a bug the caller has to notice, not a silent update.
        """
        existing = pd.read_parquet(self.path(table)) if self.path(table).exists() \
            else pd.DataFrame()
        incoming = pd.DataFrame(rows)
        if incoming.empty:
            return self.path(table)
        combined = pd.concat([existing, incoming], ignore_index=True) \
            if not existing.empty else incoming
        combined = combined.drop_duplicates(subset=list(order_by), keep="first")
        return self.write(table, combined, order_by=order_by)

    def flush_access_log(self) -> Path:
        """Write this build's access-log partition. Content-addressed."""
        dest = self.access_log_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame([vars(r) for r in self._access])
        if frame.empty:
            frame = pd.DataFrame(
                columns=["seq", "table", "actor", "purpose", "rows", "columns",
                         "read_at", "build_sha"]
            )
        frame = frame.sort_values("seq", kind="stable").reset_index(drop=True)
        tmp = dest.with_name(dest.name + ".part")
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), tmp,
                       compression="zstd")
        durable_replace(tmp, dest)
        return dest

    @property
    def access_count(self) -> int:
        return len(self._access)

    def access_frame(self) -> pd.DataFrame:
        return pd.DataFrame([vars(r) for r in self._access])


def load_fixture_into_vault(
    vault: Vault, fixture_path: Path | str, *, actor: str, purpose: str
) -> pd.DataFrame:
    """Read `fixtures/synthetic_parties.csv` and store it, tokenised.

    The ONLY place the raw fixture is read. `fixtures/README.md`: "Treat these
    values as though they were real: tokenise them, do not print them to logs,
    and do not commit intermediate files containing them." Nothing is logged
    here except a count, and the destination is under `data/`, which is
    gitignored.
    """
    from . import window as window_mod

    path = Path(fixture_path)
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    if raw.empty:
        raise VaultError(f"{path} is empty")

    frame = raw.copy()
    frame["party_id_label"] = raw["party_id"]
    frame["party_token"] = [vault.party_token(v) for v in raw["party_id"]]
    frame["phone_token"] = [vault.phone_token(v) for v in raw["phone_e164"]]
    # The area code is parsed from the E.164 number INSIDE the vault, so the
    # number itself never leaves. See window.npa_from_e164 for why it is
    # parsed and not sliced.
    frame["npa"] = [window_mod.npa_from_e164(v) for v in raw["phone_e164"]]
    frame["jurisdiction"] = raw["state"]
    frame["line_type"] = raw["line_type_reported"]
    frame["line_type_asof"] = None       # the fixture carries none; see rules.yaml
    frame["rnd_response"] = raw["rnd_response"]
    frame["on_national_dnc"] = raw["on_national_dnc"].str.lower() == "true"
    frame["dnc_scrub_age_days"] = pd.to_numeric(raw["dnc_scrub_age_days"],
                                                errors="coerce").astype("Int64")
    frame["consent_on_file"] = raw["consent_on_file"].str.lower() == "true"
    frame["consent_revoked"] = raw["consent_revoked"].str.lower() == "true"
    frame["party_latitude"] = pd.to_numeric(raw["party_latitude"], errors="coerce")
    frame["party_longitude"] = pd.to_numeric(raw["party_longitude"], errors="coerce")
    for col in ("incident_date", "report_filing_date"):
        frame[col] = raw[col].replace("", None)

    vault.write(PARTIES, frame, order_by=["party_id"])
    vault._log(PARTIES, actor, f"{purpose} (load)", len(frame), frame.columns)
    log.info("vault: stored %d party records (tokenised)", len(frame))
    return frame
