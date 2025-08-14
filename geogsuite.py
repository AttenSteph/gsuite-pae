"""
Enrich a CSV with GeoIP data using local MaxMind databases.

Features:
  - Inserts a single 'geoip' column immediately to the right of the IP column.
  - Supports GeoLite2-City.mmdb (required) and optional GeoLite2-ASN.mmdb.
  - Skips private/reserved IPs cleanly.
  - Handles large files via optional chunking.

Usage:
  python geoip_enrich.py --in input.csv --out output.csv --db /path/GeoLite2-City.mmdb --ip-col ip
  python geoip_enrich.py --in input.csv --out output.csv --db /path/GeoLite2-City.mmdb --asn-db /path/GeoLite2-ASN.mmdb

Install deps:
  pip install pandas geoip2
"""
import argparse
import ipaddress
from typing import Optional, Tuple
import pandas as pd

try:
    import geoip2.database  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency 'geoip2'. Install with: pip install geoip2") from e


def parse_args():
    p = argparse.ArgumentParser(description="Enrich CSV with a GeoIP column using local MaxMind DBs.")
    p.add_argument("--in", dest="inp", required=True, help="Input CSV path")
    p.add_argument("--out", dest="out", required=False, help="Output CSV path (default: <input>.geoip.csv)")
    p.add_argument("--db", dest="city_db", required=True, help="Path to GeoLite2-City.mmdb (or commercial City DB)")
    p.add_argument("--asn-db", dest="asn_db", required=False, help="Path to GeoLite2-ASN.mmdb (optional)")
    p.add_argument("--ip-col", dest="ip_col", required=False, default=None,
                   help="Name of the IP address column (if omitted, auto-detect common names)")
    p.add_argument("--geoip-col", dest="geoip_col", default="geoip",
                   help="Name of the inserted GeoIP column (default: geoip)")
    p.add_argument("--chunksize", type=int, default=0,
                   help="Process CSV in chunks of this many rows (0 = load all at once)")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    p.add_argument("--sep", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--quotechar", default='"', help='CSV quotechar (default: ")')
    p.add_argument("--na-filter", action="store_true", help="Enable pandas NA parsing (default: disabled for speed)")
    p.add_argument("--keep-invalid", action="store_true",
                   help="If set, writes a value for invalid IPs instead of empty string")
    p.add_argument("--invalid-marker", default="invalid_ip",
                   help="Marker used when --keep-invalid is set (default: invalid_ip)")
    return p.parse_args()


def autodetect_ip_col(df: pd.DataFrame) -> Optional[str]:
    common = ["ip", "ip_address", "client_ip", "source_ip", "src_ip", "dst_ip", "remote_ip"]
    for c in df.columns:
        if c.lower() in common:
            return c
    # fallback: pick first column that looks like IPs in >50% of non-null rows
    sample = df.head(1000)
    for c in sample.columns:
        vals = sample[c].dropna().astype(str).head(200).tolist()
        if not vals:
            continue
        hits = 0
        tot = 0
        for v in vals:
            tot += 1
            try:
                ipaddress.ip_address(v.strip())
                hits += 1
            except Exception:
                pass
        if tot and hits / tot >= 0.5:
            return c
    return None


def is_public_ip(s: str) -> bool:
    try:
        ip = ipaddress.ip_address(s.strip())
        return not (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local or ip.is_multicast)
    except Exception:
        return False


def lookup(city_reader, asn_reader, ip: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[float], Optional[int], Optional[str]]:
    """
    Returns: (country_iso, region, city, lat, lon, asn, org)
    """
    country_iso = region = city = org = None
    lat = lon = None
    asn = None
    try:
        city_resp = city_reader.city(ip)
        country_iso = getattr(getattr(city_resp, "country", None), "iso_code", None)
        # region: prefer most specific subdivision name or ISO code
        if city_resp.subdivisions and len(city_resp.subdivisions) > 0:
            region = city_resp.subdivisions.most_specific.name or city_resp.subdivisions.most_specific.iso_code
        city = getattr(getattr(city_resp, "city", None), "name", None)
        if getattr(city_resp, "location", None):
            lat = city_resp.location.latitude
            lon = city_resp.location.longitude
    except Exception:
        pass

    if asn_reader is not None:
        try:
            asn_resp = asn_reader.asn(ip)
            asn = getattr(asn_resp, "autonomous_system_number", None)
            org = getattr(asn_resp, "autonomous_system_organization", None)
        except Exception:
            pass

    return country_iso, region, city, lat, lon, asn, org


def format_geoip(country_iso, region, city, lat, lon, asn, org) -> str:
    """
    Compact single-column representation suitable for filtering and pivoting.
    Example: 'US|Washington|Seattle|47.61|-122.33|AS15169|Google LLC'
    Missing fields are empty strings.
    """
    parts = [
        country_iso or "",
        region or "",
        city or "",
        f"{lat:.6f}" if isinstance(lat, (float, int)) else "",
        f"{lon:.6f}" if isinstance(lon, (float, int)) else "",
        f"AS{asn}" if isinstance(asn, int) else "",
        org or "",
    ]
    return "|".join(parts)


def enrich_dataframe(df: pd.DataFrame, ip_col: str, city_db: str, asn_db: Optional[str], geoip_col: str) -> pd.DataFrame:
    from geoip2.database import Reader
    city_reader = Reader(city_db)
    asn_reader = Reader(asn_db) if asn_db else None
    try:
        # Compute geoip values
        def compute(ip_val: str) -> str:
            if not isinstance(ip_val, str):
                return ""
            s = ip_val.strip()
            if not s:
                return ""
            if not is_public_ip(s):
                return ""
            c, r, ci, la, lo, an, og = lookup(city_reader, asn_reader, s)
            return format_geoip(c, r, ci, la, lo, an, og)

        geo_series = df[ip_col].astype(str, errors="ignore").apply(compute)

        # Insert the new column immediately to the right of the IP column
        cols = list(df.columns)
        ip_idx = cols.index(ip_col)
        # Build new column order
        new_cols = cols[: ip_idx + 1] + [geoip_col] + cols[ip_idx + 1 :]
        # Construct new DataFrame
        df_out = df.copy()
        df_out.insert(ip_idx + 1, geoip_col, geo_series)
        return df_out[new_cols]
    finally:
        city_reader.close()
        if asn_reader:
            asn_reader.close()


def process_all(args):
    out_path = args.out or (args.inp.rsplit(".", 1)[0] + ".geoip.csv")
    read_kwargs = dict(encoding=args.encoding, sep=args.sep, quotechar=args.quotechar, low_memory=False)
    if not args.na_filter:
        read_kwargs["na_filter"] = False

    if args.chunksize and args.chunksize > 0:
        # Stream in chunks
        reader = pd.read_csv(args.inp, chunksize=args.chunksize, **read_kwargs)
        first = True
        detected_ip = None
        for chunk in reader:
            if args.ip_col:
                ip_col = args.ip_col
            else:
                detected_ip = detected_ip or autodetect_ip_col(chunk)
                if not detected_ip:
                    raise SystemExit("Failed to auto-detect IP column. Specify --ip-col.")
                ip_col = detected_ip
            enriched = enrich_dataframe(chunk, ip_col, args.city_db, args.asn_db, args.geoip_col)
            mode = "w" if first else "a"
            header = first
            enriched.to_csv(out_path, index=False, mode=mode, header=header, encoding=args.encoding)
            first = False
    else:
        df = pd.read_csv(args.inp, **read_kwargs)
        ip_col = args.ip_col or autodetect_ip_col(df)
        if not ip_col:
            raise SystemExit("Failed to auto-detect IP column. Specify --ip-col.")
        out_df = enrich_dataframe(df, ip_col, args.city_db, args.asn_db, args.geoip_col)
        out_df.to_csv(out_path, index=False, encoding=args.encoding)
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    args = parse_args()
    process_all(args)