#!/usr/bin/env python3
"""
Herbie-based GFS precipitation downloader and GeoTIFF converter.

This module exposes a single function `download_GFS(systemStartLRTime, systemEndTime, xmin, xmax, ymin, ymax, qpf_store_path)`
that downloads GFS precipitation rate (PRATE) for a given model run start time and a requested
time window, converts rate to precipitation amount per time step (rate × Δt), clips to the given
bounding box, and writes GeoTIFF files suitable for EF5.

Key details:
- Uses Herbie to select the best available source for GFS pgrb2.0p25 files.
- Fetches PRATE (precipitation rate) from GFS.
- Converts PRATE (kg m-2 s-1) to hourly precipitation rate (mm/hour) by multiplying by 3600.
- Writes EPSG:4326 GeoTIFFs using rioxarray, clipped to the provided bbox.
- File naming: gfs.YYYYMMDDHHMM.tif (valid time in UTC).

Notes:
- GFS 0.25° files generally provide hourly output to +120 h and 3-hourly beyond that.
  We generate the forecast hour list accordingly.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple, Union

import numpy as np
import xarray as xr

# rioxarray import registers the rio accessor on xarray objects
import rioxarray  # noqa: F401

try:
    from herbie import Herbie
except Exception as exc:  # pragma: no cover - provide a clearer import error
    raise ImportError(
        "Herbie is required. Install with `pip install herbie-data`"
    ) from exc


def _ensure_datetime(dt_like: Union[str, datetime]) -> datetime:
    """Convert a string or datetime-like to a Python datetime (naive, UTC-assumed).

    Acceptable string formats include:
    - "YYYY-MM-DD HH"
    - "YYYY-MM-DDTHH"
    - "YYYY-MM-DD HH:MM"
    - "YYYY-MM-DDTHH:MM"
    - "YYYY-MM-DD" (defaults to 00 UTC)
    """
    if isinstance(dt_like, datetime):
        return dt_like

    s = str(dt_like).strip()
    for fmt in (
        "%Y-%m-%d %H",
        "%Y-%m-%dT%H",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Unrecognized datetime format: {dt_like}")


def _gfs_forecast_hours(max_hours: int, upper_limit: int = 384) -> List[int]:
    """Generate forecast hours for GFS 0.25°: hourly to 120h, then 3-hourly.

    Ensures 0..min(max_hours, upper_limit), with step 1 to 120 and step 3 beyond.
    Avoids 121 and 122 which are typically unavailable in pgrb2.0p25 output.
    """
    if max_hours < 0:
        return []
    limit = min(max_hours, upper_limit)
    if limit <= 120:
        return list(range(0, limit + 1, 1))
    # Hourly 0..120, then 123..limit step 3
    tail_start = 123 if limit >= 123 else None
    head = list(range(0, 121, 1))
    if tail_start is None:
        return head
    tail = list(range(tail_start, limit + 1, 3))
    return head + tail


def _find_precip_var_name(ds: xr.Dataset) -> str:
    """Heuristically pick the precipitation variable (PRATE/APCP) from a Dataset.

    We expect exactly one primary data variable for the query. Prefer variables
    whose attributes indicate APCP/precip.
    """
    candidates = list(ds.data_vars)
    if not candidates:
        raise KeyError("No data variables found in dataset")

    # Prefer variables with GRIB/long name hints
    def score(var_name: str) -> int:
        v = ds[var_name]
        attrs = {k.lower(): str(v.attrs.get(k, "")).lower() for k in v.attrs}
        text = " ".join([var_name.lower()] + list(attrs.values()))
        hits = 0
        # prioritize prate first, then apcp
        for token in ("prate", "precipitation rate", "apcp", "precip", "total precipitation"):
            if token in text:
                hits += 1
        return hits

    scored = sorted(candidates, key=score, reverse=True)
    return scored[0]


def _standardize_latlon(var_da: xr.DataArray) -> xr.DataArray:
    """Return DataArray renamed to dims lat/lon with CRS=EPSG:4326 and spatial dims set.

    Handles common cfgrib outputs where dims may be (time, latitude, longitude), (latitude, longitude),
    or (y, x) with coordinates named latitude/longitude.
    """
    da = var_da.squeeze(drop=True)

    # Identify latitude/longitude dims
    dims = list(da.dims)
    lat_dim = None
    lon_dim = None
    for d in dims:
        dl = d.lower()
        if lat_dim is None and (dl == "latitude" or dl == "lat" or dl.endswith("_lat")):
            lat_dim = d
        if lon_dim is None and (dl == "longitude" or dl == "lon" or dl.endswith("_lon")):
            lon_dim = d

    # If dims are generic y/x, try to map via coordinates
    if lat_dim is None or lon_dim is None:
        for d in dims:
            if d.lower() in ("y",):
                lat_dim = d
            if d.lower() in ("x",):
                lon_dim = d

    # Rename dims to lat/lon
    rename_map = {}
    if lat_dim and lat_dim != "lat":
        rename_map[lat_dim] = "lat"
    if lon_dim and lon_dim != "lon":
        rename_map[lon_dim] = "lon"
    if rename_map:
        da = da.rename(rename_map)

    # Ensure coords named lat/lon exist
    if "lat" not in da.coords:
        # Try to attach from dataset coords/variables
        if "latitude" in da.coords:
            da = da.rename({"latitude": "lat"})
        elif "latitude" in da.to_dataset().variables:
            da = da.assign_coords(lat=da.to_dataset()["latitude"])  # type: ignore
        else:
            # Create synthetic lat coordinate if missing (assume regular grid)
            da = da.assign_coords(lat=np.arange(da.sizes["lat"]))

    if "lon" not in da.coords:
        if "longitude" in da.coords:
            da = da.rename({"longitude": "lon"})
        elif "longitude" in da.to_dataset().variables:
            da = da.assign_coords(lon=da.to_dataset()["longitude"])  # type: ignore
        else:
            da = da.assign_coords(lon=np.arange(da.sizes["lon"]))

    # Register spatial metadata for rioxarray
    da = da.rio.write_crs("EPSG:4326", inplace=False)
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)

    return da


def _wrap_longitudes_to_180(da: xr.DataArray) -> xr.DataArray:
    """Wrap longitude coordinate to [-180, 180] and sort by longitude ascending."""
    if "lon" not in da.coords:
        return da
    lon_vals = da.coords["lon"].values
    # Only wrap if values exceed 180 (i.e., 0..360 grid)
    if np.nanmax(lon_vals) <= 180 and np.nanmin(lon_vals) >= -180:
        return da
    lon_wrapped = ((lon_vals + 180.0) % 360.0) - 180.0
    da = da.assign_coords(lon=("lon", lon_wrapped))
    da = da.sortby("lon")
    return da




def _safe_to_raster(da: xr.DataArray, out_path: str) -> None:
    """Write DataArray to GeoTIFF with sensible defaults for EF5 compatibility."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # Fill NaNs
    data = da.data.astype(np.float32)
    data = np.where(np.isnan(data), -9999.0, data)
    # rioxarray respects dtype/nodata via kwargs
    da_to_write = xr.DataArray(
        data=data,
        dims=da.dims,
        coords=da.coords,
        name=da.name or "PRATE_mm_hr",
        attrs={"units": "mm/h"},
    )
    da_to_write.rio.write_nodata(-9999.0, inplace=True)
    da_to_write.rio.to_raster(out_path, driver="GTiff", dtype="float32")


def download_GFS(
    systemStartLRTime: Union[str, datetime],
    systemEndTime: Union[str, datetime],
    xmin: float,
    xmax: float,
    ymin: float,
    ymax: float,
    qpf_store_path: str,
) -> List[str]:
    """Download GFS PRATE with Herbie and write hourly rate GeoTIFFs clipped to bbox.

    Args:
        systemStartLRTime: GFS run start time (example: "2023-09-04 12").
        systemEndTime: Ending valid time for which to produce outputs.
        xmin: Minimum longitude for clipping.
        xmax: Maximum longitude for clipping.
        ymin: Minimum latitude for clipping.
        ymax: Maximum latitude for clipping.
        qpf_store_path: Output directory to store GeoTIFFs.

    Returns:
        List of output GeoTIFF file paths written.
    """
    init_time = _ensure_datetime(systemStartLRTime)
    end_time = _ensure_datetime(systemEndTime)

    if end_time < init_time:
        raise ValueError("systemEndTime must be >= systemStartLRTime")

    total_hours = int(round((end_time - init_time).total_seconds() / 3600.0))
    fxx_all = _gfs_forecast_hours(total_hours)
    # Skip analysis hour (f000) for rate-based data
    fxx_list = fxx_all

    outputs: List[str] = []

    for fxx in fxx_list:
        valid_time = init_time + timedelta(hours=fxx)

        # retrieve PRATE via Herbie for this forecast hour
        H = Herbie(init_time, model="gfs", product="pgrb2.0p25", fxx=fxx)

        ds: Optional[Union[xr.Dataset, List[xr.Dataset]]] = None
        last_err: Optional[Exception] = None
        for query in (":PRATE:surface", ":PRATE:", "PRATE:surface", "PRATE"):
            try:
                ds = H.xarray(query)
                break
            except Exception as e:  # pragma: no cover - remote data nuances
                last_err = e
                ds = None
        if ds is None:
            # If PRATE is missing for this hour, we skip
            sys.stderr.write(
                f"Warning: Could not retrieve PRATE for f{fxx:03d} ({valid_time:%Y-%m-%d %H:%M} UTC).\n"
            )
            if last_err:
                sys.stderr.write(f"  Reason: {last_err}\n")
            continue

        # Herbie/cfgrib may return a list of datasets (multiple hypercubes).
        # Per user request, use the first hypercube (cube 0).
        try:
            if isinstance(ds, list):
                if len(ds) == 0:
                    raise KeyError("Empty hypercube list returned for PRATE")
                ds0 = ds[0]
                ds = ds0
                if "prate" in ds.data_vars:
                    var_name = "prate"
                elif "PRATE" in ds.data_vars:
                    var_name = "PRATE"
                else:
                    # Fall back: try to find a single data var
                    data_vars = list(ds.data_vars)
                    if not data_vars:
                        raise KeyError("PRATE variable not present in first hypercube")
                    var_name = data_vars[0]
            else:
                if "prate" in ds.data_vars:
                    var_name = "prate"
                elif "PRATE" in ds.data_vars:
                    var_name = "PRATE"
                else:
                    raise KeyError("PRATE variable not present in dataset")

            prate_da = ds[var_name]
        except Exception as e:  # pragma: no cover
            sys.stderr.write(
                f"Warning: PRATE variable not found for f{fxx:03d}. Reason: {e}\n"
            )
            continue

        # Standardize spatial dims and CRS
        prate_da = _standardize_latlon(prate_da)
        prate_da = _wrap_longitudes_to_180(prate_da)

        # Convert rate (kg m-2 s-1 == mm/s) to mm/hour
        rate = prate_da.data.astype(np.float32)
        if rate.ndim == 3:
            rate = np.squeeze(rate, axis=0)
        # Convert mm/s to mm/hour (multiply by 3600)
        rate_mm_per_hour = rate * 3600.0

        # Build DataArray with mm/hour precipitation rate
        step_da = xr.DataArray(
            data=rate_mm_per_hour,
            dims=("lat", "lon"),
            coords={"lat": prate_da.coords["lat"], "lon": prate_da.coords["lon"]},
            name="PRATE_mm_per_hour",
            attrs={"units": "mm/hour"},
        )

        # Attach spatial metadata for rioxarray
        step_da = step_da.rio.write_crs("EPSG:4326", inplace=False)
        step_da = step_da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)

        # Clip to bounding box
        try:
            clipped_da = step_da.rio.clip_box(minx=float(xmin), miny=float(ymin), maxx=float(xmax), maxy=float(ymax))
        except Exception:
            # If clip fails (e.g., bbox outside domain), fall back to un-clipped writing
            clipped_da = step_da

        # Build output path and write
        out_name = f"gfs.{valid_time:%Y%m%d%H%M}.tif"
        out_path = os.path.join(qpf_store_path, out_name)
        os.makedirs(qpf_store_path, exist_ok=True)
        _safe_to_raster(clipped_da, out_path)
        outputs.append(out_path)

    return outputs


def _parse_cli_args(argv: Optional[List[str]] = None):  # pragma: no cover - CLI helper
    import argparse

    p = argparse.ArgumentParser(description="Download GFS APCP via Herbie and write step GeoTIFFs.")
    p.add_argument("--start", required=True, help="Model run start (e.g., '2023-09-04 12')")
    p.add_argument("--end", required=True, help="End valid time (e.g., '2023-09-09 00')")
    p.add_argument("--xmin", type=float, required=True)
    p.add_argument("--xmax", type=float, required=True)
    p.add_argument("--ymin", type=float, required=True)
    p.add_argument("--ymax", type=float, required=True)
    p.add_argument("--out", required=True, help="Output directory for GeoTIFFs")
    return p.parse_args(argv)


if __name__ == "__main__":  # pragma: no cover -CLI ready
    args = _parse_cli_args()
    written = download_GFS(
        systemStartLRTime=args.start,
        systemEndTime=args.end,
        xmin=args.xmin,
        xmax=args.xmax,
        ymin=args.ymin,
        ymax=args.ymax,
        qpf_store_path=args.out,
    )
    print(f"Wrote {len(written)} files to {args.out}")