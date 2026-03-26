"""
Inventory Snapshot Management using GitHub Gist

This module handles saving and loading inventory snapshots to/from GitHub Gist
for historical comparison purposes.
"""

import requests
import pandas as pd
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any


GIST_API_URL = "https://api.github.com/gists"


def _get_gist_headers(gist_token: str) -> Dict[str, str]:
    """Get headers for Gist API requests."""
    return {
        "Authorization": f"token {gist_token}",
        "Accept": "application/vnd.github.v3+json"
    }


def _get_filename(date_str: str) -> str:
    """Generate filename for a snapshot."""
    return f"inventory_snapshot_{date_str}.json"


def save_snapshot(df: pd.DataFrame, date_str: str, gist_token: str, gist_id: str) -> tuple:
    """
    Save inventory snapshot to Gist as JSON.

    Args:
        df: DataFrame with inventory data
        date_str: Date string for the snapshot (YYYY-MM-DD)
        gist_token: GitHub personal access token
        gist_id: Gist ID to update

    Returns:
        Tuple of (success: bool, debug_info: list)
    """
    debug_info = []
    filename = _get_filename(date_str)
    debug_info.append(f"Saving snapshot as '{filename}'")

    # Select only essential columns to reduce size
    essential_cols = ['SKU', 'Available_Qty', 'Brand', 'Warehouse', 'Country']
    cols_to_save = [col for col in essential_cols if col in df.columns]
    df_subset = df[cols_to_save].copy()

    # Prepare snapshot data
    snapshot_data = {
        "saved_at": datetime.now().isoformat(),
        "date": date_str,
        "snapshot_id": str(uuid.uuid4()),
        "data": df_subset.to_dict(orient="records")
    }

    # Convert to JSON string (no indent to reduce size)
    content = json.dumps(snapshot_data, ensure_ascii=False)
    debug_info.append(f"JSON size: {len(content):,} characters")

    # Prepare the Gist update request
    url = f"{GIST_API_URL}/{gist_id}"
    headers = _get_gist_headers(gist_token)

    try:
        # Get current gist to preserve existing files
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        current_gist = response.json()

        files = current_gist.get("files", {})
        file_exists = filename in files
        debug_info.append(f"File exists in Gist: {file_exists}")
        debug_info.append(f"Current files in Gist: {list(files.keys())}")

        # Build files payload - must include ALL existing files to preserve them
        files_payload = {}
        for fname, finfo in files.items():
            if fname != filename:  # Don't include the file we're updating here
                files_payload[fname] = {"content": finfo.get("content", "")}

        # Add/update our target file
        files_payload[filename] = {"content": content}

        # Prepare update payload
        payload = {
            "description": "Inventory Snapshot Storage",
            "files": files_payload
        }

        debug_info.append(f"Updating Gist with {len(files_payload)} files...")
        response = requests.patch(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        debug_info.append("Save successful!")
        return True, debug_info

    except requests.exceptions.RequestException as e:
        debug_info.append(f"Request error: {str(e)}")
        return False, debug_info
    except Exception as e:
        debug_info.append(f"Unexpected error: {str(e)}")
        return False, debug_info


def load_snapshots(gist_token: str, gist_id: str) -> tuple:
    """
    Fetch all snapshots from Gist.

    Args:
        gist_token: GitHub personal access token
        gist_id: Gist ID

    Returns:
        Tuple of (list of snapshots, debug message)
    """
    url = f"{GIST_API_URL}/{gist_id}"
    headers = _get_gist_headers(gist_token)
    debug_info = []

    try:
        debug_info.append(f"Connecting to Gist API...")
        response = requests.get(url, headers=headers, timeout=30)
        debug_info.append(f"Response status: {response.status_code}")
        response.raise_for_status()
        gist_data = response.json()

        snapshots = []
        files = gist_data.get("files", {})

        debug_info.append(f"Found {len(files)} files in Gist")
        for filename in files.keys():
            debug_info.append(f"  - File: '{filename}'")

        for filename, file_info in files.items():
            if filename.startswith("inventory_snapshot_") and filename.endswith(".json"):
                content = file_info.get("content", "{}")
                try:
                    snapshot = json.loads(content)
                    snapshots.append({
                        "date": snapshot.get("date", ""),
                        "saved_at": snapshot.get("saved_at", ""),
                        "snapshot_id": snapshot.get("snapshot_id", ""),
                        "filename": filename,
                        "data": snapshot.get("data", [])
                    })
                except json.JSONDecodeError as je:
                    debug_info.append(f"JSON decode error for {filename}: {je}")
                    continue

        # Sort by date descending (most recent first)
        snapshots.sort(key=lambda x: x.get("date", ""), reverse=True)
        debug_info.append(f"Loaded {len(snapshots)} valid snapshots")
        return snapshots, debug_info

    except requests.exceptions.RequestException as e:
        debug_info.append(f"Request error: {str(e)}")
        return [], debug_info
    except Exception as e:
        debug_info.append(f"Unexpected error: {str(e)}")
        return [], debug_info


def get_latest_snapshot(gist_token: str, gist_id: str) -> tuple:
    """
    Get the most recent snapshot metadata.

    Args:
        gist_token: GitHub personal access token
        gist_id: Gist ID

    Returns:
        Tuple of (snapshot dict or None, debug message)
    """
    snapshots, debug = load_snapshots(gist_token, gist_id)
    return snapshots[0] if snapshots else None, debug


def get_snapshot_by_date(date_str: str, gist_token: str, gist_id: str) -> tuple:
    """
    Get a specific snapshot by date.

    Args:
        date_str: Date string (YYYY-MM-DD)
        gist_token: GitHub personal access token
        gist_id: Gist ID

    Returns:
        Tuple of (snapshot dict or None, debug message)
    """
    snapshots, debug = load_snapshots(gist_token, gist_id)
    for snapshot in snapshots:
        if snapshot.get("date") == date_str:
            return snapshot, debug
    return None, debug


def get_snapshot_dataframe(snapshot: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert snapshot data to DataFrame.

    Args:
        snapshot: Snapshot metadata dict from load_snapshots

    Returns:
        DataFrame with inventory data
    """
    if not snapshot or "data" not in snapshot:
        return pd.DataFrame()

    data = snapshot.get("data", [])
    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data)
