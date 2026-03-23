import os
import exifread
import hashlib
import pandas as pd
import zipfile
from datetime import datetime
from io import BytesIO

# --- CONFIGURATION ---
SOURCE_DIR = "data"
# actual nas: smb://nas1.its.carleton.edu/arbvideo_
NAS_ROOT = "smb://nas1.its.carleton.edu/arbvideo_"
MASTER_MANIFEST_PATH = "archive.csv"
DEBUG = True 

def get_sha256(file_bytes):
    """Calculates SHA-256 hash from bytes."""
    return hashlib.sha256(file_bytes).hexdigest()

def get_exif_date(file_bytes):
    """Extracts date from image bytes."""
    tags = exifread.process_file(BytesIO(file_bytes), stop_tag='EXIF DateTimeOriginal', details=False)
    date_str = tags.get('EXIF DateTimeOriginal')
    if date_str:
        return datetime.strptime(str(date_str), '%Y:%m:%d %H:%M:%S')
    return None

# 1. Load the existing Master Manifest
if os.path.exists(MASTER_MANIFEST_PATH):
    df_master = pd.read_csv(MASTER_MANIFEST_PATH)
    existing_records = set(zip(df_master['camera_id'], df_master['original_filename']))
else:
    df_master = pd.DataFrame()
    existing_records = set()

# We only store metadata in this list, not the actual image bytes
batch_metadata = []
stats = {"processed": 0, "duplicates": 0, "no_folder": 0, "no_exif": 0}

# 2. Pre-Scan: Collect metadata to determine Date Range
print(f"Scanning {SOURCE_DIR}...")
for root, dirs, files in os.walk(SOURCE_DIR):
    for filename in files:
        if filename.lower().endswith(('.jpg', '.jpeg')):
            
            rel_path = os.path.relpath(root, SOURCE_DIR)
            folder_parts = rel_path.split(os.sep)
            
            if folder_parts[0] == '.':
                if DEBUG: print(f"[SKIP] {filename}: Not inside a camera folder.")
                stats["no_folder"] += 1
                continue
            
            camera_id = folder_parts[0]

            if (camera_id, filename) in existing_records:
                if DEBUG: print(f"[SKIP] {filename}: Duplicate in Master Manifest.")
                stats["duplicates"] += 1
                continue
            
            full_path = os.path.join(root, filename)
            
            # Read just enough to get the Date (Streaming start)
            try:
                with open(full_path, 'rb') as f:
                    # Reading the whole file here briefly to get EXIF
                    # (This is cleared as soon as we leave this loop iteration)
                    temp_bytes = f.read()
                    timestamp = get_exif_date(temp_bytes)
            except Exception as e:
                if DEBUG: print(f"[ERR] Could not read {filename}: {e}")
                continue

            if not timestamp:
                if DEBUG: print(f"[SKIP] {filename}: No EXIF date found.")
                stats["no_exif"] += 1
                continue
            
            batch_metadata.append({
                'full_path': full_path,
                'original_filename': filename,
                'timestamp_obj': timestamp,
                'camera_id': camera_id,
                'size': len(temp_bytes)
            })

# 3. Process the Streaming ZIP
new_master_rows = []

if batch_metadata:
    # Sort by timestamp to define the batch range for the filename
    batch_metadata.sort(key=lambda x: x['timestamp_obj'])
    
    first_date = batch_metadata[0]['timestamp_obj'].strftime('%Y%b%d')
    last_date = batch_metadata[-1]['timestamp_obj'].strftime('%Y%b%d')
    
    zip_name = f"{first_date}_{last_date}.zip"
    mini_csv_name = f"{first_date}_{last_date}_manifest.csv"
    
    os.makedirs(NAS_ROOT, exist_ok=True)
    zip_path = os.path.join(NAS_ROOT, zip_name)
    mini_csv_path = os.path.join(NAS_ROOT, mini_csv_name)
    
    mini_manifest_rows = []

    print(f"\nStreaming {len(batch_metadata)} files to: {zip_name}...")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for item in batch_metadata:
            # Step-by-step processing to keep RAM low
            with open(item['full_path'], 'rb') as f:
                current_file_bytes = f.read()
            
            ts = item['timestamp_obj']
            formatted_name = f"{ts.strftime('%Y-%m-%d')}_{ts.strftime('%H%M%S')}_{item['camera_id']}{item['original_filename']}"
            
            # Compress and write to the NAS
            zf.writestr(formatted_name, current_file_bytes)
            
            # Generate record
            row = {
                'internal_zip_name': formatted_name,
                'original_filename': item['original_filename'],
                'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
                'camera_id': item['camera_id'],
                'wildlife_insights_id': 'PENDING',
                'checksum_sha256': get_sha256(current_file_bytes),
                'file_size_bytes': item['size']
            }
            mini_manifest_rows.append(row)
            new_master_rows.append(row)
            stats["processed"] += 1
            
            if DEBUG: print(f"  -> Archived: {formatted_name}")
            # current_file_bytes is discarded here as we loop to the next file

    # 4. Save Mini Manifest
    pd.DataFrame(mini_manifest_rows).to_csv(mini_csv_path, index=False)

# 5. Finalize Master Manifest
if new_master_rows:
    df_new = pd.DataFrame(new_master_rows)
    df_final = pd.concat([df_master, df_new], ignore_index=True)
    df_final.sort_values(by=['camera_id', 'timestamp'], inplace=True)
    df_final.to_csv(MASTER_MANIFEST_PATH, index=False)

# 6. Final Summary Report
print("\n" + "="*30)
print("       RUN SUMMARY")
print("="*30)
print(f"Images Archived:   {stats['processed']}")
print(f"Skipped (Dupes):   {stats['duplicates']}")
print(f"Skipped (No EXIF): {stats['no_exif']}")
print(f"Skipped (No Dir):  {stats['no_folder']}")
print("="*30)

input("\nProcess finished. Press Enter to exit...")