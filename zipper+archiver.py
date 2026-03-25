import os
import exifread
import hashlib
import pandas as pd
import zipfile
from datetime import datetime
from io import BytesIO

# --- CONFIGURATION ---
SOURCE_DIR = "data"
# smb://nas1.its.carleton.edu/arbvideo_
NAS_ROOT = "ziptest"
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
    with open(MASTER_MANIFEST_PATH, 'r', encoding='utf-8') as f:
        df_master = pd.read_csv(f)
    # Track existing sizes and hashes for the hybrid check
    existing_sizes = set(df_master['file_size_bytes'].astype(int))
    existing_hashes = set(df_master['checksum_sha256'].astype(str))
else:
    df_master = pd.DataFrame()
    existing_sizes = set()
    existing_hashes = set()

batch_metadata = []
stats = {"processed": 0, "duplicates": 0, "no_folder": 0, "no_exif": 0}

# 2. Pre-Scan: Hybrid Deduplication
print(f"Scanning {SOURCE_DIR}...")
for root, dirs, files in os.walk(SOURCE_DIR):
    for filename in files:
        if filename.lower().endswith(('.jpg', '.jpeg')):
            rel_path = os.path.relpath(root, SOURCE_DIR)
            folder_parts = rel_path.split(os.sep)
            
            if folder_parts[0] == '.':
                stats["no_folder"] += 1
                continue
            
            camera_id = folder_parts[0]
            full_path = os.path.join(root, filename)
            file_size = os.path.getsize(full_path)

            # --- HYBRID STEP 1: Fast Size Check ---
            # If size isn't in our list, it's definitely a new file.
            # If size IS in the list, we must hash it to be sure.
            is_potential_dupe = file_size in existing_sizes
            
            try:
                with open(full_path, 'rb') as f:
                    # We only 'need' these bytes now if it's a potential dupe or if we're archiving it
                    file_bytes = f.read()

                # --- HYBRID STEP 2: Precise Hash Check (Only if size matched) ---
                file_hash = get_sha256(file_bytes)
                
                if is_potential_dupe and file_hash in existing_hashes:
                    if DEBUG: print(f"[SKIP] {filename}: Duplicate content confirmed by Hash.")
                    stats["duplicates"] += 1
                    continue
                
                timestamp = get_exif_date(file_bytes)
                if not timestamp:
                    if DEBUG: print(f"[SKIP] {filename}: No EXIF date found.")
                    stats["no_exif"] += 1
                    continue

                batch_metadata.append({
                    'full_path': full_path,
                    'file_bytes': file_bytes, 
                    'original_filename': filename,
                    'timestamp_obj': timestamp,
                    'camera_id': camera_id,
                    'checksum_sha256': file_hash,
                    'size': file_size
                })
                # Update our sets to catch duplicates within the same scan batch
                existing_sizes.add(file_size)
                existing_hashes.add(file_hash)

            except Exception as e:
                if DEBUG: print(f"[ERR] Failed to process {filename}: {e}")
                continue

# 3. Streaming ZIP Process
new_master_rows = []

if batch_metadata:
    batch_metadata.sort(key=lambda x: x['timestamp_obj'])
    
    first_date = batch_metadata[0]['timestamp_obj'].strftime('%Y%b%d')
    last_date = batch_metadata[-1]['timestamp_obj'].strftime('%Y%b%d')
    
    zip_name = f"{first_date}_{last_date}.zip"
    mini_csv_name = f"{first_date}_{last_date}_manifest.csv"
    
    os.makedirs(NAS_ROOT, exist_ok=True)
    zip_path = os.path.join(NAS_ROOT, zip_name)
    mini_csv_path = os.path.join(NAS_ROOT, mini_csv_name)
    
    mini_manifest_rows = []

    print(f"\nArchiving {len(batch_metadata)} unique files to: {zip_name}...")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for item in batch_metadata:
            ts = item['timestamp_obj']
            # Windows-safe filename format (No colons)
            formatted_name = f"{ts.strftime('%Y-%m-%d')}_{ts.strftime('%H%M%S')}_{item['camera_id']}_{item['original_filename']}"
            
            zf.writestr(formatted_name, item['file_bytes'])
            
            row = {
                'internal_zip_name': formatted_name,
                'original_filename': item['original_filename'],
                'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
                'camera_id': item['camera_id'],
                'wildlife_insights_id': 'PENDING',
                'checksum_sha256': item['checksum_sha256'],
                'file_size_bytes': item['size']
            }
            mini_manifest_rows.append(row)
            new_master_rows.append(row)
            stats["processed"] += 1
            item['file_bytes'] = None # Memory cleanup

    # 4. Save Mini Manifest
    if mini_manifest_rows:
        with open(mini_csv_path, 'w', encoding='utf-8', newline='') as f:
            pd.DataFrame(mini_manifest_rows).to_csv(f, index=False)

# 5. Finalize Master Manifest
if new_master_rows:
    df_new = pd.DataFrame(new_master_rows)
    df_final = pd.concat([df_master, df_new], ignore_index=True)
    df_final.sort_values(by=['camera_id', 'timestamp'], inplace=True)
    with open(MASTER_MANIFEST_PATH, 'w', encoding='utf-8', newline='') as f:
        df_final.to_csv(f, index=False)

# 6. Final Summary Report
print("\n" + "="*30)
print("       RUN SUMMARY")
print("="*30)
print(f"Unique Images Archived: {stats['processed']}")
print(f"Skipped (Dupes):        {stats['duplicates']}")
print(f"Skipped (No EXIF):      {stats['no_exif']}")
print("="*30)

input("\nProcess finished. Press Enter to exit...")