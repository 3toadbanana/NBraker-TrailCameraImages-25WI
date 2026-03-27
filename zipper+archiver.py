import os
import exifread
import hashlib
import pandas as pd
import zipfile
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
from io import BytesIO

# --- CONFIGURATION ---
DEBUG = True 
MANIFEST_NAME = "archive.csv"

def get_sha256(file_bytes):
    return hashlib.sha256(file_bytes).hexdigest()

def get_exif_date(file_bytes):
    tags = exifread.process_file(BytesIO(file_bytes), stop_tag='EXIF DateTimeOriginal', details=False)
    date_str = tags.get('EXIF DateTimeOriginal')
    if date_str:
        return datetime.strptime(str(date_str), '%Y:%m:%d %H:%M:%S')
    return None

def force_to_front(window):
    """Brings a tkinter window to the absolute front of the OS."""
    window.attributes('-topmost', True)
    window.update()
    window.attributes('-topmost', False)
    window.focus_force()

def run_archiver():
    # Setup hidden main window
    root_gui = tk.Tk()
    root_gui.withdraw()
    
    # 1. Select Folders
    source_dir = filedialog.askdirectory(title="STEP 1: Select the 'data' folder (SD Card/Dump)")
    if not source_dir: return

    nas_root = filedialog.askdirectory(title="STEP 2: Select the Destination (NAS/Archive Folder)")
    if not nas_root: return

    # 3. Load Master Manifest from NAS
    master_path = os.path.join(nas_root, MANIFEST_NAME)
    
    if os.path.exists(master_path):
        with open(master_path, 'r', encoding='utf-8') as f:
            df_master = pd.read_csv(f)
        existing_sizes = set(df_master['file_size_bytes'].astype(int))
        existing_hashes = set(df_master['checksum_sha256'].astype(str))
    else:
        df_master = pd.DataFrame()
        existing_sizes = set()
        existing_hashes = set()

    batch_metadata = []
    stats = {"processed": 0, "duplicates": 0, "no_folder": 0, "no_exif": 0}

    # 4. Hybrid Scan
    print(f"Scanning: {source_dir}...")
    for root, dirs, files in os.walk(source_dir):
        for filename in files:
            if filename.lower().endswith(('.jpg', '.jpeg')):
                rel_path = os.path.relpath(root, source_dir)
                folder_parts = rel_path.split(os.sep)
                
                if folder_parts[0] == '.':
                    stats["no_folder"] += 1
                    continue
                
                camera_id = folder_parts[0]
                full_path = os.path.join(root, filename)
                
                try:
                    file_size = os.path.getsize(full_path)
                    is_potential_dupe = file_size in existing_sizes
                    
                    with open(full_path, 'rb') as f:
                        file_bytes = f.read()

                    file_hash = get_sha256(file_bytes)
                    
                    if is_potential_dupe and file_hash in existing_hashes:
                        stats["duplicates"] += 1
                        continue
                    
                    timestamp = get_exif_date(file_bytes)
                    if not timestamp:
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
                    existing_sizes.add(file_size)
                    existing_hashes.add(file_hash)

                except Exception as e:
                    if DEBUG: print(f"[ERR] {filename}: {e}")
                    continue

    # 5. Process ZIP
    if batch_metadata:
        batch_metadata.sort(key=lambda x: x['timestamp_obj'])
        
        first_date = batch_metadata[0]['timestamp_obj'].strftime('%Y%b%d')
        last_date = batch_metadata[-1]['timestamp_obj'].strftime('%Y%b%d')
        
        zip_name = f"{first_date}_{last_date}.zip"
        mini_csv_name = f"{first_date}_{last_date}_manifest.csv"
        
        zip_path = os.path.join(nas_root, zip_name)
        mini_csv_path = os.path.join(nas_root, mini_csv_name)
        
        mini_manifest_rows = []
        new_master_rows = []

        print(f"Archiving to {zip_path}...")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for item in batch_metadata:
                ts = item['timestamp_obj']
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
                item['file_bytes'] = None 

        # 6. Save Manifests
        with open(mini_csv_path, 'w', encoding='utf-8', newline='') as f:
            pd.DataFrame(mini_manifest_rows).to_csv(f, index=False)
            
        if new_master_rows:
            df_new = pd.DataFrame(new_master_rows)
            df_final = pd.concat([df_master, df_new], ignore_index=True)
            df_final.sort_values(by=['camera_id', 'timestamp'], inplace=True)
            with open(master_path, 'w', encoding='utf-8', newline='') as f:
                df_final.to_csv(f, index=False)

        # --- PRIORITY POPUP ---
        # Create a temporary popup window to force focus
        final_popup = tk.Toplevel(root_gui)
        final_popup.withdraw()
        force_to_front(final_popup)
        messagebox.showinfo("Success", f"Archive Complete!\n\nLocation: {nas_root}\nImages: {stats['processed']}", parent=final_popup)
        final_popup.destroy()

    else:
        # Priority popup for "No Images"
        no_img_popup = tk.Toplevel(root_gui)
        no_img_popup.withdraw()
        force_to_front(no_img_popup)
        messagebox.showwarning("Notice", "No new unique images found.", parent=no_img_popup)
        no_img_popup.destroy()

if __name__ == "__main__":
    run_archiver()