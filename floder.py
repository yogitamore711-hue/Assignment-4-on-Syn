import os
import shutil
import argparse
import logging
from datetime import datetime
from pathlib import Path
import zipfile

def setup_logger(log_file):
    logger = logging.getLogger("BackupTool")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console log
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    # File log
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def file_changed(src_file, dest_file):
    """Check if file is new or modified."""
    if not dest_file.exists():
        return True
    return src_file.stat().st_mtime > dest_file.stat().st_mtime or src_file.stat().st_size != dest_file.stat().st_size


def incremental_backup(source, destination, dry_run, logger):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_folder = Path(destination) / f"backup_{timestamp}"
    latest_link = Path(destination) / "latest"

    logger.info(f"Backup folder: {backup_folder}")

    if not dry_run:
        backup_folder.mkdir(parents=True, exist_ok=True)

    copied_files = 0
    skipped_files = 0

    # If latest backup exists, compare with it
    previous_backup = latest_link if latest_link.exists() else None

    for root, dirs, files in os.walk(source):
        rel_path = Path(root).relative_to(source)
        dest_dir = backup_folder / rel_path

        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)

        for file in files:
            src_file = Path(root) / file
            dest_file = dest_dir / file

            # Compare with previous backup
            if previous_backup:
                prev_file = previous_backup / rel_path / file
            else:
                prev_file = None

            if prev_file is None or file_changed(src_file, prev_file):
                logger.info(f"Copying: {src_file} -> {dest_file}")
                copied_files += 1
                if not dry_run:
                    shutil.copy2(src_file, dest_file)
            else:
                skipped_files += 1

    logger.info(f"Copied files: {copied_files}")
    logger.info(f"Skipped files: {skipped_files}")

    # Update latest symlink/folder pointer
    if not dry_run:
        if latest_link.exists() or latest_link.is_symlink():
            if latest_link.is_symlink():
                latest_link.unlink()
            else:
                shutil.rmtree(latest_link)

        shutil.copytree(backup_folder, latest_link)

    return backup_folder


def compress_backup(backup_folder, logger, dry_run):
    zip_name = str(backup_folder) + ".zip"
    logger.info(f"Compressing backup into: {zip_name}")

    if dry_run:
        return

    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(backup_folder):
            for file in files:
                file_path = Path(root) / file
                zipf.write(file_path, file_path.relative_to(backup_folder))

    logger.info("Compression done.")


def rotate_backups(destination, keep, logger, dry_run):
    dest = Path(destination)
    backups = sorted([b for b in dest.iterdir() if b.is_dir() and b.name.startswith("backup_")])

    if len(backups) <= keep:
        logger.info("No rotation needed.")
        return

    remove_list = backups[:-keep]
    for folder in remove_list:
        logger.info(f"Rotating old backup: {folder}")
        if not dry_run:
            shutil.rmtree(folder)

        # also remove zip if exists
        zip_file = Path(str(folder) + ".zip")
        if zip_file.exists():
            logger.info(f"Removing old zip: {zip_file}")
            if not dry_run:
                zip_file.unlink()


def main():
    parser = argparse.ArgumentParser(description="Folder Backup / Sync Tool")
    parser.add_argument("--source", required=True, help="Source folder path")
    parser.add_argument("--destination", required=True, help="Destination folder path")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without copying")
    parser.add_argument("--zip", action="store_true", help="Compress backup to ZIP")
    parser.add_argument("--keep", type=int, default=5, help="How many backups to keep (rotation)")
    parser.add_argument("--log", default="backup.log", help="Log file name")

    args = parser.parse_args()

    logger = setup_logger(args.log)

    source = Path(args.source)
    destination = Path(args.destination)

    if not source.exists():
        logger.error("Source folder does not exist!")
        return

    destination.mkdir(parents=True, exist_ok=True)

    backup_folder = incremental_backup(source, destination, args.dry_run, logger)

    if args.zip:
        compress_backup(backup_folder, logger, args.dry_run)

    rotate_backups(destination, args.keep, logger, args.dry_run)

    logger.info("Backup completed successfully.")


if __name__ == "__main__":
    main()
