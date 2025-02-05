"""
Handles checkpoint management for file processing.
"""

import json
import os
from typing import Set

class CheckpointManager:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file

    def get_processed_files(self) -> Set[str]:
        """Read the checkpoint file containing already processed files."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return set(json.load(f))
        return set()

    def update_checkpoint(self, processed_file: str) -> None:
        """Update the checkpoint file with newly processed file."""
        processed_files = self.get_processed_files()
        processed_files.add(processed_file)
        with open(self.checkpoint_file, 'w') as f:
            json.dump(list(processed_files), f)

    def clear_checkpoints(self) -> None:
        """Clear the checkpoint file to start fresh."""
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            print("Checkpoint file cleared.") 