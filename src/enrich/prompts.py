"""
Extraction prompts, loaded verbatim from prompts.yaml.

Keys are <field>_primary for the embedding-matched path and <field>_fallback
for the description path. Edit the text in the YAML, not here.
"""

from pathlib import Path

import yaml

_PROMPTS_FILE = Path(__file__).with_name("prompts.yaml")

PROMPTS: dict[str, str] = yaml.safe_load(_PROMPTS_FILE.read_text(encoding="utf-8"))

__all__ = ["PROMPTS"]
