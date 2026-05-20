import sys
from pathlib import Path

# Permite importar operadores em tests/ como se estivesse em plugins/
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))
