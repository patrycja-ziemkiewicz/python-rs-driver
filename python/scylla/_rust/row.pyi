from typing import Any, Dict

class CqlRow:
    @property
    def columns(self) -> Dict[str, Any]: ...
