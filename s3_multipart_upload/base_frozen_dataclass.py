import json
from dataclasses import asdict, dataclass

@dataclass(frozen=True)
class BaseFrozenDataClass:
  def to_dict(self) -> dict:
    return asdict(self)
  
  def to_json_str(self, indent: int = 2) -> str:
    return json.dumps(self.to_dict(), indent=indent)
