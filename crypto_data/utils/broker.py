from abc import ABC, abstractclassmethod
from typing import Set, Dict, Any, Optional


class Broker:
    @abstractclassmethod
    def get_pairs(cls) -> Set[str]:
        ...

    @abstractclassmethod
    def get_batches_of_trades(cls, pair: str, last_record: Optional[Dict[str, Any]]):
        ...
